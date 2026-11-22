/*-------------------------------------------------------------------------
 * shm_channel.h
 *   PostgreSQL 16 kernel extension: shared-memory channel transport.
 *
 *   Adds a second data path between an embedded gateway process (producer)
 *   and PostgreSQL backends (consumers).  Requests flow gw -> backend
 *   through a lock-free SPSC byte ring; responses flow back through a
 *   second ring.  The backend keeps the existing socket transport fully
 *   intact; shm mode is entered only by dedicated background workers whose
 *   client "socket" is backed by the rings (see shm_channel_read/write
 *   hooks in be-secure.c).
 *
 *   Communication discipline (per ring, single producer / single consumer):
 *   - producer: writes at head, publishes with a write barrier, wakes the
 *     parked consumer by flipping reader_parked/writer_parked (Dekker) and
 *     issuing SetLatch (PG side) or an eventfd write (gateway side).
 *   - consumer: spins briefly (active poll, the fast path), then parks on
 *     its MyLatch (backend); the gateway never parks -- it registers the
 *     slot eventfds with tokio's AsyncFd and lets epoll wake the conn task.
 *
 *   Copyright (c) 2026, yuanyuansong
 *-------------------------------------------------------------------------
 */
#ifndef SHM_CHANNEL_H
#define SHM_CHANNEL_H

#include "postgres.h"

#include "storage/proc.h"
#include "storage/spin.h"

#define SHM_CHANNEL_MAGIC			0x53484D43		/* 'SHMC' */

/* upper bound for the registry slot count (GUC clamps to this) */
#define SHM_CH_MAX_SLOTS			256

/* Fixed per-direction ring buffer sizes (compile-time, keeps layout simple).
 * Both directions are 1 MB (user decision 2026-11-22): guarantees headroom
 * for large protocol messages on the request side too.  Total static shmem
 * cost is nslots * (2 MB + header) ~= 193 MB at 96 slots. */
#define SHM_CH_REQ_RING_SIZE		(1024 * 1024)	/* gw -> backend */
#define SHM_CH_RESP_RING_SIZE		(1024 * 1024)	/* backend -> gw */

/* Slot lifecycle. */
#define SCH_FREE					0	/* unclaimed / session ended */
#define SCH_READY					1	/* backend alive, waiting for client */
#define SCH_ACTIVE					2	/* client attached, session live */

/* Active-poll iterations before parking (hybrid spin-then-park). */
#define SHM_RING_SPIN_COUNT			2000

/*
 * One lock-free SPSC byte ring.  head/tail are monotonic byte cursors; the
 * buffer is size bytes.  reader_parked / writer_parked implement the
 * Dekker-style sleep flag: the sleeper sets its flag, issues a full
 * barrier, re-checks the peer cursor, then parks; the producer, after
 * advancing its cursor and a write barrier, clears the flag and wakes.
 */
typedef struct ShmRing
{
	volatile uint64 head;		/* bytes written by producer */
	volatile uint64 tail;		/* bytes consumed by consumer */
	volatile uint32 reader_parked;	/* consumer sleeping */
	volatile uint32 writer_parked;	/* producer sleeping */
	volatile uint32 closed;		/* peer gone / channel dead */
	uint32		size;
	char	   *buf;			/* points into the owning slot */
	PGPROC	   *reader_proc;	/* set when reader is a PG process */
	PGPROC	   *writer_proc;	/* set when writer is a PG process */
} ShmRing;

/*
 * One session slot.  The embedded gateway claims a READY slot to obtain a
 * fresh pseudo-connection; the claim bumps generation so a stale attach
 * against a recycled slot is detected client-side.
 */
typedef struct ShmChannelSlot
{
	volatile uint32 state;		/* SCH_FREE / SCH_READY / SCH_ACTIVE */
	volatile uint32 backend_pid;
	volatile uint64 generation;
	ShmRing		req;			/* gw -> backend */
	ShmRing		resp;			/* backend -> gw */
	char		req_buf[SHM_CH_REQ_RING_SIZE];
	char		resp_buf[SHM_CH_RESP_RING_SIZE];

	/*
	 * Diagnostic counters.  Appended AFTER the buffers on purpose: the
	 * gateway mirrors the layout above byte-for-byte in Rust, so these
	 * fields must never be inserted before resp_buf.  Rust never walks
	 * slots itself (it uses shm_channel_slot_ptr), so growing the slot
	 * tail is transparent to it.
	 */
	volatile uint64 diag_send_full;		/* producer found the ring full */
	volatile uint64 diag_recv_empty;	/* consumer found the ring empty */
	volatile uint64 diag_req_bytes;		/* bytes moved gw -> backend */
	volatile uint64 diag_resp_bytes;	/* bytes moved backend -> gw */

	/*
	 * Eventfds created by the postmaster before any fork (both the shm
	 * backend workers and the embedded gateway inherit the same open file
	 * descriptions): resp_efd signals "bytes available on the resp ring"
	 * to the gateway, req_efd signals "space freed on the req ring" to a
	 * blocked gateway writer.  The gateway polls them with tokio AsyncFd
	 * (epoll), which removes the per-slot watchdog thread entirely: the
	 * response wake chain becomes backend write(eventfd) -> epoll wake ->
	 * conn task, i.e. the same hop count as the socket transport.  Must
	 * stay AFTER resp_buf (and mirror byte-for-byte in Rust).
	 */
	int			resp_efd;
	int			req_efd;
} ShmChannelSlot;

typedef struct ShmChannelControl
{
	slock_t		mutex;			/* guards attach/claim state transitions */
	int32		magic;
	int32		nslots;
	ShmChannelSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ShmChannelControl;

/* size of the static-shmem registry for nslots slots */
extern Size ShmChannelShmemSize(int nslots);

/* core-side hooks called from be-secure.c (MyShmChannel != NULL => shm mode) */
extern ssize_t shm_channel_read(void *ptr, size_t len);
extern ssize_t shm_channel_write(const void *ptr, size_t len);

/*
 * Client (gateway) C API, resolved by dlsym from the embedded gateway.
 * All return -1 when the registry is unavailable.
 */
extern int	shm_channel_slot_count(void);
extern int	shm_channel_try_attach(void);	/* claims one READY slot */
extern void shm_channel_detach(int slot);	/* release a claim / drop session */
extern int	shm_channel_backend_alive(int slot);
extern void shm_channel_wake_backend(int slot);	/* SetLatch req.reader_proc */
extern void shm_channel_wake_writer(int slot);	/* SetLatch resp.writer_proc */

/* ring accessors handed to the client after attach */
extern ShmChannelSlot *shm_channel_slot_ptr(int slot);

/* the per-process slot pointer; NULL for ordinary socket backends */
extern PGDLLIMPORT ShmChannelSlot *MyShmChannel;

/* module setup (called from core bootstrap) */
extern void ShmChannelDefineGUCs(void);		/* before SelectConfigFiles */
extern bool shm_channel_is_enabled(void);	/* GUC accessor for other core files */
extern void ShmChannelShmemInit(void);		/* in CreateSharedMemoryAndSemaphores */
extern void ShmChannelRegisterWorkers(void);/* in PostmasterMain after config */

/* stock startup path, exported from postmaster.c for shm-channel workers.
 * Forward declaration; the full definition lives in libpq-be.h.  Keeps this
 * header self-contained for early include sites (postmaster.c includes it
 * before libpq-be.h, so a bare typedef-style "Port *" would not compile). */
struct Port;
extern void BackendInitialize(struct Port *port);

/* bgworker entry registered in InternalBGWorkers[] (kernel bgworker.c) */
extern void ShmChannelWorkerMainPublic(Datum main_arg);

#endif							/* SHM_CHANNEL_H */
