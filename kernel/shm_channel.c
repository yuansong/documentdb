/*-------------------------------------------------------------------------
 * shm_channel.c
 *   PostgreSQL 16 kernel extension: shared-memory channel transport.
 *
 *   Adds a lock-free shared-memory data path between the embedded gateway
 *   (documentdb gw worker) and PostgreSQL backends, as an alternative to
 *   the loopback TCP / unix-socket wire protocol.  The socket transport is
 *   untouched: shm mode applies only to dedicated "shmem backend" workers
 *   whose client I/O (secure_read / secure_write) is redirected to two
 *   SPSC byte rings inside a static shared-memory registry.
 *
 *   Process roles:
 *   - gateway (producer on req ring, consumer on resp ring): polls the
 *     slot's eventfds with tokio AsyncFd (epoll); wakes the backend via
 *     SetLatch through the exported C API (dlsym'd by the Rust side).
 *   - shmem backend worker (consumer on req ring, producer on resp ring):
 *     parks on MyLatch after a brief active-poll spin ("idle goes and
 *     fetches data"); wakes through SetLatch issued by the gateway.
 *
 *   Wakeup design (2026-11-23 rework): the backend notifies the gateway
 *   with one nonblocking write() to the slot eventfd after every resp
 *   chunk -- the same syscall cost as a socket send() -- and the gateway
 *   conn task is woken directly by epoll.  This removes the previous
 *   per-slot watchdog thread and its futex + waker-forwarding hop, so the
 *   shm response path has the same scheduling depth as the socket path.
 *
 *   The worker reuses the stock backend startup path: BackendInitialize()
 *   (startup packet + HBA) reads its bytes from the ring through the
 *   secure_read hook, and PostgresMain()/InitPostgres() perform normal
 *   authentication and initialization.  No protocol code is duplicated.
 *
 *   Process model and discovery follow pg_fastload's pattern: a registry
 *   control block in static shared memory, a per-slot state machine
 *   (FREE/READY/ACTIVE), and SetLatch-based cross-process wakeup.
 *
 *   Copyright (c) 2026, yuanyuansong
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <signal.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shm_channel.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/* ------------------------------------------------------------------ */
/* Module state                                                        */
/* ------------------------------------------------------------------ */

ShmChannelSlot *MyShmChannel = NULL;

static ShmChannelControl *ShmChannelCtl = NULL;

static bool		shm_channel_enabled = false;
static int		shm_channel_nslots = 64;	/* PGC_POSTMASTER */
static bool		shm_channel_gucs_defined = false;

static inline void
shm_pause(void)
{
#if defined(__x86_64__) || defined(__i386__)
	__asm__ __volatile__ ("pause");
#else
	pg_compiler_barrier();
#endif
}

/*
 * Wake the gateway-side poller through the slot's eventfd.
 *
 * The gateway registers both slot eventfds with tokio's AsyncFd (epoll), so
 * one nonblocking write() here has the same cost as one socket send() and
 * directly marks the connection task runnable -- no intermediate user-space
 * forwarding thread.  The eventfd counter coalesces notifications: a burst
 * of produces costs one readiness event on the consumer side.  EAGAIN is
 * fine (the counter only saturates after ~2^64 pending notifications; the
 * gateway drains on every wait anyway).
 */
static inline void
shm_notify_eventfd(int efd)
{
	uint64		one = 1;

	if (efd >= 0)
	{
		ssize_t		rc = write(efd, &one, sizeof(one));

		(void) rc;				/* EAGAIN / EINTR: the poller rechecks */
	}
}

/*
 * Thread-safe backend liveness check for the gateway side.
 *
 * The embedded gateway runs a multi-threaded tokio runtime inside a
 * background worker.  BackendPidGetProc()/GetPGProcByNumber() acquire
 * ProcArrayLock through the LWLock manager, which assumes a single-threaded
 * PG process and mutates MyProc->lwWaiting; calling them from several tokio
 * threads at once trips the "queueing for lock while waiting on another one"
 * PANIC and corrupts shared state.  kill(pid, 0) is a pure syscall with no
 * PG global-state dependency, so it is safe from any thread.
 */
static inline bool
shm_pid_alive(uint32 pid)
{
	if (pid == 0)
		return false;
	if (kill((pid_t) pid, 0) == 0)
		return true;
	return (errno == EPERM);	/* exists but not signalable: still alive */
}

/* ------------------------------------------------------------------ */
/* Ring primitives (lock-free SPSC)                                    */
/* ------------------------------------------------------------------ */

/* read len bytes out of the ring; len must be <= ring_used(r) */
static inline void
ring_copy_out(ShmRing *r, char *dest, uint64 len)
{
	uint64		pos = r->tail % r->size;

	if (pos + len <= (uint64) r->size)
		memcpy(dest, r->buf + pos, len);
	else
	{
		uint64		first = r->size - pos;

		memcpy(dest, r->buf + pos, first);
		memcpy(dest + first, r->buf, len - first);
	}
}

/* write len bytes into the ring; len must be <= r->size - ring_used(r) */
static inline void
ring_copy_in(ShmRing *r, const char *src, uint64 len)
{
	uint64		pos = r->head % r->size;

	if (pos + len <= (uint64) r->size)
		memcpy(r->buf + pos, src, len);
	else
	{
		uint64		first = r->size - pos;

		memcpy(r->buf + pos, src, first);
		memcpy(r->buf, src + first, len - first);
	}
}

/*
 * Runtime sanity gate for ring cursors.  Catches struct-layout drift,
 * cursor corruption and use-before-init BEFORE a wild memcpy can poison
 * neighbouring shared memory (the failure mode seen on 2026-11-22: the
 * gateway leader crashed with a corrupted linker map).  Runs in backend
 * PG context only, so elog() is safe here.
 */
static void
shm_ring_sane(ShmRing *r, const char *ctx)
{
	uint64		used = r->head - r->tail;

	if (r->buf == NULL ||
		r->size == 0 ||
		(r->size != SHM_CH_REQ_RING_SIZE &&
		 r->size != SHM_CH_RESP_RING_SIZE) ||
		used > (uint64) r->size ||
		(r->tail > r->head))
		elog(FATAL,
			 "shm ring corrupted (%s): buf=%p size=%u head=%llu tail=%llu "
			 "used=%llu reader_parked=%u writer_parked=%u closed=%u",
			 ctx, (void *) r->buf, r->size,
			 (unsigned long long) r->head,
			 (unsigned long long) r->tail,
			 (unsigned long long) used,
			 r->reader_parked, r->writer_parked, r->closed);
}

/*
 * Consumer side (backend on the req ring).  Block until at least one byte
 * is available; returns bytes copied (always >= 1), or 0 on a closed
 * channel (EOF).  space_efd is the slot's req_efd: written when a gateway
 * writer is blocked on ring space, so its tokio write task can proceed.
 */
static Size
ring_recv(ShmRing *r, char *dest, Size want, int space_efd)
{
	for (;;)
	{
		uint64		avail = r->head - r->tail;

		shm_ring_sane(r, "recv");

		/* fast path: data already present */
		if (avail > 0)
		{
			Size		n = (Size) Min((uint64) want, avail);

			pg_read_barrier();
			ring_copy_out(r, dest, n);
			pg_write_barrier();
			r->tail += n;
			if (MyShmChannel != NULL)
				MyShmChannel->diag_req_bytes += n;
			pg_memory_barrier();
			if (r->writer_parked)
			{
				r->writer_parked = 0;
				shm_notify_eventfd(space_efd);
			}
			return n;
		}
		if (r->closed)
			return 0;

		/* consumer had to wait: count the stall for capacity analysis */
		if (MyShmChannel != NULL)
			MyShmChannel->diag_recv_empty += 1;

		/* active poll: spin for a few iterations before parking */
		for (int i = 0; i < SHM_RING_SPIN_COUNT; i++)
		{
			if (r->head - r->tail > 0)
				break;
			if (r->closed)
				return 0;
			shm_pause();
		}
		if (r->head - r->tail > 0)
			continue;

		/* slow path: park on the latch (Dekker flag + barrier + recheck) */
		r->reader_parked = 1;
		pg_memory_barrier();
		if (r->head - r->tail > 0)
		{
			r->reader_parked = 0;
			continue;
		}
		if (r->closed)
		{
			r->reader_parked = 0;
			return 0;
		}

		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  10 * 1000L /* ms */,
				  WAIT_EVENT_CLIENT_READ);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		if (r->closed)
			return 0;
	}
}

/*
 * Producer side (backend on the resp ring).  Blocks until all len bytes
 * are written; returns len, or -1 if the channel was closed by the
 * consumer.  data_efd is the slot's resp_efd: notified after every chunk
 * so the gateway's epoll poller can mark the conn task readable.
 */
static Size
ring_send(ShmRing *r, const char *src, Size len, int data_efd)
{
	Size		sent = 0;

	while (sent < len)
	{
		uint64		free_space = r->size - (r->head - r->tail);

		shm_ring_sane(r, "send");

		if (free_space > 0)
		{
			uint64		pos = r->head % r->size;
			uint64		chunk = Min((uint64) (len - sent), free_space);

			chunk = Min(chunk, r->size - pos);
			ring_copy_in(r, src + sent, chunk);
			pg_write_barrier();
			r->head += chunk;
			sent += chunk;
			if (MyShmChannel != NULL)
				MyShmChannel->diag_resp_bytes += chunk;

			/* eventfd coalesces: one write() per chunk, like a socket send */
			shm_notify_eventfd(data_efd);
		}
		else
		{
			/* full: spin briefly, then park on the latch */
			if (MyShmChannel != NULL)
				MyShmChannel->diag_send_full += 1;
			for (int i = 0; i < SHM_RING_SPIN_COUNT; i++)
			{
				if (r->size - (r->head - r->tail) > 0)
					break;
				if (r->closed)
					return -1;
				shm_pause();
			}
			if (r->size - (r->head - r->tail) > 0)
				continue;

			r->writer_parked = 1;
			pg_memory_barrier();
			if (r->size - (r->head - r->tail) > 0)
			{
				r->writer_parked = 0;
				continue;
			}
			if (r->closed)
				return -1;

			/*
			 * The gateway's poll_read clears writer_parked and wakes us
			 * through shm_channel_wake_writer() (SetLatch) after draining
			 * resp ring space; the 10 s timeout is the backstop.
			 */
			WaitLatch(MyLatch,
					  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					  10 * 1000L,
					  WAIT_EVENT_CLIENT_WRITE);
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
			if (r->closed)
				return -1;
		}
	}
	return (Size) sent;
}

/* ------------------------------------------------------------------ */
/* be-secure.c hooks: the backend's "socket" is the ring pair          */
/* ------------------------------------------------------------------ */

ssize_t
shm_channel_read(void *ptr, size_t len)
{
	Assert(MyShmChannel != NULL);
	Assert(len > 0);
	return (ssize_t) ring_recv(&MyShmChannel->req, (char *) ptr, len,
							   MyShmChannel->req_efd);
}

ssize_t
shm_channel_write(const void *ptr, size_t len)
{
	Assert(MyShmChannel != NULL);
	return (ssize_t) ring_send(&MyShmChannel->resp, (const char *) ptr, len,
							   MyShmChannel->resp_efd);
}

/* ------------------------------------------------------------------ */
/* Gateway client API (resolved via dlsym from gw_host)                */
/* ------------------------------------------------------------------ */

ShmChannelSlot *
shm_channel_slot_ptr(int slot)
{
	if (ShmChannelCtl == NULL || slot < 0 ||
		(ShmChannelCtl != NULL && slot >= ShmChannelCtl->nslots))
		return NULL;
	return &ShmChannelCtl->slots[slot];
}

int
shm_channel_slot_count(void)
{
	if (ShmChannelCtl == NULL || !shm_channel_enabled)
		return -1;
	return ShmChannelCtl->nslots;
}

int
shm_channel_try_attach(void)
{
	int			i;

	if (ShmChannelCtl == NULL || !shm_channel_enabled)
		return -1;

	/* recover slots whose backend died mid-session (crash cleanup missed) */
	for (i = 0; i < ShmChannelCtl->nslots; i++)
	{
		ShmChannelSlot *s = &ShmChannelCtl->slots[i];

		if (s->state == SCH_ACTIVE &&
			!shm_pid_alive(s->backend_pid))
		{
			SpinLockAcquire(&ShmChannelCtl->mutex);
			if (s->state == SCH_ACTIVE &&
				!shm_pid_alive(s->backend_pid))
				s->state = SCH_FREE;
			SpinLockRelease(&ShmChannelCtl->mutex);
		}
	}

	SpinLockAcquire(&ShmChannelCtl->mutex);
	for (i = 0; i < ShmChannelCtl->nslots; i++)
	{
		ShmChannelSlot *s = &ShmChannelCtl->slots[i];

		if (s->state == SCH_READY)
		{
			s->state = SCH_ACTIVE;
			s->generation++;
			SpinLockRelease(&ShmChannelCtl->mutex);
			return i;
		}
	}
	SpinLockRelease(&ShmChannelCtl->mutex);
	return -1;
}

void
shm_channel_detach(int slot)
{
	ShmChannelSlot *s = shm_channel_slot_ptr(slot);

	if (s == NULL)
		return;
	SpinLockAcquire(&ShmChannelCtl->mutex);
	if (s->state == SCH_ACTIVE)
		s->state = SCH_FREE;
	SpinLockRelease(&ShmChannelCtl->mutex);
}

int
shm_channel_backend_alive(int slot)
{
	ShmChannelSlot *s = shm_channel_slot_ptr(slot);

	if (s == NULL)
		return -1;
	return shm_pid_alive(s->backend_pid) ? 1 : 0;
}

void
shm_channel_wake_backend(int slot)
{
	ShmChannelSlot *s = shm_channel_slot_ptr(slot);

	if (s != NULL && s->req.reader_proc != NULL)
		SetLatch(&s->req.reader_proc->procLatch);
	else if (s != NULL)
	{
		/*
		 * Diagnostic only: this runs on a tokio thread where elog() is
		 * forbidden (longjmp across Rust frames), so report via write(2),
		 * which is async-signal-safe, and drop the wake.
		 */
		static const char msg[] =
			"shm_channel: wake_backend on a slot without a backend reader\n";

		write(2, msg, sizeof(msg) - 1);
	}
}

/*
 * Wake a backend parked on a full resp ring (latch park in ring_send).
 * The gateway's poll_read calls this after advancing the resp tail; it
 * runs on a tokio thread, so no elog() on the failure path.
 */
PGDLLEXPORT void
shm_channel_wake_writer(int slot)
{
	ShmChannelSlot *s = shm_channel_slot_ptr(slot);

	if (s != NULL && s->resp.writer_proc != NULL)
		SetLatch(&s->resp.writer_proc->procLatch);
}

/* ------------------------------------------------------------------ */
/* Shmem backend worker main: a PostgresMain over the rings            */
/* ------------------------------------------------------------------ */

static void
shm_channel_slot_cleanup(int code, Datum arg)
{
	ShmChannelSlot *s = (ShmChannelSlot *) DatumGetPointer(arg);

	if (s == NULL)
		return;

	/* per-session transport stats: ring-full stalls reveal capacity issues */
	elog(LOG,
		 "shm slot session stats: backend_pid=%u send_full=%llu "
		 "recv_empty=%llu req_bytes=%llu resp_bytes=%llu",
		 s->backend_pid,
		 (unsigned long long) s->diag_send_full,
		 (unsigned long long) s->diag_recv_empty,
		 (unsigned long long) s->diag_req_bytes,
		 (unsigned long long) s->diag_resp_bytes);

	SpinLockAcquire(&ShmChannelCtl->mutex);
	if (s->state == SCH_ACTIVE)
		s->state = SCH_FREE;
	s->req.closed = 1;
	s->resp.closed = 1;
	SpinLockRelease(&ShmChannelCtl->mutex);
	/*
	 * Wake the gateway poller on both eventfds so its next readiness event
	 * observes closed=1 and the conn task sees EOF instead of parking.
	 */
	shm_notify_eventfd(s->resp_efd);
	shm_notify_eventfd(s->req_efd);
}

static void
shm_channel_ring_init(ShmRing *r, char *buf, uint32 size, bool backend_reader)
{
	r->head = 0;
	r->tail = 0;
	r->reader_parked = 0;
	r->writer_parked = 0;
	r->closed = 0;
	r->size = size;
	r->buf = buf;
	if (backend_reader)
	{
		r->reader_proc = MyProc;	/* backend consumes */
		r->writer_proc = NULL;		/* gateway produces */
	}
	else
	{
		r->reader_proc = NULL;		/* gateway consumes */
		r->writer_proc = MyProc;	/* backend produces */
	}
}

PGDLLEXPORT void
ShmChannelWorkerMainPublic(Datum main_arg)
{
	int			idx = DatumGetInt32(main_arg);
	ShmChannelSlot *slot;
	Port	   *port;

	Assert(ShmChannelCtl != NULL);
	slot = &ShmChannelCtl->slots[idx];

	/* claim the slot for this lifetime */
	SpinLockAcquire(&ShmChannelCtl->mutex);
	slot->backend_pid = MyProcPid;
	slot->generation++;
	/* fresh per-session diagnostic counters */
	slot->diag_send_full = 0;
	slot->diag_recv_empty = 0;
	slot->diag_req_bytes = 0;
	slot->diag_resp_bytes = 0;
	shm_channel_ring_init(&slot->req, slot->req_buf, SHM_CH_REQ_RING_SIZE,
						  true);
	shm_channel_ring_init(&slot->resp, slot->resp_buf, SHM_CH_RESP_RING_SIZE,
						  false);
	slot->state = SCH_READY;
	SpinLockRelease(&ShmChannelCtl->mutex);

	/* drain notifications left over from a previous session on this slot */
	{
		uint64		v;

		while (read(slot->resp_efd, &v, sizeof(v)) == (ssize_t) sizeof(v))
			;
		while (read(slot->req_efd, &v, sizeof(v)) == (ssize_t) sizeof(v))
			;
	}

	/*
	 * Announce the fresh worker on the resp eventfd: a gateway stuck in a
	 * read wait on a recycled slot wakes immediately, compares the slot
	 * generation against the one it recorded at connect time, and sees EOF
	 * for the dead session without any per-request liveness syscall.
	 */
	shm_notify_eventfd(slot->resp_efd);

	MyShmChannel = slot;

	/*
	 * Build a minimal Port so the stock backend startup path behaves like a
	 * local unix-socket connection: raddr is AF_UNIX so pg_hba.conf "local"
	 * lines match.
	 */
	port = (Port *) MemoryContextAllocZero(TopMemoryContext, sizeof(Port));
	port->raddr.addr.ss_family = AF_UNIX;
	port->raddr.salen = sizeof(struct sockaddr_un);
	port->laddr.addr.ss_family = AF_UNIX;
	port->laddr.salen = sizeof(struct sockaddr_un);

	/*
	 * Give the port a real (unused) socket fd: libpq startup code performs
	 * fd bookkeeping on MyProcPort->sock (e.g. pq_setnonblocking), which
	 * fails fatally on PGINVALID_SOCKET.  Protocol data never touches this
	 * fd because secure_read/secure_write are redirected to the ring.
	 */
	{
		int		sv[2];

		if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0)
			port->sock = sv[0];		/* keep both ends: avoids EPIPE/SIGPIPE */
		else
			port->sock = PGINVALID_SOCKET;
	}
	MyProcPort = port;

	/* startup packet + cancel-request handling, bytes come from the ring */
	elog(LOG, "shm worker slot %d: waiting for startup packet", idx);
	BackendInitialize(port);
	elog(LOG, "shm worker slot %d: backend startup completed", idx);

	/* the startup path may have decided we are a dead-end (proc_exit'ed) */

	/*
	 * from here on, our lifetime maps to one client session: releasing the
	 * slot on any exit path lets the gateway rediscover it afterwards
	 */
	on_shmem_exit(shm_channel_slot_cleanup, PointerGetDatum(slot));

	/* full backend loop: auth, query protocol, COPY, everything */
	PostgresMain(port->database_name, port->user_name);
	/* noreturn */
}

/* ------------------------------------------------------------------ */
/* Bootstrap: GUCs, shmem, worker registration                         */
/* ------------------------------------------------------------------ */

Size
ShmChannelShmemSize(int nslots)
{
	Size		size;

	size = offsetof(ShmChannelControl, slots);
	size = add_size(size, mul_size(sizeof(ShmChannelSlot), nslots));
	return size;
}

/*
 * GUC accessor for other core files (e.g. hba.c choosing the parser-context
 * parent): shm_channel_enabled itself is file-static.
 */
bool
shm_channel_is_enabled(void)
{
	return shm_channel_enabled;
}

void
ShmChannelDefineGUCs(void)
{
	if (shm_channel_gucs_defined)
		return;
	shm_channel_gucs_defined = true;
	DefineCustomBoolVariable("shm_channel.enabled",
							 "Enable the shared-memory channel transport "
							 "(spawns shm-channel backend workers).",
							 NULL,
							 &shm_channel_enabled,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("shm_channel.num_channels",
							"Number of shared-memory channel slots "
							"(one backend worker per slot).",
							NULL,
							&shm_channel_nslots,
							64,
							1,
							SHM_CH_MAX_SLOTS,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("shm_channel");
}

/*
 * Saved predecessors so our hooks CHAIN instead of clobber.
 *
 * shmem_request_hook / shmem_startup_hook are single-slot function
 * pointers.  The shared_preload_libraries (pg_cron, pg_documentdb,
 * pg_stat_statements) install the same hooks with their own chaining, and
 * overwriting the pointer here silently DROPPED DocumentDB's
 * DocumentDBSharedMemoryRequest/Init: its background-worker job registry
 * was never allocated, so the job leader dereferenced NULL+4 as an LWLock
 * and SIGSEGV'd in a ~13 s loop whenever the shm transport was enabled
 * (mode B), while mode A (transport off, early return here) stayed clean.
 * gdb live-attach on 2026-11-22 captured the full backtrace:
 *   LWLockAttemptLock <- LWLockAcquire <- PublishBackgroundWorkerJobCompletion
 *   <- CompleteJobAttempt <- ManageJobsLifeCycle <- DocumentDBBackgroundWorkerMain
 */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void shm_channel_shmem_request(void);
static void shm_channel_shmem_startup(void);

void
ShmChannelShmemInit(void)
{
	/*
	 * Called at the top of CreateSharedMemoryAndSemaphores().  The hooks
	 * must be installed before CalculateShmemSize() runs so our registry
	 * space is included in the segment layout.
	 */
	if (!shm_channel_enabled)
		return;

	prev_shmem_request_hook = shmem_request_hook;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_request_hook = shm_channel_shmem_request;
	shmem_startup_hook = shm_channel_shmem_startup;
}

static void
shm_channel_shmem_request(void)
{
	/* preload libraries' requests first, ours appended to the segment */
	if (prev_shmem_request_hook != NULL)
		prev_shmem_request_hook();

	if (shm_channel_enabled)
		RequestAddinShmemSpace(ShmChannelShmemSize(shm_channel_nslots));
}

static void
shm_channel_shmem_startup(void)
{
	bool		found;

	/* build the preload libraries' shared memory before ours */
	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	if (!shm_channel_enabled)
	{
		ShmChannelCtl = NULL;
		return;
	}

	ShmChannelCtl = (ShmChannelControl *)
		ShmemInitStruct("ShmChannel Registry",
						ShmChannelShmemSize(shm_channel_nslots), &found);

	if (!found)
	{
		SpinLockInit(&ShmChannelCtl->mutex);
		ShmChannelCtl->magic = SHM_CHANNEL_MAGIC;
		ShmChannelCtl->nslots = shm_channel_nslots;
		for (int i = 0; i < shm_channel_nslots; i++)
		{
			ShmChannelSlot *s = &ShmChannelCtl->slots[i];

			MemSet(s, 0, sizeof(ShmChannelSlot));
			s->state = SCH_FREE;

			/*
			 * One eventfd per ring direction, created in the postmaster so
			 * every forked child (shm backend workers AND the embedded
			 * gateway bgworker) inherits the same open file description.
			 * Nonblocking: notifications never block the producing side.
			 */
			s->resp_efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
			s->req_efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
			if (s->resp_efd < 0 || s->req_efd < 0)
				elog(ERROR,
					 "shm channel: eventfd creation failed for slot %d: %m",
					 i);
		}
	}
}

void
ShmChannelRegisterWorkers(void)
{
	BackgroundWorker worker;

	if (!shm_channel_enabled)
		return;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "shmem-channel-worker");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 1;
	strcpy(worker.bgw_library_name, "postgres");
	strcpy(worker.bgw_function_name, "ShmChannelWorkerMainPublic");

	for (int i = 0; i < shm_channel_nslots; i++)
	{
		worker.bgw_main_arg = Int32GetDatum(i);
		RegisterBackgroundWorker(&worker);
	}
}
