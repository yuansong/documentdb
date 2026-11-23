/*-------------------------------------------------------------------------
 * shmem_transport.rs
 *   documentdb_gateway_core: shared-memory channel transport (client side).
 *
 *   Implements tokio_postgres' AsyncRead/AsyncWrite over the PG16 kernel
 *   shm_channel rings, so tokio-postgres can run its stock wire protocol
 *   (startup packet, SCRAM, extended protocol) through shared memory
 *   instead of a loopback socket.  The channel registry and the rings live
 *   in PostgreSQL's static shared memory (kernel module shm_channel); the
 *   C API is resolved lazily via dlsym so the gateway keeps working on
 *   clusters without the kernel module.
 *
 *   Wakeup design (2026-11-23 rework, replaces the per-slot watchdog
 *   thread): the kernel side notifies the slot eventfds -- resp_efd after
 *   producing response bytes, req_efd after consuming request bytes -- and
 *   this side registers both with tokio's AsyncFd (epoll).  A backend
 *   write(eventfd) therefore wakes the conn task directly through the same
 *   reactor the socket transport uses: identical scheduling depth, no
 *   intermediate user-space forwarding thread, no futex park on the Rust
 *   side.  Slot cursors/flags still carry the Dekker park markers the
 *   kernel expects (reader_parked for latch-parked backend readers,
 *   writer_parked for latch-parked backend resp writers).
 *
 *   Instrumentation: every ring access passes a corruption gate (cursor
 *   sanity + layout check at connect).  A violated invariant produces a
 *   loud tracing::error + BrokenPipe instead of a wild memcpy.
 *
 *   Copyright (c) 2026, yuanyuansong
 *-------------------------------------------------------------------------
 */

use std::{
    ffi::{c_int, c_void, CString},
    io,
    os::unix::io::RawFd,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        OnceLock,
    },
    task::{Context, Poll},
};

use deadpool_postgres::Connect;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};

/// Structurally identical to the private `BoxFuture` alias used inside
/// deadpool-postgres' `Connect` trait (`Pin<Box<dyn Future + Send>>`, a
/// transparent type alias), so this impl matches the trait signature
/// without adding a futures dependency.
type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Ring sizes must match kernel/shm_channel.h (compile-time in the kernel).
/// Both directions are 1 MB (user decision 2026-11-22).
const REQ_RING_SIZE: usize = 1024 * 1024;
const RESP_RING_SIZE: usize = 1024 * 1024;

/* ------------------------------------------------------------------ */
/* Shared-memory layout mirrors of the kernel structures               */
/* ------------------------------------------------------------------ */

/// Cacheline-split mirror of the kernel ShmRing (192 bytes, 3 lines).
/// Layout must stay byte-for-byte identical to kernel/shm_channel.h:
/// line 0 = producer side (head/writer_parked), line 1 = consumer side
/// (tail/reader_parked), line 2 = cold (closed/size/buf/procs).
#[repr(C)]
struct ShmRing {
    head: AtomicU64,
    writer_parked: AtomicU32,
    _pad0: [u32; 12],
    tail: AtomicU64,
    reader_parked: AtomicU32,
    _pad1: [u32; 13],
    closed: AtomicU32,
    size: u32,
    buf: u64,
    reader_proc: u64,
    writer_proc: u64,
    _pad2: [u32; 8],
}

#[repr(C)]
struct ShmChannelSlot {
    state: AtomicU32,
    backend_pid: AtomicU32,
    generation: AtomicU64,
    req: ShmRing,
    resp: ShmRing,
    req_buf: [u8; REQ_RING_SIZE],
    resp_buf: [u8; RESP_RING_SIZE],
    // Diagnostic counters (single writer: the slot's backend process).
    diag_send_full: AtomicU64,
    diag_recv_empty: AtomicU64,
    diag_req_bytes: AtomicU64,
    diag_resp_bytes: AtomicU64,
    // Eventfds created by the postmaster, inherited through fork; valid
    // in this process.  Must mirror the kernel slot tail byte-for-byte.
    resp_efd: i32,
    req_efd: i32,
}

impl ShmRing {
    unsafe fn used(&self) -> u64 {
        // saturating_sub: a corrupt cursor pair must never wrap around and
        // turn `free()` into a huge bogus value (which would drive a wild
        // memcpy).  Call sites check `used() > size` explicitly and bail.
        self.head.load(Ordering::Acquire)
            .saturating_sub(self.tail.load(Ordering::Acquire))
    }

    unsafe fn free(&self) -> u64 {
        unsafe { (self.size as u64).saturating_sub(self.used()) }
    }

    /// true when the cursor pair is inconsistent with the ring size.
    unsafe fn corrupted(&self) -> bool {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire);
        self.size == 0
            || self.buf == 0
            || t > h
            || h - t > self.size as u64
    }
}

/* ------------------------------------------------------------------ */
/* C API resolved with dlsym(RTLD_DEFAULT)                             */
/* ------------------------------------------------------------------ */

struct ShmApi {
    slot_ptr: unsafe extern "C" fn(c_int) -> *mut c_void,
    try_attach: unsafe extern "C" fn() -> c_int,
    detach: unsafe extern "C" fn(c_int),
    backend_alive: unsafe extern "C" fn(c_int) -> c_int,
    wake_backend: unsafe extern "C" fn(c_int),
    wake_writer: unsafe extern "C" fn(c_int),
    slot_count: unsafe extern "C" fn() -> c_int,
}

unsafe fn dlsym_sym(name: &str) -> Option<*mut c_void> {
    let cname = CString::new(name).ok()?;
    let sym = libc::dlsym(libc::RTLD_DEFAULT, cname.as_ptr());
    if sym.is_null() {
        None
    } else {
        Some(sym)
    }
}

static API: OnceLock<Option<ShmApi>> = OnceLock::new();

fn api() -> Option<&'static ShmApi> {
    API
        .get_or_init(|| unsafe {
            let slot_ptr = match dlsym_sym("shm_channel_slot_ptr") {
                Some(p) => p,
                None => {
                    eprintln!("GW-SHM: dlsym shm_channel_slot_ptr FAILED (kernel symbols invisible)");
                    return None;
                }
            };
            let try_attach = dlsym_sym("shm_channel_try_attach")?;
            let detach = dlsym_sym("shm_channel_detach")?;
            let backend_alive = dlsym_sym("shm_channel_backend_alive")?;
            let wake_backend = dlsym_sym("shm_channel_wake_backend")?;
            let wake_writer = dlsym_sym("shm_channel_wake_writer")?;
            let slot_count = dlsym_sym("shm_channel_slot_count")?;
            Some(ShmApi {
                slot_ptr: std::mem::transmute(slot_ptr),
                try_attach: std::mem::transmute(try_attach),
                detach: std::mem::transmute(detach),
                backend_alive: std::mem::transmute(backend_alive),
                wake_backend: std::mem::transmute(wake_backend),
                wake_writer: std::mem::transmute(wake_writer),
                slot_count: std::mem::transmute(slot_count),
            })
        })
        .as_ref()
}

/// Set by the gateway host at startup from the
/// `documentdb_gateway.transport` GUC ("shmem" enables the transport).
static TRANSPORT_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn set_transport_enabled(enabled: bool) {
    TRANSPORT_ENABLED.store(enabled, Ordering::SeqCst);
}

fn transport_enabled() -> bool {
    TRANSPORT_ENABLED.load(Ordering::SeqCst) && api().is_some()
}

/// Drain an eventfd counter (one read resets it to zero).  Errors are
/// ignored: the ring cursors are the source of truth, the eventfd only
/// accelerates wakeup.
fn drain_eventfd(fd: RawFd) {
    let mut val = [0u8; 8];
    unsafe {
        let _ = libc::read(fd, val.as_mut_ptr() as *mut c_void, 8);
    }
}

/* ------------------------------------------------------------------ */
/* ShmemStream: tokio-postgres compatible duplex over the ring pair    */
/* ------------------------------------------------------------------ */

pub struct ShmemStream {
    slot: i32,
    slot_ptr: *mut ShmChannelSlot,
    /// Slot generation recorded at connect time.  The backend worker bumps
    /// it when a recycled slot is re-claimed, so a mismatch means the peer
    /// of THIS session is gone: EOF without any per-request liveness
    /// syscall (the old design paid one kill(pid, 0) per parked read).
    generation: u64,
    /// Readable when the backend produced response bytes (resp_efd).
    resp_afd: AsyncFd<RawFd>,
    /// Readable when the backend freed request-ring space (req_efd).
    req_afd: AsyncFd<RawFd>,
}

// The slot pointer lives in PostgreSQL static shared memory, stable for the
// process lifetime; access is atomic/fenced.  The eventfds were created by
// the postmaster and inherited through fork, so epoll on them is valid.
unsafe impl Send for ShmemStream {}
unsafe impl Sync for ShmemStream {}

impl ShmemStream {
    /// Claim one READY slot and wrap it as a duplex stream.  Returns None
    /// when the transport is disabled, no module is loaded, no slot is
    /// free (the caller falls back to the network transport), the slot
    /// fails the layout sanity gate, or the eventfds cannot be registered
    /// with the tokio reactor.
    pub fn connect_slot() -> Option<Self> {
        // Direct stderr diagnostics on every early-return path: silent None
        // returns made the transport bypass invisible in the PG log.
        if !transport_enabled() {
            eprintln!(
                "GW-SHM: connect_slot skip: TRANSPORT_ENABLED={} api_resolved={}",
                TRANSPORT_ENABLED.load(Ordering::SeqCst),
                api().is_some()
            );
            return None;
        }
        let a = api()?;
        let slot = unsafe { (a.try_attach)() };
        if slot < 0 {
            eprintln!("GW-SHM: try_attach returned {} (no READY slot or module disabled)", slot);
            return None;
        }
        let ptr = unsafe { (a.slot_ptr)(slot) as *mut ShmChannelSlot };
        if ptr.is_null() {
            unsafe { (a.detach)(slot) };
            return None;
        }

        /*
         * Layout sanity gate.  Any drift between the Rust mirror and the
         * kernel struct would turn every ring access below into arbitrary
         * memory corruption (ring.buf pointing anywhere).  Verify the
         * kernel-side buf pointers land exactly on this slot's own buffer
         * arrays and the sizes match, otherwise refuse to operate.
         */
        let (req_ok, resp_ok) = unsafe {
            let base = ptr as usize;
            let req_buf_off = (&(*ptr).req_buf as *const _ as usize) - base;
            let resp_buf_off = (&(*ptr).resp_buf as *const _ as usize) - base;
            let req = &(*ptr).req;
            let resp = &(*ptr).resp;
            (
                req.size as usize == REQ_RING_SIZE
                    && req.buf as usize == base + req_buf_off,
                resp.size as usize == RESP_RING_SIZE
                    && resp.buf as usize == base + resp_buf_off,
            )
        };
        if !req_ok || !resp_ok {
            unsafe {
                tracing::error!(
                    "shmem slot {} layout mismatch: req_ok={} resp_ok={} \
                     req.size={} req.buf={:#x} resp.size={} resp.buf={:#x}",
                    slot, req_ok, resp_ok,
                    (*ptr).req.size,
                    (*ptr).req.buf,
                    (*ptr).resp.size,
                    (*ptr).resp.buf
                );
                (a.detach)(slot);
            }
            return None;
        }

        let (resp_efd, req_efd) = unsafe { ((*ptr).resp_efd, (*ptr).req_efd) };
        if resp_efd < 0 || req_efd < 0 {
            eprintln!("GW-SHM: slot {} has invalid eventfds ({}, {})", slot, resp_efd, req_efd);
            unsafe { (a.detach)(slot) };
            return None;
        }
        let resp_afd = match AsyncFd::with_interest(resp_efd, Interest::READABLE) {
            Ok(a) => a,
            Err(e) => {
                eprintln!("GW-SHM: slot {} resp eventfd registration failed: {}", slot, e);
                unsafe { (a.detach)(slot) };
                return None;
            }
        };
        let req_afd = match AsyncFd::with_interest(req_efd, Interest::READABLE) {
            Ok(a) => a,
            Err(e) => {
                eprintln!("GW-SHM: slot {} req eventfd registration failed: {}", slot, e);
                unsafe { (a.detach)(slot) };
                return None;
            }
        };

        eprintln!("GW-SHM: slot {} claimed", slot);
        let generation = unsafe { (*ptr).generation.load(Ordering::Acquire) };
        Some(Self {
            slot,
            slot_ptr: ptr,
            generation,
            resp_afd,
            req_afd,
        })
    }

    unsafe fn resp_ring(&self) -> &ShmRing {
        &(*self.slot_ptr).resp
    }

    unsafe fn req_ring(&self) -> &ShmRing {
        &(*self.slot_ptr).req
    }

    /// EOF detection without syscalls: the slot's backend sets `closed` on
    /// a clean exit (and notifies both eventfds), while a recycled slot
    /// bumps `generation` when the fresh worker claims it.  Either way the
    /// session this stream belongs to is over.
    unsafe fn session_dead(&self) -> bool {
        let slot = &*self.slot_ptr;
        slot.resp.closed.load(Ordering::Acquire) != 0
            || slot.req.closed.load(Ordering::Acquire) != 0
            || slot.generation.load(Ordering::Acquire) != self.generation
    }

    /// Copy the available resp bytes into `buf` (single-copy fast path).
    /// Returns the number of bytes copied (>= 1).
    unsafe fn copy_resp_available(&self, buf: &mut ReadBuf<'_>) -> usize {
        let ring = self.resp_ring();
        let used = ring.used();
        let want = buf.remaining();
        let n = (used.min(want as u64)) as usize;
        let bptr = ring.buf as *const u8;
        let tail = ring.tail.load(Ordering::Acquire) as usize;
        let size = ring.size as usize;
        let pos = tail % size;
        let unfilled = buf.initialize_unfilled();
        let dst = unfilled.as_mut_ptr() as *mut u8;
        if pos + n <= size {
            std::ptr::copy_nonoverlapping(bptr.add(pos), dst, n);
        } else {
            let first = size - pos;
            std::ptr::copy_nonoverlapping(bptr.add(pos), dst, first);
            std::ptr::copy_nonoverlapping(bptr, dst.add(first), n - first);
        }
        buf.advance(n);
        ring.tail.store((tail as u64) + n as u64, Ordering::Release);

        // A backend parked on a full resp ring needs a SetLatch to resume;
        // its writer_parked flag is the Dekker marker for that.
        if ring.writer_parked.load(Ordering::Acquire) != 0 {
            ring.writer_parked.store(0, Ordering::SeqCst);
            if let Some(a) = api() {
                (a.wake_writer)(self.slot);
            }
        }
        n
    }
}

/// Release a claim whose session never established (connect error): mark
/// the request ring closed so the backend's blocking read returns EOF and
/// the worker recycles itself, then release the claim.
pub fn discard_slot(slot: i32) {
    if let Some(a) = api() {
        let ptr = unsafe { (a.slot_ptr)(slot) as *mut ShmChannelSlot };
        if !ptr.is_null() {
            unsafe {
                (*ptr).req.closed.store(1, Ordering::SeqCst);
                (*ptr).resp.closed.store(1, Ordering::SeqCst);
            }
        }
        unsafe { (a.detach)(slot) };
    }
}

impl Drop for ShmemStream {
    fn drop(&mut self) {
        // The connection task sends Terminate; the backend exits and frees
        // the slot itself.  Clear our Dekker park markers so the backend
        // never notifies a dead waiter, then let the AsyncFds deregister.
        unsafe {
            self.req_ring().writer_parked.store(0, Ordering::SeqCst);
            self.resp_ring().reader_parked.store(0, Ordering::SeqCst);
        }
    }
}

impl AsyncRead for ShmemStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        unsafe {
            let ring = self.resp_ring();

            // corruption gate: refuse to copy from an insane ring
            if ring.corrupted() {
                tracing::error!(
                    "shmem slot {} resp ring corrupted in poll_read: \
                     head={} tail={} size={} buf={:#x}",
                    self.slot,
                    ring.head.load(Ordering::Acquire),
                    ring.tail.load(Ordering::Acquire),
                    ring.size,
                    ring.buf
                );
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "shmem resp ring corrupted",
                )));
            }

            loop {
                // fast path: bytes already available
                if ring.used() > 0 {
                    self.copy_resp_available(buf);
                    return Poll::Ready(Ok(()));
                }

                if ring.closed.load(Ordering::Acquire) != 0 {
                    // EOF: peer released the session
                    return Poll::Ready(Ok(()));
                }

                // Wait for the backend's eventfd notification; epoll does
                // the parking, exactly like a socket read.
                match self.resp_afd.poll_read_ready(cx) {
                    Poll::Ready(Ok(guard)) => {
                        // drain the notification counter first, then drop
                        // the guard (clears readiness): a notify racing with
                        // the drain re-marks readability, none is lost
                        drain_eventfd(*self.resp_afd.get_ref());
                        drop(guard);
                        // loop: recheck the ring (data is likely present)
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {
                        if self.session_dead() {
                            // EOF: peer exited cleanly or the slot was
                            // recycled by a fresh worker
                            return Poll::Ready(Ok(()));
                        }
                        // lost-wake double check before sleeping
                        if ring.used() > 0 {
                            continue;
                        }
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

impl AsyncWrite for ShmemStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        unsafe {
            let ring = self.req_ring();

            // corruption gate: refuse to write into an insane ring
            if ring.corrupted() {
                tracing::error!(
                    "shmem slot {} req ring corrupted in poll_write: \
                     head={} tail={} size={} buf={:#x}",
                    self.slot,
                    ring.head.load(Ordering::Acquire),
                    ring.tail.load(Ordering::Acquire),
                    ring.size,
                    ring.buf
                );
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "shmem req ring corrupted",
                )));
            }

            loop {
                let free = ring.free();
                if free > 0 {
                    // we are about to proceed without waiting; make sure a
                    // stale park marker does not cause a pointless notify
                    if ring.writer_parked.load(Ordering::Acquire) != 0 {
                        ring.writer_parked.store(0, Ordering::SeqCst);
                    }

                    let n = buf.len().min(free as usize);
                    let head = ring.head.load(Ordering::Acquire) as usize;
                    let size = ring.size as usize;
                    let pos = head % size;
                    let bptr = ring.buf as *mut u8;
                    if pos + n <= size {
                        std::ptr::copy_nonoverlapping(buf.as_ptr(), bptr.add(pos), n);
                    } else {
                        let first = size - pos;
                        std::ptr::copy_nonoverlapping(buf.as_ptr(), bptr.add(pos), first);
                        std::ptr::copy_nonoverlapping(
                            buf.as_ptr().add(first),
                            bptr,
                            n - first,
                        );
                    }
                    std::sync::atomic::fence(Ordering::Release);
                    ring.head.store((head as u64) + n as u64, Ordering::Release);

                    // A latch-parked backend reader (Dekker marker) needs a
                    // SetLatch to resume.
                    if ring.reader_parked.load(Ordering::Acquire) != 0 {
                        ring.reader_parked.store(0, Ordering::SeqCst);
                        if let Some(a) = api() {
                            (a.wake_backend)(self.slot);
                        }
                    }
                    return Poll::Ready(Ok(n));
                }

                if ring.closed.load(Ordering::Acquire) != 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "shm channel closed",
                    )));
                }

                // Ring full (1 MB of outstanding request bytes): set the
                // marker the backend consumer checks after every consume,
                // then wait for its req_efd notification.
                ring.writer_parked.store(1, Ordering::SeqCst);
                std::sync::atomic::fence(Ordering::SeqCst);

                match self.req_afd.poll_read_ready(cx) {
                    Poll::Ready(Ok(guard)) => {
                        drain_eventfd(*self.req_afd.get_ref());
                        drop(guard);
                        ring.writer_parked.store(0, Ordering::SeqCst);
                        // loop: recheck free space
                    }
                    Poll::Ready(Err(e)) => {
                        ring.writer_parked.store(0, Ordering::SeqCst);
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        // Dekker recheck: the backend may have freed space
                        // between our fence and its flag read.
                        if ring.free() > 0 {
                            ring.writer_parked.store(0, Ordering::SeqCst);
                            continue;
                        }
                        if self.session_dead() {
                            ring.writer_parked.store(0, Ordering::SeqCst);
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "shm channel closed",
                            )));
                        }
                        // keep writer_parked set: the backend's next consume
                        // writes req_efd and epoll wakes this task
                        return Poll::Pending;
                    }
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Kept for diagnostics; hot-path liveness is decided by the slot
/// `closed` flag + `generation` counter (see `session_dead`), so no
/// kill(pid, 0) syscall is paid per request any more.
#[allow(dead_code)]
unsafe fn backend_alive(slot: i32) -> bool {
    match api() {
        Some(a) => (a.backend_alive)(slot) == 1,
        None => false,
    }
}

/* ------------------------------------------------------------------ */
/* deadpool Connect implementation: shm first, network fallback        */
/* ------------------------------------------------------------------ */

/// A deadpool `Connect` that first tries the shared-memory channel and
/// falls back to the wrapped configuration-based connector (loopback
/// socket) when no channel is available.  This keeps the original network
/// path fully operational and provides seamless degradation.
pub struct ShmemFirstConnect {
    fallback: Box<dyn Connect>,
}

impl ShmemFirstConnect {
    pub fn new(fallback: impl Connect + 'static) -> Self {
        Self {
            fallback: Box::new(fallback),
        }
    }
}

impl Connect for ShmemFirstConnect {
    fn connect(
        &self,
        pg_config: &tokio_postgres::Config,
    ) -> BoxFuture<
        '_,
        Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::Error>,
    > {
        // The future may only borrow &self (deadpool Connect trait bound),
        // so clone the config up front instead of capturing the reference.
        let pg_config = pg_config.clone();
        let fallback = &self.fallback;
        Box::pin(async move {
            if transport_enabled() {
                if let Some(stream) = ShmemStream::connect_slot() {
                    let slot = stream.slot;
                    match pg_config.connect_raw(stream, tokio_postgres::NoTls).await {
                        Ok((client, conn)) => {
                            eprintln!("GW-SHM: slot {} session established", slot);
                            let conn_task = tokio::spawn(async move {
                                if let Err(e) = conn.await {
                                    tracing::warn!(
                                        "shmem channel connection task failed: {}",
                                        e
                                    );
                                }
                            });
                            return Ok((client, conn_task));
                        }
                        Err(e) => {
                            eprintln!("GW-SHM: slot {} connect_raw failed, falling back to socket: {}", slot, e);
                            discard_slot(slot);
                        }
                    }
                }
            }
            fallback.connect(&pg_config).await
        })
    }
}
