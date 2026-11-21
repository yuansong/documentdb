/*-------------------------------------------------------------------------
 * shmem_transport.rs
 *   documentdb_gateway_core: shared-memory channel transport (client side).
 *
 *   Implements tokio_postgres' AsyncRead/AsyncWrite over the PG16 kernel
 *   shm_channel rings, so tokio-postgres can run its stock wire protocol
 *   (startup packet, SCRAM, extended protocol) through shared memory
 *   instead of a loopback socket.  The channel registry and the rings live
 *   in PostgreSQL's static shared memory (module pg_shm_channel, kernel
 *   commit "shm_channel: ..."); the C API is resolved lazily via dlsym so
 *   the gateway keeps working on clusters without the kernel module.
 *
 *   Parking protocol per ring (hybrid spin-then-park, measured 0.4 us/op
 *   on the busy path by ipc_bench):
 *   - the gateway parks in a dedicated watchdog thread via FUTEX_WAIT with
 *     timeout (so a dead backend can never hang the pool);
 *   - the backend parks on its MyLatch and is woken by SetLatch through
 *     shm_channel_wake_backend().
 *
 *   Copyright (c) 2026, yuanyuansong
 *-------------------------------------------------------------------------
 */

use std::{
    collections::HashMap,
    ffi::{c_int, c_void, CString},
    io,
    pin::Pin,
    sync::{
        atomic::{fence, AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context, Poll, Waker},
};

use deadpool_postgres::Connect;
use futures::future::BoxFuture;
use libc::{futex, FUTEX_WAIT, FUTEX_WAKE};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Ring sizes must match kernel/shm_channel.h (compile-time in the kernel).
const REQ_RING_SIZE: usize = 256 * 1024;
const RESP_RING_SIZE: usize = 1024 * 1024;

const SPIN_COUNT: usize = 2000;
const FUTEX_WAIT_MS: i64 = 200;

/* ------------------------------------------------------------------ */
/* Shared-memory layout mirrors of the kernel structures               */
/* ------------------------------------------------------------------ */

#[repr(C)]
struct ShmRing {
    head: AtomicU64,
    tail: AtomicU64,
    reader_parked: AtomicU32,
    writer_parked: AtomicU32,
    closed: AtomicU32,
    size: u32,
    buf: u64,
    reader_proc: u64,
    writer_proc: u64,
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
}

impl ShmRing {
    unsafe fn used(&self) -> u64 {
        self.head.load(Ordering::Acquire) - self.tail.load(Ordering::Acquire)
    }

    unsafe fn free(&self) -> u64 {
        (self.size as u64) - self.used()
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
}static API: OnceLock<Option<ShmApi>> = OnceLock::new();

fn api() -> Option<&'static ShmApi> {
    API
        .get_or_init(|| unsafe {
            let slot_ptr = dlsym_sym("shm_channel_slot_ptr")?;
            let try_attach = dlsym_sym("shm_channel_try_attach")?;
            let detach = dlsym_sym("shm_channel_detach")?;
            let backend_alive = dlsym_sym("shm_channel_backend_alive")?;
            let wake_backend = dlsym_sym("shm_channel_wake_backend")?;
            let slot_count = dlsym_sym("shm_channel_slot_count")?;
            Some(ShmApi {
                slot_ptr: std::mem::transmute(slot_ptr),
                try_attach: std::mem::transmute(try_attach),
                detach: std::mem::transmute(detach),
                backend_alive: std::mem::transmute(backend_alive),
                wake_backend: std::mem::transmute(wake_backend),
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

/* ------------------------------------------------------------------ */
/* Per-slot waker registry + watchdog                                  */
/* ------------------------------------------------------------------ */

struct SlotWatch {
    waker: Mutex<Option<Waker>>,
    shutdown: AtomicBool,
}

static WATCHES: OnceLock<Mutex<HashMap<i32, Arc<SlotWatch>>>> = OnceLock::new();

fn watches() -> &'static Mutex<HashMap<i32, Arc<SlotWatch>>> {
    WATCHES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn futex_wait(addr: &AtomicU64, expected: u32) {
    let ptr = (addr as *const AtomicU64) as *const u32;
    let ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: FUTEX_WAIT_MS * 1_000_000,
    };
    unsafe {
        futex(ptr as *const u32, FUTEX_WAIT, expected, &ts as *const _ as *const c_void, std::ptr::null_mut(), 0);
    }
}

fn futex_wake(addr: &AtomicU64) {
    let ptr = (addr as *const AtomicU64) as *const u32;
    unsafe {
        futex(ptr as *const u32, FUTEX_WAKE, 1, std::ptr::null(), std::ptr::null_mut(), 0);
    }
}

/*
 * Watchdog thread for one active slot: parks on the shared-memory futexes
 * exactly where the kernel C code expects its counterpart to be, and
 * forwards wakeups to the tokio waker.  One parked thread per active
 * connection (the kernel counterpart for socket reads is one parked
 * thread in epoll anyway).
 */
fn spawn_watchdog(slot: i32, slot_ptr: *mut ShmChannelSlot, watch: Arc<SlotWatch>) {
    tokio::task::spawn_blocking(move || unsafe {
        let slot = &*slot_ptr;
        while !watch.shutdown.load(Ordering::Acquire) {
            let resp_empty = slot.resp.used() == 0;
            let req_full = slot.req.free() == 0;

            let mut parked_resp = false;
            let mut parked_req = false;
            if resp_empty {
                slot.resp.reader_parked.store(1, Ordering::SeqCst);
                parked_resp = true;
            }
            if req_full {
                slot.req.writer_parked.store(1, Ordering::SeqCst);
                parked_req = true;
            }
            std::sync::atomic::fence(Ordering::SeqCst);

            let resp_empty = slot.resp.used() == 0;
            let req_full = slot.req.free() == 0;
            let closed = slot.resp.closed.load(Ordering::Acquire) != 0
                || slot.req.closed.load(Ordering::Acquire) != 0;

            if !closed && (parked_resp || parked_req) {
                let want = if parked_resp { resp_empty } else { req_full };
                if want {
                    // park on the cursor the producer will wake
                    if parked_resp {
                        let expected =
                            (slot.resp.head.load(Ordering::Acquire) & 0xFFFF_FFFF) as u32;
                        futex_wait(&slot.resp.head, expected);
                    } else {
                        let expected =
                            (slot.req.tail.load(Ordering::Acquire) & 0xFFFF_FFFF) as u32;
                        futex_wait(&slot.req.tail, expected);
                    }
                }
            }
            slot.resp.reader_parked.store(0, Ordering::SeqCst);
            slot.req.writer_parked.store(0, Ordering::SeqCst);

            let dead = (slot.resp.closed.load(Ordering::Acquire) != 0)
                || (slot.req.closed.load(Ordering::Acquire) != 0)
                || backend_alive(slot) == 0;

            if dead || slot.resp.used() > 0 || slot.req.free() > 0 {
                if let Some(w) = watch.waker.lock().unwrap().take() {
                    w.wake();
                }
            }
            if dead {
                // one more wake after a short grace so the stream observes EOF
                std::thread::sleep(std::time::Duration::from_millis(10));
                if let Some(w) = watch.waker.lock().unwrap().take() {
                    w.wake();
                }
                break;
            }
        }
    });
}

unsafe fn backend_alive(slot: i32) -> bool {
    match api() {
        Some(a) => (a.backend_alive)(slot) == 1,
        None => false,
    }
}

/* ------------------------------------------------------------------ */
/* ShmemStream: tokio-postgres compatible duplex over the ring pair    */
/* ------------------------------------------------------------------ */

pub struct ShmemStream {
    slot: i32,
    slot_ptr: *mut ShmChannelSlot,
    watch: Arc<SlotWatch>,
}

// The slot pointer lives in PostgreSQL static shared memory, stable for the
// process lifetime; access is atomic/fenced.  The watchdog thread owns the
// cross-thread waker registry.
unsafe impl Send for ShmemStream {}
unsafe impl Sync for ShmemStream {}

impl ShmemStream {
    /// Claim one READY slot and wrap it as a duplex stream.  Returns None
    /// when the transport is disabled, no module is loaded or no slot is
    /// free (the caller falls back to the network transport).
    pub fn connect_slot() -> Option<Self> {
        if !transport_enabled() {
            return None;
        }
        let a = api()?;
        let slot = unsafe { (a.try_attach)() };
        if slot < 0 {
            return None;
        }
        let ptr = unsafe { (a.slot_ptr)(slot) as *mut ShmChannelSlot };
        if ptr.is_null() {
            unsafe { (a.detach)(slot) };
            return None;
        }
        let watch = Arc::new(SlotWatch {
            waker: Mutex::new(None),
            shutdown: AtomicBool::new(false),
        });
        watches().lock().unwrap().insert(slot, Arc::clone(&watch));
        spawn_watchdog(slot, ptr, Arc::clone(&watch));
        Some(Self {
            slot,
            slot_ptr: ptr,
            watch,
        })
    }

    unsafe fn resp_ring(&self) -> &ShmRing {
        &(*self.slot_ptr).resp
    }

    unsafe fn req_ring(&self) -> &ShmRing {
        &(*self.slot_ptr).req
    }

    fn wake_me(&self) {
        if let Some(w) = self.watch.waker.lock().unwrap().take() {
            w.wake();
        }
    }

    fn register_waker(&self, cx: &mut Context<'_>) {
        let mut g = self.watch.waker.lock().unwrap();
        if g.as_ref().map(|w| !w.will_wake(cx.waker())).unwrap_or(true) {
            *g = Some(cx.waker().clone());
        }
    }
}

/// Release a claim whose session never established (connect error):
/// the backend is still waiting for its startup packet and can serve the
/// next client that claims the slot.
pub fn discard_slot(slot: i32) {
    if let Some(a) = api() {
        unsafe { (a.detach)(slot) };
    }
    if let Some(w) = watches().lock().unwrap().remove(&slot) {
        w.shutdown.store(true, Ordering::SeqCst);
        if let Some(w) = w.waker.lock().unwrap().take() {
            w.wake();
        }
    }
}

impl Drop for ShmemStream {
    fn drop(&mut self) {
        // The connection task sends Terminate; the backend exits and frees
        // the slot itself.  We only stop the watchdog.
        self.watch.shutdown.store(true, Ordering::SeqCst);
        self.wake_me();
        watches().lock().unwrap().remove(&self.slot);
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

            // fast path: bytes already available
            let used = ring.used();
            if used > 0 {
                let want = buf.remaining();
                let n = (used.min(want as u64)) as usize;
                let bptr = ring.buf as *const u8;
                let tail = ring.tail.load(Ordering::Acquire) as usize;
                let size = ring.size as usize;
                let pos = tail % size;
                let unfilled = buf.initialize_unfilled();
                if pos + n <= size {
                    let src = std::slice::from_raw_parts(bptr.add(pos), n);
                    std::mem::MaybeUninit::copy_from_slice(&mut unfilled[..n], src);
                } else {
                    let first = size - pos;
                    let (a, b) = unfilled.split_at_mut(first);
                    std::mem::MaybeUninit::copy_from_slice(
                        a,
                        std::slice::from_raw_parts(bptr.add(pos), first),
                    );
                    std::mem::MaybeUninit::copy_from_slice(
                        b,
                        std::slice::from_raw_parts(bptr, n - first),
                    );
                }
                buf.advance(n);
                ring.tail.store((tail as u64) + n as u64, Ordering::Release);
                if ring.writer_parked.load(Ordering::Acquire) != 0 {
                    ring.writer_parked.store(0, Ordering::SeqCst);
                    futex_wake(&ring.tail);
                }
                return Poll::Ready(Ok(()));
            }

            if ring.closed.load(Ordering::Acquire) != 0
                || !backend_alive(self.slot)
            {
                // EOF: peer gone
                return Poll::Ready(Ok(()));
            }

            // slow path: register waker and wait for the watchdog
            self.register_waker(cx);
            // double check to avoid a lost wake
            if ring.used() > 0 {
                self.wake_me();
            }
            Poll::Pending
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

            // active poll: a few spins, the backend usually drains instantly
            for _ in 0..SPIN_COUNT {
                let free = ring.free();
                if free > 0 {
                    break;
                }
                if ring.closed.load(Ordering::Acquire) != 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "shm channel closed",
                    )));
                }
                std::hint::spin_loop();
            }

            let free = ring.free();
            if free == 0 {
                if ring.closed.load(Ordering::Acquire) != 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "shm channel closed",
                    )));
                }
                self.register_waker(cx);
                if ring.free() > 0 {
                    self.wake_me();
                }
                return Poll::Pending;
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

            if ring.reader_parked.load(Ordering::Acquire) != 0 {
                ring.reader_parked.store(0, Ordering::SeqCst);
                // the reader is the backend: wake through its PGPROC latch
                if let Some(a) = api() {
                    (a.wake_backend)(self.slot);
                }
            }
            Poll::Ready(Ok(n))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
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
        let fallback = &self.fallback;
        Box::pin(async move {
            if transport_enabled() {
                if let Some(stream) = ShmemStream::connect_slot() {
                    let slot = stream.slot;
                    match pg_config.connect_raw(stream, tokio_postgres::NoTls).await {
                        Ok((client, conn)) => {
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
                            tracing::debug!(
                                "shmem connect failed, falling back to socket: {}",
                                e
                            );
                            discard_slot(slot);
                        }
                    }
                }
            }
            fallback.connect(pg_config).await
        })
    }
}
