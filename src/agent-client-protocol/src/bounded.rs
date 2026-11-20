//! Opt-in admission-bounded frame channels. Legacy [`crate::Channel`] is unchanged.
use crate::{Channel, ConnectTo, Error, Role, TransportFrame};
use futures::{FutureExt, Stream, StreamExt, channel::mpsc, future::BoxFuture, task::AtomicWaker};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    io::Write,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
};

/// Finite per-direction admission limits. Byte limits measure encoded JSON,
/// not allocator overhead or application-owned values/future captures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelLimits {
    /// Maximum encoded size of one frame.
    pub max_frame_bytes: usize,
    /// Maximum reserved encoded bytes, including queued and handed-off frames.
    pub max_buffered_bytes: usize,
    /// Maximum queued, preparing, or handed-off frames.
    pub max_buffered_frames: usize,
    /// Maximum requests awaiting a reply.
    pub max_pending_requests: usize,
    /// Maximum queued/running tasks and registered dynamic handlers (including
    /// the transport driver). Also bounds suspended [`BoundedSender::send`]
    /// registrations independently in each direction.
    pub max_tasks: usize,
}
impl Default for ChannelLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_buffered_bytes: 16 * 1024 * 1024,
            max_buffered_frames: 256,
            max_pending_requests: 256,
            max_tasks: 256,
        }
    }
}
impl ChannelLimits {
    /// Reject zero limits and a byte budget smaller than one maximum frame.
    pub fn validate(self) -> Result<Self, Error> {
        if self.max_frame_bytes == 0
            || self.max_buffered_bytes < self.max_frame_bytes
            || self.max_buffered_frames == 0
            || self.max_pending_requests == 0
            || self.max_tasks == 0
        {
            return Err(limit_error("invalid channel limits"));
        }
        Ok(self)
    }
}
fn limit_error(message: &str) -> Error {
    crate::util::internal_error(message)
}
#[derive(Default)]
struct Terminal {
    state: Mutex<TerminalState>,
}

#[derive(Default)]
struct TerminalState {
    error: Option<Error>,
    observers: HashMap<usize, Arc<AtomicWaker>>,
}

// Polled observers are registered until terminal failure or cancellation. The
// terminal error and retirement of all registrations commit under a single
// lock; the actual wakers (including reentrant drops) run unlocked.
#[derive(Debug)]
struct Failure {
    terminal: Weak<Terminal>,
    observer: Arc<AtomicWaker>,
}

impl Failure {
    fn id(&self) -> usize {
        Arc::as_ptr(&self.observer) as usize
    }
}

impl Future for Failure {
    type Output = Error;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Error> {
        self.observer.register(cx.waker());
        let Some(terminal) = self.terminal.upgrade() else {
            return Poll::Ready(limit_error("bounded channel closed"));
        };
        let mut state = terminal.state.lock().expect("terminal mutex poisoned");
        if let Some(error) = &state.error {
            Poll::Ready(error.clone())
        } else {
            state
                .observers
                .entry(self.id())
                .or_insert_with(|| self.observer.clone());
            Poll::Pending
        }
    }
}

impl Drop for Failure {
    fn drop(&mut self) {
        if let Some(terminal) = self.terminal.upgrade() {
            let retired = terminal
                .state
                .lock()
                .expect("terminal mutex poisoned")
                .observers
                .remove(&self.id());
            drop(retired);
        }
    }
}

fn wake_observers(observers: HashMap<usize, Arc<AtomicWaker>>) {
    struct WakeObserver(Arc<AtomicWaker>);
    impl Drop for WakeObserver {
        fn drop(&mut self) {
            self.0.wake();
        }
    }
    // Vec drop glue still visits the remaining observers if one unwinds.
    drop(
        observers
            .into_values()
            .map(WakeObserver)
            .collect::<Vec<_>>(),
    );
}

impl Drop for Terminal {
    fn drop(&mut self) {
        let state = self.state.get_mut().expect("terminal mutex poisoned");
        wake_observers(std::mem::take(&mut state.observers));
    }
}

impl Terminal {
    fn subscribe(self: &Arc<Self>) -> Failure {
        Failure {
            terminal: Arc::downgrade(self),
            observer: Arc::default(),
        }
    }

    fn fail(&self, message: &str) -> Error {
        let error = limit_error(message);
        let observers = {
            let mut state = self.state.lock().expect("terminal mutex poisoned");
            if let Some(first) = &state.error {
                return first.clone();
            }
            state.error = Some(error.clone());
            std::mem::take(&mut state.observers)
        };
        wake_observers(observers);
        error
    }

    fn check(&self) -> Result<(), Error> {
        match &self.state.lock().expect("terminal mutex poisoned").error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}
#[derive(Default)]
struct Usage {
    frames: usize,
    bytes: usize,
    tasks: usize,
    admission_closed: bool,
    protocol_closed: bool,
    receiver_closed: bool,
    waiters: VecDeque<Arc<AdmissionWaiter>>,
}
#[derive(Clone)]
pub(crate) struct Budget {
    limits: ChannelLimits,
    usage: Arc<Mutex<Usage>>,
    terminal: Arc<Terminal>,
}
impl std::fmt::Debug for Budget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Budget")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl Budget {
    pub(crate) fn limits(&self) -> ChannelLimits {
        self.limits
    }
    pub(crate) fn fail(&self, message: &str) -> Error {
        self.terminal.fail(message)
    }
    pub(crate) fn check(&self) -> Result<(), Error> {
        self.terminal.check()
    }
    pub(crate) fn check_admission(&self) -> Result<(), Error> {
        self.check()?;
        if self
            .usage
            .lock()
            .expect("budget mutex poisoned")
            .admission_closed
        {
            return Err(limit_error("bounded channel admission closed"));
        }
        Ok(())
    }
    fn close_admission(&self) -> Vec<WakeOnDrop> {
        let mut used = self.usage.lock().expect("budget mutex poisoned");
        used.admission_closed = true;
        used.waiters.iter().cloned().map(WakeOnDrop).collect()
    }
    async fn reserve_wait(&self) -> Result<Charge, Error> {
        self.check_admission()?;
        let mut admission = Admission {
            budget: self.clone(),
            waiter: Arc::new(AdmissionWaiter {
                wake: futures::task::AtomicWaker::new(),
            }),
            failure: self.terminal.subscribe(),
        };
        {
            let mut used = self.usage.lock().expect("budget mutex poisoned");
            if used.waiters.len() >= self.limits.max_tasks {
                return Err(limit_error("bounded channel waiting send limit exhausted"));
            }
            used.waiters.push_back(admission.waiter.clone());
        }
        futures::future::poll_fn(|cx| admission.poll(cx)).await
    }

    pub(crate) fn failure(&self) -> BoxFuture<'static, Error> {
        self.terminal.subscribe().boxed()
    }
    pub(crate) fn close_protocol(&self) {
        self.usage
            .lock()
            .expect("budget mutex poisoned")
            .protocol_closed = true;
    }
    pub(crate) fn reserve_protocol(&self) -> Result<Charge, Error> {
        self.reserve_frame(true)
    }
    pub(crate) fn reserve(&self) -> Result<Charge, Error> {
        self.reserve_frame(false)
    }
    fn reserve_frame(&self, protocol: bool) -> Result<Charge, Error> {
        self.check()?;
        let mut used = self.usage.lock().expect("budget mutex poisoned");
        if used.admission_closed || (protocol && used.protocol_closed) {
            return Err(limit_error("bounded channel admission closed"));
        }
        let bytes = self.limits.max_frame_bytes;
        if used.frames >= self.limits.max_buffered_frames
            || bytes > self.limits.max_buffered_bytes - used.bytes
        {
            drop(used);
            return Err(self.fail("bounded channel frame/byte admission exhausted"));
        }
        used.frames += 1;
        used.bytes += bytes;
        Ok(Charge(Arc::new(Permit {
            budget: self.clone(),
            bytes,
            task: false,
        })))
    }
    pub(crate) fn task(&self) -> Result<Charge, Error> {
        self.check()?;
        let mut used = self.usage.lock().expect("budget mutex poisoned");
        if used.admission_closed {
            return Err(limit_error("bounded channel admission closed"));
        }
        if used.tasks >= self.limits.max_tasks {
            drop(used);
            return Err(self.fail("bounded channel task admission exhausted"));
        }
        used.tasks += 1;
        Ok(Charge(Arc::new(Permit {
            budget: self.clone(),
            bytes: 0,
            task: true,
        })))
    }
}
struct AdmissionWaiter {
    wake: futures::task::AtomicWaker,
}

// Vec drop glue visits the remaining registrations if one wake unwinds. This
// keeps close broadcasts complete without catching panics in production.
struct WakeOnDrop(Arc<AdmissionWaiter>);
impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.wake.wake();
    }
}

// The queue owns only bounded waiter registrations, never caller frames. FIFO
// admission and cancellation update Usage atomically; callbacks run unlocked.
struct Admission {
    budget: Budget,
    waiter: Arc<AdmissionWaiter>,
    failure: Failure,
}
impl Admission {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Charge, Error>> {
        if let Poll::Ready(error) = Pin::new(&mut self.failure).poll(cx) {
            return Poll::Ready(Err(error));
        }
        self.waiter.wake.register(cx.waker());
        let mut used = self.budget.usage.lock().expect("budget mutex poisoned");
        if used.admission_closed {
            return Poll::Ready(Err(limit_error("bounded channel admission closed")));
        }
        if used.receiver_closed {
            drop(used);
            return Poll::Ready(Err(self.budget.fail("bounded channel receiver closed")));
        }
        let bytes = self.budget.limits.max_frame_bytes;
        if !used
            .waiters
            .front()
            .is_some_and(|w| Arc::ptr_eq(w, &self.waiter))
            || used.frames >= self.budget.limits.max_buffered_frames
            || bytes > self.budget.limits.max_buffered_bytes - used.bytes
        {
            return Poll::Pending;
        }
        let retired = used.waiters.pop_front();
        used.frames += 1;
        used.bytes += bytes;
        let next = used.waiters.front().cloned();
        drop(used);
        drop(retired);
        let charge = Charge(Arc::new(Permit {
            budget: self.budget.clone(),
            bytes,
            task: false,
        }));
        if let Some(next) = next {
            next.wake.wake();
        }
        Poll::Ready(Ok(charge))
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        let (retired, next) = {
            let mut used = self.budget.usage.lock().expect("budget mutex poisoned");
            let retired = used
                .waiters
                .iter()
                .position(|w| Arc::ptr_eq(w, &self.waiter))
                .and_then(|index| used.waiters.remove(index));
            (retired, used.waiters.front().cloned())
        };
        drop(retired);
        if let Some(next) = next {
            next.wake.wake();
        }
    }
}

struct Permit {
    budget: Budget,
    bytes: usize,
    task: bool,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut used = self.budget.usage.lock().expect("budget mutex poisoned");
        if self.task {
            used.tasks -= 1;
        } else {
            used.frames -= 1;
            used.bytes -= self.bytes;
        }
        let next = used.waiters.front().cloned();
        drop(used);
        if let Some(next) = next {
            next.wake.wake();
        }
    }
}
#[derive(Clone)]
pub(crate) struct Charge(Arc<Permit>);
impl std::fmt::Debug for Charge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Charge").finish_non_exhaustive()
    }
}

/// Serialized frame with admission ownership. Receiving or forwarding it does
/// not release its reservation. Retain it until body handoff/drop, which is NOT
/// an acknowledgment of socket flush, peer parsing, or protocol dispatch.
pub struct ChargedFrame {
    bytes: Box<[u8]>,
    pub(crate) charges: Vec<Charge>,
}
impl std::fmt::Debug for ChargedFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChargedFrame")
            .field("encoded_len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
impl ChargedFrame {
    /// Borrow the charged JSON bytes without releasing admission.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Decode while retaining admission. Decoded JSON has additional structural
    /// overhead; consumers must not accumulate uncharged decoded copies.
    #[must_use]
    pub fn decode(&self) -> TransportFrame {
        TransportFrame::parse_json(
            std::str::from_utf8(&self.bytes).expect("serialized frame is UTF-8"),
        )
    }
}

/// Cloneable fail-fast producer. There is no waiting-producer queue.
#[derive(Clone)]
pub struct BoundedSender {
    tx: Arc<SendQueue>,
    pub(crate) budget: Budget,
}
// The raw queue is polled with a noop waker. The exposed receiver registers
// separately, so enqueueing and invoking arbitrary receiver code can be split.
struct SendQueue {
    tx: mpsc::UnboundedSender<ChargedFrame>,
    ready: Arc<AtomicWaker>,
}

impl SendQueue {
    fn close(&self) {
        self.tx.close_channel();
        self.ready.wake();
    }
}

impl Drop for SendQueue {
    fn drop(&mut self) {
        self.close();
    }
}

/// A serialized frame with reserved admission and deferred receiver notification.
/// Keep the token outside the scope of any external lock used during enqueue.
#[derive(Debug)]
#[must_use = "enqueue the prepared frame, then drop the token outside external locks"]
pub struct PreparedBoundedFrame {
    sender: BoundedSender,
    frame: Option<ChargedFrame>,
    enqueued: bool,
    receiver_lost: bool,
}

impl PreparedBoundedFrame {
    /// Enqueue at most once without invoking receiver or admission callbacks.
    /// If enqueue is rejected, the reservation remains in this token until it
    /// is dropped. The receiver can observe committed output independently of
    /// the deferred wake; publish any required bookkeeping before unlocking.
    pub fn enqueue(&mut self) -> Result<(), Error> {
        let sender = &self.sender;
        sender.budget.check_admission()?;
        let Some(frame) = self.frame.take() else {
            return Err(limit_error("bounded channel frame already enqueued"));
        };
        match sender.tx.tx.unbounded_send(frame) {
            Ok(()) => {
                self.enqueued = true;
                Ok(())
            }
            Err(error) => {
                self.frame = Some(error.into_inner());
                if let Err(error) = sender.budget.check_admission() {
                    Err(error)
                } else {
                    self.receiver_lost = true;
                    Err(limit_error("bounded channel receiver closed"))
                }
            }
        }
    }
}

impl Drop for PreparedBoundedFrame {
    fn drop(&mut self) {
        if self.receiver_lost {
            drop(self.sender.fail("bounded channel receiver closed"));
        } else if self.enqueued {
            self.sender.tx.ready.wake();
        }
    }
}

impl std::fmt::Debug for BoundedSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedSender")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}
impl BoundedSender {
    /// Validated per-direction limits.
    #[must_use]
    pub fn limits(&self) -> ChannelLimits {
        self.budget.limits
    }
    /// Resolve when any admission/serialization failure makes this channel terminal.
    pub fn failure(&self) -> BoxFuture<'static, Error> {
        self.budget.failure()
    }
    /// Fail all escaped producers and wake the connection driver.
    #[allow(
        clippy::must_use_candidate,
        reason = "The primary effect is terminating the channel; callers may optionally propagate the error."
    )]
    pub fn fail(&self, reason: &str) -> Error {
        self.budget.fail(reason)
    }
    /// Gracefully close this direction for every sender clone.
    ///
    /// New sends fail without making the channel terminal. Already enqueued
    /// frames retain their charges and remain available to the receiver, which
    /// observes EOF after draining them. This does not signal [`Self::failure`]
    /// or close the opposite direction.
    pub fn close_channel(&self) {
        struct CloseQueue<'a>(&'a SendQueue);
        impl Drop for CloseQueue<'_> {
            fn drop(&mut self) {
                self.0.close();
            }
        }
        // Seal admission first; queue closure still runs if a waiter wake
        // unwinds. Both waiter and receiver callbacks run outside Usage.
        let close = CloseQueue(&self.tx);
        drop(self.budget.close_admission());
        drop(close);
    }
    /// Admit and serialize without allocating an unbounded intermediate String.
    pub fn try_send(&self, frame: TransportFrame) -> Result<(), Error> {
        let charge = self.budget.reserve()?;
        self.send_charged(frame, vec![charge])
    }
    /// Wait for frame/byte admission, then serialize and enqueue one frame.
    ///
    /// Unlike [`Self::try_send`], temporary frame/byte exhaustion is not terminal.
    /// Waiting sends are FIFO relative to other waiting sends; fail-fast sends
    /// retain their existing behavior and do not join this queue. At most
    /// [`ChannelLimits::max_tasks`] sends may wait per direction; an additional
    /// waiter is rejected without terminating the channel. Caller-owned frames
    /// held by send futures are not serialized or included in wire-byte budgets.
    ///
    /// Dropping this future before admission removes its waiter and sends nothing.
    /// There is no suspension after admission. Closure, terminal failure, or
    /// receiver loss ends the wait. Callers must provide their own stall deadline
    /// and continue driving the consumer concurrently to make progress.
    pub async fn send(&self, frame: TransportFrame) -> Result<(), Error> {
        let charge = self.budget.reserve_wait().await?;
        self.send_charged(frame, vec![charge])
    }
    /// Copy already serialized wire text after admission and size validation.
    /// Like TransportFrame::parse_json, malformed wire text is preserved.
    pub fn try_send_serialized(&self, json: &str) -> Result<(), Error> {
        self.prepare_serialized(json)?.enqueue()
    }
    /// Reserve and copy serialized text without making it visible to the receiver.
    /// This phase can report terminal failures and invoke wakers; call it outside
    /// external locks. Follow with [`PreparedBoundedFrame::enqueue`], then drop
    /// the token outside the lock to publish readiness or release admission.
    pub fn prepare_serialized(&self, json: &str) -> Result<PreparedBoundedFrame, Error> {
        let charge = self.budget.reserve()?;
        if json.len() > self.limits().max_frame_bytes {
            return Err(self.fail("bounded channel frame too large"));
        }
        Ok(self.prepare_frame(ChargedFrame {
            bytes: json.as_bytes().into(),
            charges: vec![charge],
        }))
    }
    /// Forward admission ownership without releasing it at an internal handoff.
    /// Cross-budget forwarding acquires destination admission before releasing
    /// the original reservation. Same-budget forwarding does not double charge.
    pub fn try_forward(&self, mut frame: ChargedFrame) -> Result<(), Error> {
        self.budget.check_admission()?;
        if frame.bytes.len() > self.limits().max_frame_bytes {
            return Err(self.fail("bounded channel forwarded frame too large"));
        }
        if !frame
            .charges
            .iter()
            .any(|c| Arc::ptr_eq(&c.0.budget.usage, &self.budget.usage))
        {
            let charge = self.budget.reserve()?;
            frame.charges = vec![charge];
        }
        self.enqueue(frame)
    }
    fn prepare_frame(&self, frame: ChargedFrame) -> PreparedBoundedFrame {
        PreparedBoundedFrame {
            sender: self.clone(),
            frame: Some(frame),
            enqueued: false,
            receiver_lost: false,
        }
    }
    fn enqueue(&self, frame: ChargedFrame) -> Result<(), Error> {
        self.prepare_frame(frame).enqueue()
    }
    pub(crate) fn send_charged(
        &self,
        frame: TransportFrame,
        charges: Vec<Charge>,
    ) -> Result<(), Error> {
        let mut charges = charges;
        if !charges
            .iter()
            .any(|c| Arc::ptr_eq(&c.0.budget.usage, &self.budget.usage))
        {
            charges.push(self.budget.reserve()?);
        }
        self.budget.check_admission()?;
        let bytes = serialize_frame(&frame, self.limits().max_frame_bytes)
            .map_err(|_| self.fail("bounded channel frame serialization exceeded limit"))?;
        self.enqueue(ChargedFrame { bytes, charges })
    }
}
/// Single consumer of serialized, charged frames.
#[derive(Debug)]
pub struct BoundedReceiver {
    rx: mpsc::UnboundedReceiver<ChargedFrame>,
    ready: Arc<AtomicWaker>,
    budget: Budget,
    failure: Failure,
}
impl BoundedReceiver {
    /// Gracefully seal this receive direction against further admission.
    ///
    /// New sends, including sends through cloned producers, are rejected and
    /// waiting sends are woken. Already accepted frames remain available from
    /// this receiver and retain their admission charges until released. Continue
    /// polling the receiver to drain those frames before it returns `None`.
    ///
    /// Closing is idempotent. It does not signal terminal failure or close the
    /// opposite direction of the channel.
    pub fn close(&mut self) {
        // Close the queue even if a registered waker unwinds.
        struct CloseQueue<'a>(&'a mut mpsc::UnboundedReceiver<ChargedFrame>);
        impl Drop for CloseQueue<'_> {
            fn drop(&mut self) {
                self.0.close();
            }
        }
        let close = CloseQueue(&mut self.rx);
        drop(self.budget.close_admission());
        drop(close);
    }
}
impl Drop for BoundedReceiver {
    fn drop(&mut self) {
        let waiters = {
            let mut used = self.budget.usage.lock().expect("budget mutex poisoned");
            used.receiver_closed = true;
            used.waiters
                .iter()
                .cloned()
                .map(WakeOnDrop)
                .collect::<Vec<_>>()
        };
        drop(waiters);
    }
}
impl Stream for BoundedReceiver {
    type Item = ChargedFrame;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut queue_context = Context::from_waker(futures::task::noop_waker_ref());
        if Pin::new(&mut self.failure).poll(cx).is_ready() {
            self.rx.close();
            while let Poll::Ready(Some(_)) = Pin::new(&mut self.rx).poll_next(&mut queue_context) {}
            return Poll::Ready(None);
        }
        self.ready.register(cx.waker());
        Pin::new(&mut self.rx).poll_next(&mut queue_context)
    }
}
/// Opt-in bounded frame endpoint; no concrete unbounded sender is exposed.
#[derive(Debug)]
pub struct BoundedChannel {
    /// Admission-bounded producer with fail-fast and waiting send APIs.
    pub tx: BoundedSender,
    /// Charged frames received from the peer.
    pub rx: BoundedReceiver,
}
impl BoundedChannel {
    /// Construct two endpoints with independent directional budgets and shared
    /// terminal state. Limits must be nonzero and fit one maximum-sized frame.
    pub fn duplex(limits: ChannelLimits) -> Result<(Self, Self), Error> {
        let limits = limits.validate()?;
        let terminal = Arc::<Terminal>::default();
        let a = Budget {
            limits,
            usage: Arc::default(),
            terminal: terminal.clone(),
        };
        let b = Budget {
            limits,
            usage: Arc::default(),
            terminal,
        };
        let (atx, brx) = mpsc::unbounded();
        let (btx, arx) = mpsc::unbounded();
        let a_ready = Arc::<AtomicWaker>::default();
        let b_ready = Arc::<AtomicWaker>::default();
        Ok((
            Self {
                tx: BoundedSender {
                    tx: Arc::new(SendQueue {
                        tx: atx,
                        ready: a_ready.clone(),
                    }),
                    budget: a.clone(),
                },
                rx: BoundedReceiver {
                    rx: arx,
                    ready: b_ready.clone(),
                    failure: b.terminal.subscribe(),
                    budget: b.clone(),
                },
            },
            Self {
                tx: BoundedSender {
                    tx: Arc::new(SendQueue {
                        tx: btx,
                        ready: b_ready,
                    }),
                    budget: b,
                },
                rx: BoundedReceiver {
                    rx: brx,
                    ready: a_ready,
                    failure: a.terminal.subscribe(),
                    budget: a,
                },
            },
        ))
    }
    /// Validated per-direction limits.
    #[must_use]
    pub fn limits(&self) -> ChannelLimits {
        self.tx.limits()
    }
}
impl BoundedChannel {
    /// Drive owned work concurrently, closing admission and draining accepted
    /// output when it succeeds. No driver means independent half-close lifetime.
    async fn copy_with_driver(
        mut self,
        driver: &mut Option<crate::ConnectionDriver>,
    ) -> Result<(), Error> {
        use futures::future::Either;
        loop {
            let event = futures::future::poll_fn(|cx| {
                if let Some(work) = driver.as_mut()
                    && let Poll::Ready(result) = Pin::new(work).poll(cx)
                {
                    return Poll::Ready(Either::Left(result));
                }
                self.rx.poll_next_unpin(cx).map(Either::Right)
            })
            .await;
            match event {
                Either::Left(result) => {
                    drop(driver.take());
                    result?;
                    self.rx.close();
                }
                Either::Right(Some(frame)) => self.tx.try_forward(frame)?,
                Either::Right(None) => break,
            }
        }
        self.rx.budget.check()
    }
}

/// Additive transport boundary. The legacy variant has no admission guarantee.
#[derive(Debug)]
pub enum TransportChannel {
    /// Compatibility-only, unbounded endpoint.
    Legacy(Channel),
    /// Opt-in admission-bounded endpoint.
    Bounded(BoundedChannel),
}
impl<R: Role> ConnectTo<R> for BoundedChannel {
    async fn connect_to(self, client: impl ConnectTo<R::Counterpart>) -> Result<(), Error> {
        async fn copy(mut rx: BoundedReceiver, tx: BoundedSender) -> Result<(), Error> {
            while let Some(frame) = rx.next().await {
                tx.try_forward(frame)?;
            }
            rx.budget.check()
        }
        let (other, mut driver) = client.into_bounded_channel_and_future(self.limits())?;
        let passive = driver.is_none();
        let mut finish = driver
            .as_mut()
            .and_then(crate::ConnectionDriver::take_finish);
        let result = {
            let outgoing = Box::pin(
                BoundedChannel {
                    rx: other.rx,
                    tx: self.tx,
                }
                .copy_with_driver(&mut driver),
            );
            let incoming = Box::pin(copy(self.rx, other.tx));
            if passive {
                futures::try_join!(outgoing, incoming).map(|_| ())
            } else {
                match futures::future::select(outgoing, incoming).await {
                    futures::future::Either::Left((result, _)) => result,
                    futures::future::Either::Right((result, outgoing)) => {
                        if let Some(finish) = &mut finish {
                            finish.request();
                        }
                        if result.is_ok() {
                            outgoing.await
                        } else {
                            result
                        }
                    }
                }
            }
        };
        if let Some(driver) = driver {
            if let Some(finish) = &mut finish {
                finish.request();
            }
            if result.is_ok() || finish.is_some() {
                return result.and(driver.await);
            }
        }
        result
    }

    fn into_channel_and_future(self) -> (Channel, Option<crate::ConnectionDriver>) {
        drop(self);
        let (channel, peer) = Channel::duplex();
        drop(peer);
        (
            channel,
            Some(crate::ConnectionDriver::new(async {
                Err(limit_error(
                    "bounded channel cannot be extracted as legacy Channel",
                ))
            })),
        )
    }
    fn into_transport_and_future(self) -> (TransportChannel, Option<crate::ConnectionDriver>) {
        (TransportChannel::Bounded(self), None)
    }
    fn into_bounded_channel_and_future(
        self,
        limits: ChannelLimits,
    ) -> Result<(BoundedChannel, Option<crate::ConnectionDriver>), Error> {
        let limits = limits.validate()?;
        if self.limits() != limits {
            return Err(self
                .tx
                .fail("bounded endpoint limits differ from requested limits"));
        }
        Ok((self, None))
    }
}

pub(crate) struct CappedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl CappedWriter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
    pub(crate) fn finish(self) -> Box<[u8]> {
        self.bytes.into_boxed_slice()
    }
}
impl Write for CappedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other("encoded JSON limit exceeded"));
        }
        self.bytes
            .try_reserve_exact(buf.len())
            .map_err(std::io::Error::other)?;
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(crate) fn serialize<T: serde::Serialize + ?Sized>(
    value: &T,
    limit: usize,
) -> Result<Box<[u8]>, Error> {
    let mut out = CappedWriter::new(limit);
    serde_json::to_writer(&mut out, value)?;
    Ok(out.finish())
}
fn serialize_frame(frame: &TransportFrame, limit: usize) -> Result<Box<[u8]>, Error> {
    match frame {
        TransportFrame::Single(value) => serialize(value, limit),
        TransportFrame::Malformed { raw, .. } => {
            if raw.len() > limit {
                return Err(limit_error("frame too large"));
            }
            Ok(raw.as_bytes().into())
        }
    }
}

#[derive(Debug)]
pub(crate) struct IncomingFrame {
    pub(crate) frame: TransportFrame,
    pub(crate) charges: Vec<Charge>,
}
impl From<TransportFrame> for IncomingFrame {
    fn from(frame: TransportFrame) -> Self {
        Self {
            frame,
            charges: vec![],
        }
    }
}
impl From<ChargedFrame> for IncomingFrame {
    fn from(frame: ChargedFrame) -> Self {
        Self {
            frame: frame.decode(),
            charges: frame.charges,
        }
    }
}
pub(crate) enum TransportSender {
    Legacy(mpsc::UnboundedSender<TransportFrame>),
    Bounded(BoundedSender),
}
impl From<mpsc::UnboundedSender<TransportFrame>> for TransportSender {
    fn from(tx: mpsc::UnboundedSender<TransportFrame>) -> Self {
        Self::Legacy(tx)
    }
}
impl TransportSender {
    pub(crate) fn close_protocol(&self) {
        if let Self::Bounded(tx) = self {
            tx.budget.close_protocol();
        }
    }
    pub(crate) fn finish(self) {
        if let Self::Bounded(tx) = self {
            tx.close_channel();
        }
    }
    pub(crate) fn send(&self, frame: TransportFrame, charges: Vec<Charge>) -> Result<(), Error> {
        match self {
            Self::Legacy(tx) => tx
                .unbounded_send(frame)
                .map_err(crate::util::internal_error),
            Self::Bounded(tx) => tx.send_charged(frame, charges),
        }
    }
}
pub(crate) enum TransportReceiver {
    Legacy(mpsc::UnboundedReceiver<TransportFrame>),
    Bounded(BoundedReceiver),
}
impl TransportReceiver {
    pub(crate) fn close(&mut self) {
        match self {
            Self::Legacy(rx) => rx.close(),
            Self::Bounded(rx) => rx.close(),
        }
    }
}
impl Stream for TransportReceiver {
    type Item = IncomingFrame;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Legacy(rx) => Pin::new(rx)
                .poll_next(cx)
                .map(|item| item.map(IncomingFrame::from)),
            Self::Bounded(rx) => Pin::new(rx)
                .poll_next(cx)
                .map(|item| item.map(IncomingFrame::from)),
        }
    }
}

impl TransportChannel {
    pub(crate) fn split(self) -> (TransportReceiver, TransportSender, Option<Budget>) {
        match self {
            Self::Legacy(channel) => (
                TransportReceiver::Legacy(channel.rx),
                TransportSender::Legacy(channel.tx),
                None,
            ),
            Self::Bounded(channel) => {
                let budget = channel.tx.budget.clone();
                (
                    TransportReceiver::Bounded(channel.rx),
                    TransportSender::Bounded(channel.tx),
                    Some(budget),
                )
            }
        }
    }
}

pub(crate) struct DriverLifetime {
    budget: Budget,
    graceful: bool,
}
impl DriverLifetime {
    pub(crate) fn new(budget: Budget) -> Self {
        Self {
            budget,
            graceful: false,
        }
    }
    pub(crate) fn finish_gracefully(&mut self) {
        self.budget.close_admission();
        self.graceful = true;
    }
}
impl Drop for DriverLifetime {
    fn drop(&mut self) {
        if !self.graceful {
            self.budget.fail("bounded protocol driver stopped");
        }
    }
}

#[cfg(test)]
impl Budget {
    pub(crate) fn snapshot(&self) -> (usize, usize, usize) {
        let used = self.usage.lock().unwrap();
        (used.frames, used.bytes, used.tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> ChannelLimits {
        ChannelLimits {
            max_frame_bytes: 64,
            max_buffered_bytes: 128,
            max_buffered_frames: 2,
            max_pending_requests: 2,
            max_tasks: 2,
        }
    }

    fn small_frame(id: usize) -> TransportFrame {
        TransportFrame::parse_json(&format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{{}}}}"#))
    }

    #[test]
    fn receiver_close_seals_admission_and_preserves_both_directions_frames() {
        let (mut a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let producer = a.tx.clone();
        a.tx.try_send(small_frame(1)).unwrap();
        b.rx.close();
        b.rx.close();
        assert!(producer.try_send(small_frame(2)).is_err());
        assert!(a.tx.failure().now_or_never().is_none());
        assert!(b.rx.next().now_or_never().unwrap().is_some());
        assert!(b.rx.next().now_or_never().unwrap().is_none());
        b.tx.try_send(small_frame(3)).unwrap();
        assert!(a.rx.next().now_or_never().unwrap().is_some());
        assert!(b.tx.failure().now_or_never().is_none());
    }

    #[test]
    fn owned_driver_completion_drains_accepted_output_with_live_producers() {
        let (source, source_peer) = BoundedChannel::duplex(limits()).unwrap();
        let (destination, mut destination_peer) = BoundedChannel::duplex(limits()).unwrap();
        source_peer.tx.try_send(small_frame(1)).unwrap();
        let bridge = BoundedChannel {
            rx: source.rx,
            tx: destination.tx,
        };
        let driver = crate::ConnectionDriver::new(async { Ok(()) });
        assert!(
            bridge
                .copy_with_driver(&mut Some(driver))
                .now_or_never()
                .unwrap()
                .is_ok()
        );
        assert!(destination_peer.rx.next().now_or_never().unwrap().is_some());
        assert!(source_peer.tx.try_send(small_frame(2)).is_err());
        assert!(source_peer.tx.failure().now_or_never().is_none());
    }

    #[tokio::test]
    async fn bounded_bridge_requests_and_awaits_cooperative_finish() {
        use crate::{ConnectionDriver, UntypedRole};
        use std::{
            sync::atomic::{AtomicBool, Ordering},
            time::Duration,
        };

        struct Owned {
            endpoint: BoundedChannel,
            driver: ConnectionDriver,
        }
        impl ConnectTo<UntypedRole> for Owned {
            async fn connect_to(self, _: impl ConnectTo<UntypedRole>) -> Result<(), Error> {
                unreachable!("test extracts bounded transport")
            }
            fn into_bounded_channel_and_future(
                self,
                _: ChannelLimits,
            ) -> Result<(BoundedChannel, Option<ConnectionDriver>), Error> {
                Ok((self.endpoint, Some(self.driver)))
            }
        }

        for failed in [false, true] {
            let (bridge, peer) = BoundedChannel::duplex(limits()).unwrap();
            let (endpoint, remote) = BoundedChannel::duplex(limits()).unwrap();
            let cleaned = Arc::new(AtomicBool::new(false));
            let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
            let driver = ConnectionDriver::with_finish(
                {
                    let cleaned = cleaned.clone();
                    async move {
                        finish_rx.await.unwrap();
                        tokio::task::yield_now().await;
                        cleaned.store(true, Ordering::Release);
                        drop(remote);
                        Ok(())
                    }
                },
                move || {
                    finish_tx.send(()).unwrap();
                },
            );
            if failed {
                drop(peer.tx.fail("test bridge failure"));
            } else {
                peer.tx.close_channel();
            }
            let result = tokio::time::timeout(
                Duration::from_secs(3),
                ConnectTo::<UntypedRole>::connect_to(bridge, Owned { endpoint, driver }),
            )
            .await
            .unwrap();
            assert_eq!(result.is_err(), failed);
            assert!(cleaned.load(Ordering::Acquire));
        }
    }

    #[test]
    fn passive_bridge_does_not_treat_absent_driver_as_completion() {
        let (source, source_peer) = BoundedChannel::duplex(limits()).unwrap();
        let (destination, mut destination_peer) = BoundedChannel::duplex(limits()).unwrap();
        let bridge = BoundedChannel {
            rx: source.rx,
            tx: destination.tx,
        };
        let mut driver = None;
        let mut copying = Box::pin(bridge.copy_with_driver(&mut driver));
        assert!(copying.as_mut().now_or_never().is_none());
        source_peer.tx.try_send(small_frame(1)).unwrap();
        assert!(copying.as_mut().now_or_never().is_none());
        assert!(destination_peer.rx.next().now_or_never().unwrap().is_some());
        source_peer.tx.close_channel();
        assert!(copying.now_or_never().unwrap().is_ok());
    }

    #[tokio::test]
    async fn waiting_send_retains_shared_charge_until_final_drop() {
        let (a, mut b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 1,
            ..limits()
        })
        .unwrap();
        a.tx.try_send(small_frame(0)).unwrap();
        let mut sending = Box::pin(a.tx.send(small_frame(1)));
        assert!(futures::poll!(&mut sending).is_pending());
        let held = Arc::new(b.rx.next().await.unwrap());
        let shared = held.clone();
        drop(held);
        assert!(futures::poll!(&mut sending).is_pending());
        assert!(a.tx.failure().now_or_never().is_none());
        drop(shared);
        sending.await.unwrap();
        assert!(b.rx.next().await.is_some());
    }

    #[tokio::test]
    async fn waiting_sends_are_fifo_and_cancelled_head_does_not_block() {
        let (a, mut b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 1,
            ..limits()
        })
        .unwrap();
        a.tx.try_send(small_frame(0)).unwrap();
        let held = b.rx.next().await.unwrap();
        let mut first = Box::pin(a.tx.send(small_frame(1)));
        let mut second = Box::pin(a.tx.send(small_frame(2)));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        drop(held);
        assert!(
            futures::poll!(&mut second).is_pending(),
            "later waiter bypassed FIFO"
        );
        drop(first);
        second.await.unwrap();
        let received = b.rx.next().await.unwrap();
        assert_eq!(
            received.decode().to_json().unwrap(),
            small_frame(2).to_json().unwrap()
        );
    }

    #[tokio::test]
    async fn waiting_send_registrations_are_bounded_and_cancellation_reuses_slot() {
        let (a, mut b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 1,
            ..limits()
        })
        .unwrap();
        a.tx.try_send(small_frame(0)).unwrap();
        let mut first = Box::pin(a.tx.send(small_frame(1)));
        let mut second = Box::pin(a.tx.send(small_frame(2)));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert!(
            a.tx.send(small_frame(3))
                .await
                .unwrap_err()
                .to_string()
                .contains("waiting send limit")
        );
        assert!(a.tx.failure().now_or_never().is_none());
        drop(second);
        let mut replacement = Box::pin(a.tx.send(small_frame(4)));
        assert!(futures::poll!(&mut replacement).is_pending());
        drop(first);
        drop(b.rx.next().await.unwrap());
        replacement.await.unwrap();
        assert!(b.rx.next().await.is_some());
    }

    #[tokio::test]
    async fn waiting_sends_end_on_graceful_close_without_discarding_admitted_frames() {
        let (a, mut b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 1,
            ..limits()
        })
        .unwrap();
        a.tx.try_send(small_frame(0)).unwrap();
        let mut first = Box::pin(a.tx.send(small_frame(1)));
        let mut second = Box::pin(a.tx.send(small_frame(2)));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        a.tx.close_channel();
        assert!(first.await.is_err());
        assert!(second.await.is_err());
        assert!(a.tx.failure().now_or_never().is_none());
        assert!(b.rx.next().await.is_some());
        assert!(b.rx.next().await.is_none());
    }

    #[tokio::test]
    async fn waiting_send_ends_on_receiver_loss_even_with_escaped_charge() {
        let (a, mut b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 1,
            ..limits()
        })
        .unwrap();
        a.tx.try_send(small_frame(0)).unwrap();
        let held = b.rx.next().await.unwrap();
        let mut sending = Box::pin(a.tx.send(small_frame(1)));
        assert!(futures::poll!(&mut sending).is_pending());
        drop(b);
        assert!(sending.await.is_err());
        drop(held);
    }

    #[tokio::test]
    async fn waiting_send_rejected_after_graceful_receiver_drop_preserves_opposite_frames() {
        let (mut a, mut b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 1,
            ..limits()
        })
        .unwrap();
        a.tx.try_send(small_frame(0)).unwrap();
        let held = b.rx.next().await.unwrap();
        b.tx.try_send(small_frame(2)).unwrap();
        let mut sending = Box::pin(a.tx.send(small_frame(1)));
        assert!(futures::poll!(&mut sending).is_pending());
        a.tx.close_channel();
        drop(b);
        assert!(sending.await.is_err());
        assert!(a.tx.failure().now_or_never().is_none());
        assert!(a.rx.next().await.is_some());
        drop(held);
    }

    #[tokio::test]
    async fn waiting_send_observes_terminal_try_send_exhaustion() {
        let (a, _b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 1,
            ..limits()
        })
        .unwrap();
        a.tx.try_send(small_frame(0)).unwrap();
        let mut sending = Box::pin(a.tx.send(small_frame(1)));
        assert!(futures::poll!(&mut sending).is_pending());
        let error = a.tx.try_send(small_frame(2)).unwrap_err();
        assert_eq!(sending.await.unwrap_err().to_string(), error.to_string());
    }

    #[tokio::test]
    async fn waiting_send_preserves_serialization_limit_failure_and_releases_charge() {
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let oversized = TransportFrame::parse_json(&format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":"{}"}}"#,
            "x".repeat(128)
        ));
        assert!(a.tx.send(oversized).await.is_err());
        assert!(b.rx.next().await.is_none());
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_waiting_senders_progress_as_shared_charges_are_released() {
        let (a, mut b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 2,
            max_tasks: 32,
            ..limits()
        })
        .unwrap();
        let senders = (0..32)
            .map(|id| {
                let sender = a.tx.clone();
                tokio::spawn(async move {
                    sender.send(small_frame(id)).await.unwrap();
                })
            })
            .collect::<Vec<_>>();
        let receive = async {
            let mut ids = Vec::new();
            for _ in 0..32 {
                let held = Arc::new(b.rx.next().await.unwrap());
                let shared = held.clone();
                drop(held);
                tokio::task::yield_now().await;
                let value: serde_json::Value = serde_json::from_slice(shared.as_bytes()).unwrap();
                ids.push(value["id"].as_u64().unwrap());
                drop(shared);
            }
            ids.sort_unstable();
            assert_eq!(ids, (0..32).collect::<Vec<_>>());
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            futures::join!(receive, async {
                for sender in senders {
                    sender.await.unwrap();
                }
            });
        })
        .await
        .expect("concurrent senders lost a capacity wake");
    }

    #[test]
    fn waiting_admission_releases_reservation_when_next_waker_unwinds() {
        struct PanicWake;
        impl std::task::Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                panic!("wake failed");
            }
        }
        let (a, mut b) = BoundedChannel::duplex(ChannelLimits {
            max_buffered_frames: 1,
            ..limits()
        })
        .unwrap();
        a.tx.try_send(small_frame(0)).unwrap();
        let held = b.rx.next().now_or_never().unwrap().unwrap();
        let mut first = Box::pin(a.tx.send(small_frame(1)));
        let mut second = Box::pin(a.tx.send(small_frame(2)));
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        let panic_waker = std::task::Waker::from(Arc::new(PanicWake));
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&panic_waker))
                .is_pending()
        );
        drop(held);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                first.as_mut().poll(&mut cx)
            }))
            .is_err()
        );
        drop(first);
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
        assert!(matches!(second.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        assert!(b.rx.next().now_or_never().unwrap().is_some());
    }

    #[test]
    fn close_finishes_queue_and_waiter_cleanup_when_a_waker_unwinds() {
        struct PanicWake;
        impl std::task::Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                panic!("wake failed");
            }
        }
        struct ObservedWake(std::sync::atomic::AtomicBool);
        impl std::task::Wake for ObservedWake {
            fn wake(self: Arc<Self>) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        for drop_receiver in [false, true] {
            let (a, mut b) = BoundedChannel::duplex(ChannelLimits {
                max_buffered_frames: 1,
                ..limits()
            })
            .unwrap();
            a.tx.try_send(small_frame(0)).unwrap();
            let held = b.rx.next().now_or_never().unwrap().unwrap();
            let mut first = Box::pin(a.tx.send(small_frame(1)));
            let mut second = Box::pin(a.tx.send(small_frame(2)));
            let panic_waker = std::task::Waker::from(Arc::new(PanicWake));
            assert!(
                first
                    .as_mut()
                    .poll(&mut Context::from_waker(&panic_waker))
                    .is_pending()
            );
            let observed = Arc::new(ObservedWake(std::sync::atomic::AtomicBool::new(false)));
            let observed_waker = std::task::Waker::from(observed.clone());
            assert!(
                second
                    .as_mut()
                    .poll(&mut Context::from_waker(&observed_waker))
                    .is_pending()
            );
            if drop_receiver {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(b))).is_err()
                );
            } else {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| a.tx.close_channel()))
                        .is_err()
                );
                assert!(
                    b.rx.next().now_or_never().unwrap().is_none(),
                    "queue was not closed"
                );
            }
            assert!(
                observed.0.load(std::sync::atomic::Ordering::SeqCst),
                "later waiter was not notified"
            );
            assert!(first.now_or_never().unwrap().is_err());
            assert!(second.now_or_never().unwrap().is_err());
            drop(held);
        }
    }

    #[test]
    fn prepared_enqueue_does_not_wake_receiver_until_token_drop() {
        struct Observe(Arc<Mutex<bool>>);
        impl std::task::Wake for Observe {
            fn wake(self: Arc<Self>) {
                assert!(
                    *self
                        .0
                        .try_lock()
                        .expect("woke while bookkeeping was locked")
                );
            }
        }
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let committed = Arc::new(Mutex::new(false));
        let waker = std::task::Waker::from(Arc::new(Observe(committed.clone())));
        assert!(
            Pin::new(&mut b.rx)
                .poll_next(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let mut frame =
            a.tx.prepare_serialized(r#"{"jsonrpc":"2.0","method":"test"}"#)
                .unwrap();
        {
            let mut committed = committed.lock().unwrap();
            frame.enqueue().unwrap();
            *committed = true;
        }
        drop(frame);
        assert!(b.rx.next().now_or_never().unwrap().is_some());
    }

    #[test]
    fn rejected_prepared_enqueue_releases_reservation_only_after_token_drop() {
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let mut frame = a.tx.prepare_serialized("{}").unwrap();
        b.rx.close();
        assert!(frame.enqueue().is_err());
        assert_eq!(a.tx.budget.snapshot().0, 1);
        drop(frame);
        assert_eq!(a.tx.budget.snapshot().0, 0);
        assert!(a.tx.failure().now_or_never().is_none());
    }

    #[test]
    fn terminal_waker_can_cancel_another_observer_reentrantly() {
        struct CancelOther(Mutex<Option<BoxFuture<'static, Error>>>);
        impl std::task::Wake for CancelOther {
            fn wake(self: Arc<Self>) {
                let other = self.0.lock().unwrap().take();
                drop(other);
            }
        }
        let (a, _b) = BoundedChannel::duplex(limits()).unwrap();
        let mut target = a.tx.failure();
        assert!(target.as_mut().now_or_never().is_none());
        let cancel = Arc::new(CancelOther(Mutex::new(Some(target))));
        let mut observer = a.tx.failure();
        let waker = std::task::Waker::from(cancel.clone());
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let error = a.tx.fail("test terminal failure");
        assert!(cancel.0.lock().unwrap().is_none());
        assert_eq!(observer.now_or_never().unwrap(), error);
    }

    #[test]
    fn terminal_wake_unwind_preserves_error_and_notifies_other_observers() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Panics;
        impl std::task::Wake for Panics {
            fn wake(self: Arc<Self>) {
                panic!("test wake unwinds");
            }
        }
        struct Observes(AtomicBool);
        impl std::task::Wake for Observes {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::Relaxed);
            }
        }
        let (a, _b) = BoundedChannel::duplex(limits()).unwrap();
        let mut panics = a.tx.failure();
        let mut observes = a.tx.failure();
        let observed = Arc::new(Observes(AtomicBool::new(false)));
        for (future, waker) in [
            (&mut panics, std::task::Waker::from(Arc::new(Panics))),
            (&mut observes, std::task::Waker::from(observed.clone())),
        ] {
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| a.tx.fail("original error")))
                .is_err()
        );
        assert!(observed.0.load(Ordering::Relaxed));
        assert_eq!(
            observes.now_or_never().unwrap(),
            limit_error("original error")
        );
        assert_eq!(a.tx.fail("ignored error"), limit_error("original error"));
    }

    #[test]
    fn protocol_close_seals_local_producers_without_discarding_accepted_transport_output() {
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let reserved = a.tx.budget.reserve_protocol().unwrap();
        a.tx.budget.close_protocol();
        assert!(a.tx.budget.reserve_protocol().is_err());
        a.tx.send_charged(small_frame(0), vec![reserved]).unwrap();
        assert!(b.rx.next().now_or_never().unwrap().is_some());
        assert!(a.tx.failure().now_or_never().is_none());
    }

    #[test]
    fn receiver_waker_can_close_admission_reentrantly() {
        struct CloseOnWake(BoundedSender);
        impl std::task::Wake for CloseOnWake {
            fn wake(self: Arc<Self>) {
                self.0.close_channel();
            }
        }
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let waker = std::task::Waker::from(Arc::new(CloseOnWake(a.tx.clone())));
        assert!(
            Pin::new(&mut b.rx)
                .poll_next(&mut Context::from_waker(&waker))
                .is_pending()
        );
        a.tx.try_send(small_frame(0)).unwrap();
        assert!(a.tx.failure().now_or_never().is_none());
        assert!(b.rx.next().now_or_never().unwrap().is_some());
        assert!(b.rx.next().now_or_never().unwrap().is_none());
        assert!(a.tx.try_send(small_frame(1)).is_err());
    }

    #[test]
    fn byte_and_frame_admission_persist_after_dequeue_and_forward() {
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        a.tx.try_send_serialized("{}").unwrap();
        let frame = b.rx.next().now_or_never().unwrap().unwrap();
        assert_eq!(a.tx.budget.snapshot(), (1, 64, 0));
        a.tx.try_forward(frame).unwrap();
        assert_eq!(a.tx.budget.snapshot(), (1, 64, 0));
        let frame = b.rx.next().now_or_never().unwrap().unwrap();
        drop(frame);
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
        a.tx.try_send_serialized(&"x".repeat(64)).unwrap();
        assert!(a.tx.try_send_serialized(&"x".repeat(65)).is_err());
        assert!(a.tx.failure().now_or_never().is_some());
        assert!(b.rx.next().now_or_never().unwrap().is_none());
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
    }

    #[test]
    fn graceful_close_rejects_clones_but_drains_charged_frames_without_failure() {
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let clone = a.tx.clone();
        a.tx.try_send_serialized("{}").unwrap();
        a.tx.try_send_serialized("[]").unwrap();
        a.tx.close_channel();
        a.tx.close_channel();
        assert!(clone.try_send_serialized("rejected").is_err());
        assert!(clone.try_send(TransportFrame::parse_json("{}")).is_err());
        assert!(a.tx.failure().now_or_never().is_none());
        assert_eq!(a.tx.budget.snapshot(), (2, 128, 0));
        let first = b.rx.next().now_or_never().unwrap().unwrap();
        let second = b.rx.next().now_or_never().unwrap().unwrap();
        assert_eq!(first.as_bytes(), b"{}");
        assert_eq!(second.as_bytes(), b"[]");
        assert!(b.rx.next().now_or_never().unwrap().is_none());
        assert_eq!(a.tx.budget.snapshot(), (2, 128, 0));
        drop((first, second));
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
        b.tx.try_send_serialized("{}").unwrap();
        assert!(a.tx.failure().now_or_never().is_none());
    }

    #[test]
    fn producer_flood_before_poll_is_terminal_and_drains_charges() {
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let escaped = a.tx.clone();
        a.tx.try_send_serialized("{}").unwrap();
        a.tx.try_send_serialized("{}").unwrap();
        assert!(escaped.try_send_serialized("{}").is_err());
        assert_eq!(a.tx.budget.snapshot(), (2, 128, 0));
        assert!(b.rx.next().now_or_never().unwrap().is_none());
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
        assert!(escaped.try_send_serialized("{}").is_err());
    }

    #[test]
    fn cross_budget_forward_reserves_destination_and_releases_source() {
        let (a, mut b) = BoundedChannel::duplex(limits()).unwrap();
        let (c, mut d) = BoundedChannel::duplex(limits()).unwrap();
        a.tx.try_send_serialized("{}").unwrap();
        let frame = b.rx.next().now_or_never().unwrap().unwrap();
        c.tx.try_forward(frame).unwrap();
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
        assert_eq!(c.tx.budget.snapshot(), (1, 64, 0));
        drop(d.rx.next().now_or_never().unwrap().unwrap());
        assert_eq!(c.tx.budget.snapshot(), (0, 0, 0));
    }

    #[test]
    fn dropped_receiver_and_serialization_errors_release_reservations() {
        let (a, b) = BoundedChannel::duplex(limits()).unwrap();
        drop(b);
        assert!(a.tx.try_send_serialized("{}").is_err());
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
        let (a, _b) = BoundedChannel::duplex(limits()).unwrap();
        let frame = TransportFrame::Single(
            crate::RawJsonRpcMessage::notification(
                "large".into(),
                serde_json::json!({"payload": "x".repeat(65)}),
            )
            .unwrap(),
        );
        assert!(a.tx.try_send(frame).is_err());
        assert_eq!(a.tx.budget.snapshot(), (0, 0, 0));
    }

    #[test]
    fn dyn_transport_preserves_bounded_variant_and_legacy_extraction_fails() {
        let (a, _b) = BoundedChannel::duplex(limits()).unwrap();
        let erased = crate::DynConnectTo::<crate::UntypedRole>::new(a);
        let (channel, _) = erased.into_transport_and_future();
        assert!(matches!(channel, TransportChannel::Bounded(_)));
        let (a, _b) = BoundedChannel::duplex(limits()).unwrap();
        let (mut channel, future) =
            <BoundedChannel as ConnectTo<crate::UntypedRole>>::into_channel_and_future(a);
        assert!(future.unwrap().now_or_never().unwrap().is_err());
        assert!(channel.rx.next().now_or_never().unwrap().is_none());
    }
}
