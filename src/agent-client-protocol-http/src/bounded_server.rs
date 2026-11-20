//! Opt-in HTTP-only server. This deliberately never uses the legacy connection pumps.
use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use agent_client_protocol::{
    BoundedChannel, BoundedSender, ChannelLimits, ChargedFrame, Client, ConnectTo,
    ConnectionDriver, RawJsonRpcId, RawJsonRpcMessage, RawJsonRpcResponse, TransportFrame,
};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use futures::{FutureExt, StreamExt};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};

use super::ServerOptions;
use crate::{
    connection::{MAX_UNCONFIRMED_SESSION_STREAMS, ResponseRoute, SessionEffect},
    http_server::{
        batch_rejection, initial_initialize_request, initialize_response_failed,
        prepare_message_route,
    },
    protocol::{HEADER_CONNECTION_ID, HEADER_SESSION_ID, JSON_MIME_TYPE, session_id_from_message},
};

/// Finite limits for the opt-in HTTP server, independent of the core channel limits.
///
/// Byte limits count encoded JSON/SSE bytes, not allocator overhead. POST bodies reserve
/// `max_frame_bytes` before their first poll. Parsed JSON and serialization scratch are
/// additional bounded multiples of that size. Each connection has its own egress budget;
/// total server egress is therefore at most `max_connections * max_egress_bytes`.
#[derive(Clone, Debug)]
pub struct ServerLimits {
    /// Core producer/transport admission limits supplied to the component factory.
    pub channel_limits: ChannelLimits,
    /// Concurrent connections, including initialization and draining.
    pub max_connections: usize,
    /// POST handlers concurrently admitted to read a body.
    pub max_in_flight_posts: usize,
    /// Global byte budget reserved by admitted POST bodies.
    pub max_body_bytes: usize,
    /// Maximum encoded frame size before and after session-header injection.
    pub max_frame_bytes: usize,
    /// Request-to-response routes retained per connection.
    pub max_pending_routes: usize,
    /// Session mailboxes per connection, including unconfirmed sessions.
    pub max_registered_sessions: usize,
    /// Concurrent SSE streams per connection, including the connection stream.
    pub max_active_streams: usize,
    /// Frames retained per connection through their HTTP body handoff.
    pub max_egress_frames: usize,
    /// SSE wire bytes retained per connection, including framing overhead.
    pub max_egress_bytes: usize,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            channel_limits: ChannelLimits {
                max_frame_bytes: 256 * 1024,
                ..ChannelLimits::default()
            },
            max_connections: 64,
            max_in_flight_posts: 32,
            max_body_bytes: 8 * 1024 * 1024,
            max_frame_bytes: 256 * 1024,
            max_pending_routes: 256,
            max_registered_sessions: 64,
            max_active_streams: 65,
            max_egress_frames: 64,
            max_egress_bytes: 4 * 1024 * 1024,
        }
    }
}

/// An invalid bounded server configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerLimitsError(pub &'static str);

impl std::fmt::Display for ServerLimitsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ServerLimitsError {}

impl ServerLimits {
    /// Check that all limits are finite, nonzero, and mutually consistent.
    ///
    /// # Errors
    /// Returns an error if a limit cannot be enforced.
    pub fn validate(&self) -> Result<(), ServerLimitsError> {
        self.channel_limits
            .validate()
            .map_err(|_| ServerLimitsError("invalid core channel limits"))?;
        for value in [
            self.max_connections,
            self.max_in_flight_posts,
            self.max_body_bytes,
            self.max_frame_bytes,
            self.max_pending_routes,
            self.max_registered_sessions,
            self.max_active_streams,
            self.max_egress_frames,
            self.max_egress_bytes,
        ] {
            if value == 0 || value > u32::MAX as usize || value > Semaphore::MAX_PERMITS {
                return Err(ServerLimitsError(
                    "limits must be nonzero and fit semaphore counters",
                ));
            }
        }
        if self.max_body_bytes < self.max_frame_bytes
            || self.max_egress_bytes < self.max_frame_bytes + 8
        {
            return Err(ServerLimitsError(
                "body and egress budgets must fit one maximum frame",
            ));
        }
        if self
            .max_connections
            .checked_mul(self.max_egress_bytes)
            .is_none()
        {
            return Err(ServerLimitsError("aggregate egress budget overflows usize"));
        }
        Ok(())
    }
}

type Factory = dyn Fn(ChannelLimits) -> agent_client_protocol::Result<(BoundedChannel, Option<ConnectionDriver>)>
    + Send
    + Sync;

/// HTTP/SSE-only bounded server. WebSocket upgrades are explicitly rejected.
///
/// Components are connected via `ConnectTo::into_bounded_channel_and_future`. Components
/// that only extract legacy channels fail closed; no unbounded adapter is introduced.
/// Admission errors before core enqueue return 429/413. An admitted frame is never retried;
/// later overload terminates the connection. Body polling is not peer acknowledgment.
/// POST admission is fail-fast. Saturation of POST/body slots also terminates the
/// addressed connection, so callback responses cannot remain indefinitely blocked behind
/// slow request bodies. Callers must not automatically retry uncertain POST results.
/// Unconfirmed session streams are temporary. No SSE replay is provided.
pub struct BoundedAcpHttpServer {
    state: Arc<Registry>,
    options: ServerOptions,
}

impl std::fmt::Debug for BoundedAcpHttpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedAcpHttpServer")
            .field("limits", &self.state.limits)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl BoundedAcpHttpServer {
    pub(super) fn new<F, C>(factory: F, limits: ServerLimits) -> Result<Self, ServerLimitsError>
    where
        F: Fn() -> C + Send + Sync + 'static,
        C: ConnectTo<Client>,
    {
        limits.validate()?;
        Ok(Self {
            state: Arc::new(Registry {
                factory: Arc::new(move |limits| factory().into_bounded_channel_and_future(limits)),
                connections: Mutex::new(HashMap::new()),
                connection_slots: Arc::new(Semaphore::new(limits.max_connections)),
                posts: Arc::new(Semaphore::new(limits.max_in_flight_posts)),
                bodies: Arc::new(Semaphore::new(limits.max_body_bytes)),
                limits,
                graceful_delete: None,
            }),
            options: ServerOptions::default(),
        })
    }

    /// Replace the routing, health, and CORS options.
    #[must_use]
    pub fn with_options(mut self, options: ServerOptions) -> Self {
        self.options = options;
        self
    }

    /// Opt into graceful DELETE with a bounded wait per HTTP request.
    ///
    /// DELETE seals inbound admission without allocating a body or core frame. It
    /// returns 202 only after the component future succeeds and its output reaches
    /// clean EOF. This is not acknowledgment that an HTTP peer consumed the output.
    /// A deadline or terminal failure returns 503. A deadline never reopens admission
    /// or aborts accepted work; canceling the DELETE request only drops its waiter.
    /// Concurrent/repeated DELETE requests join the connection-owned drain while it
    /// exists. Once completion removes the connection, subsequent requests return 404.
    /// POST requests racing with the seal are rejected with 410 before enqueue.
    /// Without this option, DELETE retains its legacy abortive behavior.
    #[must_use]
    pub fn with_graceful_delete(mut self, timeout: Duration) -> Self {
        // The server has not exposed the registry before into_router consumes it.
        Arc::get_mut(&mut self.state).unwrap().graceful_delete = Some(timeout);
        self
    }

    /// Build the axum router for the bounded HTTP/SSE endpoint.
    ///
    /// # Panics
    /// Panics for invalid route configuration, as with [`super::AcpHttpServer::into_router`].
    pub fn into_router(self) -> Router {
        let mut router = Router::new()
            .route(&self.options.path, post(handle_post))
            .route(&self.options.path, get(handle_get))
            .route(&self.options.path, delete(handle_delete))
            .with_state(self.state);
        if self.options.health_endpoint {
            router = router.route("/health", get(super::health));
        }
        if let Some(origin) = self.options.cors.allow_origin_layer() {
            router = router.layer(super::default_cors(origin));
        }
        router
    }
}

struct Registry {
    graceful_delete: Option<Duration>,
    factory: Arc<Factory>,
    limits: ServerLimits,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
    connection_slots: Arc<Semaphore>,
    posts: Arc<Semaphore>,
    bodies: Arc<Semaphore>,
}

impl Drop for Registry {
    fn drop(&mut self) {
        for connection in self.connections.get_mut().unwrap().values() {
            connection.close();
        }
    }
}

struct Connection {
    id: String,
    registry: Weak<Registry>,
    limits: ServerLimits,
    inner: Mutex<ConnectionState>,
    wake: Notify,
    frame_slots: Arc<Semaphore>,
    byte_slots: Arc<Semaphore>,
    stream_slots: Arc<Semaphore>,
    // Held until tasks, requests and response bodies have released this connection.
    _slot: OwnedSemaphorePermit,
}

struct ConnectionState {
    tx: Option<BoundedSender>,
    closed: bool,
    draining: bool,
    drain_result: Option<bool>,
    task: Option<tokio::task::AbortHandle>,
    pending: VecDeque<PendingRoute>,
    streams: HashMap<Option<String>, Mailbox>,
    // Detached mailboxes each own a live subscription, bounded by stream_slots.
    retired_streams: HashMap<u64, Mailbox>,
    next_subscription: u64,
}

#[derive(Default)]
struct Mailbox {
    queue: VecDeque<Envelope>,
    subscription: Option<u64>,
    confirmed: bool,
}

struct Envelope {
    // Keep the core byte/frame admission across decode, routing and HTTP body handoff.
    frame: ChargedFrame,
    _frame_slot: OwnedSemaphorePermit,
    _byte_slot: OwnedSemaphorePermit,
}

impl Connection {
    fn close(&self) {
        self.close_if_open(false);
    }

    // A POST that raced with DELETE must not turn a harmless admission rejection
    // into terminal core failure. All destructors and wakeups run after unlocking.
    fn close_if_open(&self, only_open: bool) -> bool {
        let retired = {
            let mut state = self.inner.lock().unwrap();
            if only_open && (state.draining || state.closed) {
                return false;
            }
            state.closed = true;
            state.drain_result.get_or_insert(false);
            (
                state.tx.take(),
                state.task.take(),
                std::mem::take(&mut state.pending),
                std::mem::take(&mut state.streams),
                std::mem::take(&mut state.retired_streams),
            )
        };
        if let Some(tx) = &retired.0 {
            tx.fail("HTTP connection terminated");
        }
        if let Some(task) = &retired.1 {
            task.abort();
        }
        drop(retired);
        self.wake.notify_waiters();
        true
    }

    fn remove(&self) {
        if let Some(registry) = self.registry.upgrade() {
            let removed = registry.connections.lock().unwrap().remove(&self.id);
            drop(removed);
        }
    }

    fn terminate(&self) {
        self.close();
        self.remove();
    }

    fn terminate_if_open(&self) -> bool {
        let terminated = self.close_if_open(true);
        if terminated {
            self.remove();
        }
        terminated
    }

    fn begin_drain(&self) {
        let tx = {
            let mut state = self.inner.lock().unwrap();
            if state.closed || state.draining {
                return;
            }
            // This is the HTTP acceptance linearization point, shared with enqueue.
            state.draining = true;
            state.tx.take()
        };
        // No await separates seal publication and the core close. Cancellation cannot
        // strand this operation; close preserves all already accepted frame charges.
        if let Some(tx) = tx {
            tx.close_channel();
        }
    }

    fn finish_drain(&self) -> bool {
        let finished = {
            let mut state = self.inner.lock().unwrap();
            if state.closed || !state.draining {
                return false;
            }
            state.closed = true;
            state.drain_result = Some(true);
            state.task.take()
        };
        drop(finished);
        self.wake.notify_waiters();
        self.remove();
        true
    }

    async fn drain_result(&self) -> bool {
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.inner.lock().unwrap().drain_result {
                return result;
            }
            notified.await;
        }
    }

    fn envelope(&self, frame: ChargedFrame) -> Result<Envelope, ()> {
        let len = frame.as_bytes().len();
        if len > self.limits.max_frame_bytes || frame.as_bytes().contains(&b'\r') {
            return Err(());
        }
        // Reserve actual SSE framing too, before its allocation. JSON is UTF-8; the
        // serializer normally emits one line, but raw/malformed frames may contain LF.
        let text = std::str::from_utf8(frame.as_bytes()).map_err(|_| ())?;
        let wire_len = len
            .checked_add(8)
            .and_then(|n| n.checked_add(text.matches('\n').count() * 6))
            .ok_or(())?;
        let frames = self
            .frame_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ())?;
        let bytes = self
            .byte_slots
            .clone()
            .try_acquire_many_owned(u32::try_from(wire_len).map_err(|_| ())?)
            .map_err(|_| ())?;
        Ok(Envelope {
            frame,
            _frame_slot: frames,
            _byte_slot: bytes,
        })
    }

    fn complete_initial_route(&self, id: &RawJsonRpcId) {
        take_pending_route(&mut self.inner.lock().unwrap().pending, id);
    }

    fn route(&self, envelope: Envelope, frame: &TransportFrame) -> Result<(), ()> {
        let mut discarded = None;
        let mut state = self.inner.lock().unwrap();
        if state.closed {
            return Err(());
        }
        let target = match frame {
            TransportFrame::Single(message) => outbound_target(
                &mut state,
                message,
                self.limits.max_registered_sessions,
                &mut discarded,
            )?,
            _ => StreamTarget::Active(None),
        };
        let mailbox = match target {
            StreamTarget::Active(key) => state.streams.get_mut(&key).unwrap(),
            StreamTarget::Retired(subscription) => {
                state.retired_streams.get_mut(&subscription).unwrap()
            }
        };
        mailbox.queue.push_back(envelope);
        drop(state);
        drop(discarded);
        self.wake.notify_waiters();
        Ok(())
    }
}

struct PendingRoute {
    id: RawJsonRpcId,
    route: ResponseRoute,
    effect: SessionEffect,
}

fn take_pending_route(
    pending: &mut VecDeque<PendingRoute>,
    id: &RawJsonRpcId,
) -> Option<PendingRoute> {
    let index = pending.iter().position(|route| &route.id == id)?;
    pending.remove(index)
}

fn ensure_stream(
    state: &mut ConnectionState,
    key: Option<String>,
    max: usize,
) -> Result<&mut Mailbox, ()> {
    if !state.streams.contains_key(&key) && state.streams.len() - 1 >= max {
        return Err(());
    }
    Ok(state.streams.entry(key).or_default())
}

enum StreamTarget {
    Active(Option<String>),
    Retired(u64),
}

fn outbound_target(
    state: &mut ConnectionState,
    message: &RawJsonRpcMessage,
    max: usize,
    discarded: &mut Option<Mailbox>,
) -> Result<StreamTarget, ()> {
    let RawJsonRpcMessage::Response(response) = message else {
        let key = session_id_from_message(message);
        if key.is_some() {
            let mailbox = ensure_stream(state, key.clone(), max)?;
            mailbox.confirmed = true;
        }
        return Ok(StreamTarget::Active(key));
    };
    let Some(pending) = message
        .response_id()
        .and_then(|id| take_pending_route(&mut state.pending, id))
    else {
        return Ok(StreamTarget::Active(None));
    };
    if let RawJsonRpcResponse::Result { result, .. } = response {
        if pending.effect == SessionEffect::Opens
            && let Some(id) = result.get("sessionId").and_then(serde_json::Value::as_str)
        {
            let mailbox = ensure_stream(state, Some(id.to_owned()), max)?;
            mailbox.confirmed = true;
        }
        let ResponseRoute::Session(id) = pending.route else {
            return Ok(StreamTarget::Active(None));
        };
        let key = Some(id);
        if pending.effect == SessionEffect::Closes {
            if let Some(mailbox) = state.streams.remove(&key) {
                if let Some(subscription) = mailbox.subscription {
                    // Detach the retiring generation. New output for the same ID
                    // can confirm a new mailbox without reactivating this lease.
                    state.retired_streams.insert(subscription, mailbox);
                    return Ok(StreamTarget::Retired(subscription));
                }
                *discarded = Some(mailbox);
            }
            return Ok(StreamTarget::Active(None));
        }
        // A result confirms the session, but its response is connection-scoped
        // if the client had not opened a stream for it before the POST.
        let had_stream = state.streams.contains_key(&key);
        let mailbox = ensure_stream(state, key.clone(), max)?;
        mailbox.confirmed = true;
        return Ok(StreamTarget::Active(key.filter(|_| had_stream)));
    }
    // Errors for unknown sessions reach the client without allocating mailboxes.
    Ok(StreamTarget::Active(match pending.route {
        ResponseRoute::Session(id) => {
            Some(id).filter(|id| state.streams.contains_key(&Some(id.clone())))
        }
        ResponseRoute::Connection => None,
    }))
}

struct Cleanup {
    connection: Arc<Connection>,
    armed: bool,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        if self.armed {
            self.connection.terminate();
        }
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Result<Option<String>, StatusCode> {
    headers
        .get(name)
        .map(|value| {
            if value.len() > 1024 {
                return Err(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
            }
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| StatusCode::BAD_REQUEST)
        })
        .transpose()
}

// Serialize into a capped writer before core enqueue, including session-header injection.
fn encode_frame(frame: &TransportFrame, max: usize) -> Result<String, StatusCode> {
    struct Capped {
        bytes: Vec<u8>,
        max: usize,
    }
    impl std::io::Write for Capped {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.max - self.bytes.len() {
                return Err(std::io::Error::other("frame limit exceeded"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Capped {
        bytes: Vec::new(),
        max,
    };
    let result = match frame {
        TransportFrame::Single(message) => serde_json::to_writer(&mut output, message),
        TransportFrame::Malformed { raw, .. } => {
            if raw.len() > max {
                return Err(StatusCode::PAYLOAD_TOO_LARGE);
            }
            return Ok(raw.clone());
        }
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    result.map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;
    Ok(String::from_utf8(output.bytes).expect("JSON is UTF-8"))
}

async fn handle_post(State(registry): State<Arc<Registry>>, request: Request<Body>) -> Response {
    match post_inner(registry, request).await {
        Ok(response) => response,
        Err(status) => status.into_response(),
    }
}

async fn post_inner(
    registry: Arc<Registry>,
    request: Request<Body>,
) -> Result<Response, StatusCode> {
    if !request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with(JSON_MIME_TYPE))
    {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let connection_id = header_value(request.headers(), HEADER_CONNECTION_ID)?;
    let session_id = header_value(request.headers(), HEADER_SESSION_ID)?;
    // Reject closing connections before reserving global POST/body capacity. A
    // second check at enqueue handles bodies already being read when DELETE seals.
    let existing = if let Some(id) = &connection_id {
        let connection = registry.connections.lock().unwrap().get(id).cloned();
        if let Some(connection) = &connection {
            let state = connection.inner.lock().unwrap();
            if state.draining || state.closed {
                return Err(StatusCode::GONE);
            }
        }
        connection
    } else {
        None
    };

    // Body shape is unknown until read: rather than let slow request bodies starve
    // callback responses indefinitely, saturation terminates the addressed connection.
    // The rejected POST itself has not been accepted and is never resubmitted here.
    let saturated = || {
        if let Some(id) = &connection_id {
            let connection = registry.connections.lock().unwrap().get(id).cloned();
            if let Some(connection) = connection {
                connection.terminate_if_open();
            }
        }
        StatusCode::TOO_MANY_REQUESTS
    };
    let post_slot = registry
        .posts
        .clone()
        .try_acquire_owned()
        .map_err(|_| saturated())?;
    let body_slot = registry
        .bodies
        .clone()
        .try_acquire_many_owned(
            u32::try_from(registry.limits.max_frame_bytes).expect("validated frame limit"),
        )
        .map_err(|_| saturated())?;
    // Admission before the first body read and before factory invocation.
    let creating = connection_id.is_none();
    let connection_slot = if creating {
        Some(
            registry
                .connection_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?,
        )
    } else {
        None
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > registry.limits.max_frame_bytes)
    {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let body = axum::body::to_bytes(request.into_body(), registry.limits.max_frame_bytes)
        .await
        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;
    let text = std::str::from_utf8(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut frame = TransportFrame::parse_json(text);
    if let Some(response) = batch_rejection(&frame) {
        return Ok(response);
    }
    let initialize = initial_initialize_request(&frame).cloned();
    if creating != initialize.is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let pending = match &mut frame {
        TransportFrame::Single(message) => {
            let route = prepare_message_route(message, session_id.as_deref())
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            match (message, route) {
                (RawJsonRpcMessage::Request(request), Some(route))
                    if request.id != RawJsonRpcId::Null =>
                {
                    Some(PendingRoute {
                        id: request.id.clone(),
                        route,
                        effect: SessionEffect::for_method(&request.method),
                    })
                }
                _ => None,
            }
        }
        _ => None,
    };
    let encoded = encode_frame(&frame, registry.limits.max_frame_bytes)?;
    drop(frame);
    let mut initialization = None;
    let connection = if connection_id.is_some() {
        existing.ok_or(StatusCode::NOT_FOUND)?
    } else {
        let (BoundedChannel { tx, mut rx }, agent) =
            (registry.factory)(registry.limits.channel_limits)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let terminal_owner = tx.clone();
        let failure = tx.failure();
        let graceful = registry.graceful_delete.is_some();
        let mut streams = HashMap::new();
        streams.insert(None, Mailbox::default());
        let connection = Arc::new(Connection {
            id: uuid::Uuid::new_v4().to_string(),
            registry: Arc::downgrade(&registry),
            limits: registry.limits.clone(),
            inner: Mutex::new(ConnectionState {
                tx: Some(tx),
                closed: false,
                draining: false,
                drain_result: None,
                task: None,
                pending: VecDeque::new(),
                streams,
                retired_streams: HashMap::new(),
                next_subscription: 0,
            }),
            wake: Notify::new(),
            frame_slots: Arc::new(Semaphore::new(registry.limits.max_egress_frames)),
            byte_slots: Arc::new(Semaphore::new(registry.limits.max_egress_bytes)),
            stream_slots: Arc::new(Semaphore::new(registry.limits.max_active_streams)),
            _slot: connection_slot.unwrap(),
        });
        let (init_tx, init_rx) = oneshot::channel();
        initialization = Some(init_rx);
        registry
            .connections
            .lock()
            .unwrap()
            .insert(connection.id.clone(), connection.clone());
        let cleanup = Cleanup {
            connection: connection.clone(),
            armed: true,
        };
        let init_id = initialize.unwrap();
        let task_connection = connection.clone();
        // Capture cleanup before spawning: dropping even a never-polled task cleans up.
        let task = tokio::spawn(async move {
            let mut cleanup = cleanup;
            // Keep the terminal signal owner alive: dropping every channel endpoint
            // cancels its signal, which is not evidence of a core failure.
            let _terminal_owner = terminal_owner;
            let completion_connection = task_connection.clone();
            let owned = agent.is_some();
            let (finish_tx, finish_rx) = oneshot::channel();
            let agent = async move {
                if let Some(driver) = agent {
                    driver.await.map_err(|_| ())?;
                    // No producer is retained by this signal. Seal escaped
                    // producers only after actual owned work succeeds.
                    let _ = finish_tx.send(());
                }
                Ok::<(), ()>(())
            };
            let router = async move {
                let mut finish = Some(finish_rx);
                let mut rx = futures::stream::poll_fn(move |cx| {
                    if let Some(signal) = finish.as_mut() {
                        match std::future::Future::poll(std::pin::Pin::new(signal), cx) {
                            std::task::Poll::Ready(Ok(())) => {
                                rx.close();
                                finish = None;
                            }
                            std::task::Poll::Ready(Err(_)) => finish = None,
                            std::task::Poll::Pending => {}
                        }
                    }
                    futures::Stream::poll_next(std::pin::Pin::new(&mut rx), cx)
                });
                let mut init_tx = Some(init_tx);
                while let Some(charged) = rx.next().await {
                    if charged.as_bytes().len() > task_connection.limits.max_frame_bytes {
                        return Err(());
                    }
                    let frame = charged.decode();
                    let Ok(envelope) = task_connection.envelope(charged) else {
                        return Err(());
                    };
                    if let Some(failed) = initialize_response_failed(&frame, &init_id)
                        && let Some(sender) = init_tx.take()
                    {
                        task_connection.complete_initial_route(&init_id);
                        if sender.send((envelope, failed)).is_err() {
                            return Err(());
                        }
                        continue;
                    }
                    if task_connection.route(envelope, &frame).is_err() {
                        return Err(());
                    }
                }
                Ok::<(), ()>(())
            };
            // EOF alone is insufficient: core failure also produces EOF, and
            // owned work can still fail after closing its output direction.
            let done = async { futures::try_join!(router, agent).map(|_| ()) };
            tokio::pin!(failure);
            let result = tokio::select! {
                biased;
                _ = &mut failure => Err(()),
                result = done => result,
            };
            if result.is_ok() && failure.now_or_never().is_none() {
                if owned {
                    // Natural owned completion also seals POST admission. Keep
                    // queued SSE output available to existing subscribers.
                    completion_connection.begin_drain();
                }
                if (owned || graceful) && completion_connection.finish_drain() {
                    cleanup.armed = false;
                }
            }
        });
        let abort = {
            let mut state = connection.inner.lock().unwrap();
            if state.closed {
                true
            } else {
                state.task = Some(task.abort_handle());
                false
            }
        };
        if abort {
            task.abort();
        }
        connection
    };
    let mut cleanup = Cleanup {
        connection: connection.clone(),
        armed: initialization.is_some(),
    };
    let check_acceptance = |state: &ConnectionState| {
        if state.closed || state.draining {
            return Err(StatusCode::GONE);
        }
        if pending.is_some() && state.pending.len() >= connection.limits.max_pending_routes {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
        Ok(())
    };
    let tx = {
        let state = connection.inner.lock().unwrap();
        check_acceptance(&state)?;
        state.tx.as_ref().unwrap().clone()
    };
    // Preparation can run admission callbacks. The guard constructed below
    // always drops before the token, which owns the frame and the deferred wake.
    let mut prepared = tx.prepare_serialized(&encoded).map_err(|_| {
        if connection.terminate_if_open() {
            StatusCode::INTERNAL_SERVER_ERROR
        } else {
            StatusCode::GONE
        }
    })?;
    {
        // A POST can race with DELETE, another POST, or receiver polling. Commit
        // the route under the same lock that seals admission and routes output.
        // The enqueue is visible to the receiver, but neither wakes it nor drops
        // charged data here, and routing cannot run until the route is committed.
        let mut state = connection.inner.lock().unwrap();
        check_acceptance(&state)?;
        if prepared.enqueue().is_err() {
            drop(state);
            drop(prepared);
            connection.terminate_if_open();
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        state.pending.extend(pending);
    }
    drop(encoded);
    drop(body);
    drop(body_slot);
    drop(post_slot);
    drop(prepared);
    drop(tx);
    let Some(initialization) = initialization else {
        return Ok(StatusCode::ACCEPTED.into_response());
    };
    let (envelope, failed) = initialization
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if failed {
        connection.terminate();
    } else if connection.inner.lock().unwrap().closed {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    // An unpolled/dropped initialization body still owns cleanup. Only yielding its bytes
    // publishes success; this is an HTTP-stack handoff, not a peer acknowledgment.
    let id = connection.id.clone();
    let stream = async_stream::stream! {
        let envelope = envelope;
        let bytes = envelope.frame.as_bytes().to_vec();
        cleanup.armed = false;
        yield Ok::<_, Infallible>(bytes);
        drop(envelope);
        drop(cleanup);
    };
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(JSON_MIME_TYPE),
    );
    if !failed {
        response
            .headers_mut()
            .insert(HEADER_CONNECTION_ID, HeaderValue::from_str(&id).unwrap());
    }
    Ok(response)
}

struct Lease {
    connection: Arc<Connection>,
    key: Option<String>,
    subscription: u64,
    _slot: OwnedSemaphorePermit,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let discarded = {
            let mut state = self.connection.inner.lock().unwrap();
            if let Some(mailbox) = state.retired_streams.remove(&self.subscription) {
                Some(mailbox)
            } else if let Some(mailbox) = state.streams.get_mut(&self.key)
                && mailbox.subscription == Some(self.subscription)
            {
                if self.key.is_some() && !mailbox.confirmed {
                    state.streams.remove(&self.key)
                } else {
                    mailbox.subscription = None;
                    None
                }
            } else {
                None
            }
        };
        // Charged frame destruction can wake a producer that re-enters HTTP.
        drop(discarded);
    }
}

async fn handle_get(State(registry): State<Arc<Registry>>, request: Request<Body>) -> Response {
    match get_inner(registry, request) {
        Ok(response) => response,
        Err(status) => status.into_response(),
    }
}

fn get_inner(registry: Arc<Registry>, request: Request<Body>) -> Result<Response, StatusCode> {
    if request.headers().contains_key(header::UPGRADE) {
        return Err(StatusCode::NOT_IMPLEMENTED);
    }
    let id =
        header_value(request.headers(), HEADER_CONNECTION_ID)?.ok_or(StatusCode::BAD_REQUEST)?;
    let key = header_value(request.headers(), HEADER_SESSION_ID)?;
    let connection = registry
        .connections
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;
    let slot = connection
        .stream_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let subscription = {
        let mut state = connection.inner.lock().unwrap();
        if state.closed {
            return Err(StatusCode::GONE);
        }
        if key.is_some()
            && !state.streams.contains_key(&key)
            && (state.draining
                || state
                    .streams
                    .iter()
                    .filter(|(key, mailbox)| key.is_some() && !mailbox.confirmed)
                    .count()
                    >= MAX_UNCONFIRMED_SESSION_STREAMS
                        .min(connection.limits.max_registered_sessions))
        {
            return Err(StatusCode::NOT_FOUND);
        }
        let subscription = state.next_subscription;
        state.next_subscription = subscription.checked_add(1).ok_or(StatusCode::GONE)?;
        let mailbox = ensure_stream(
            &mut state,
            key.clone(),
            connection.limits.max_registered_sessions,
        )
        .map_err(|()| StatusCode::TOO_MANY_REQUESTS)?;
        if mailbox.subscription.is_some() {
            return Err(StatusCode::CONFLICT);
        }
        mailbox.subscription = Some(subscription);
        subscription
    };
    let lease = Lease {
        connection,
        key: key.clone(),
        subscription,
        _slot: slot,
    };
    let stream = async_stream::stream! {
        let lease = lease;
        loop {
            // Register before checking the queue to avoid a lost notify_waiters race.
            let notified = lease.connection.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (envelope, closed) = {
                let mut state = lease.connection.inner.lock().unwrap();
                if let Some(mailbox) = state.retired_streams.get_mut(&lease.subscription) {
                    (mailbox.queue.pop_front(), true)
                } else {
                    let closed = state.closed;
                    state.streams.get_mut(&lease.key)
                        .filter(|mailbox| mailbox.subscription == Some(lease.subscription))
                        .map_or((None, true), |mailbox| (mailbox.queue.pop_front(), closed))
                }
            };
            if let Some(envelope) = envelope {
                let text = std::str::from_utf8(envelope.frame.as_bytes()).expect("serialized JSON is UTF-8");
                let mut event = String::with_capacity(text.len() + 8 + text.matches('\n').count() * 6);
                for line in text.split('\n') { event.push_str("data: "); event.push_str(line); event.push('\n'); }
                event.push('\n');
                yield Ok::<_, Infallible>(event.into_bytes());
                // Retained through yield; release on next body poll or cancellation/drop.
                drop(envelope);
            } else if closed { break; } else { notified.await; }
        }
    };
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
        .headers_mut()
        .insert(HEADER_CONNECTION_ID, HeaderValue::from_str(&id).unwrap());
    if let Some(key) = key {
        response
            .headers_mut()
            .insert(HEADER_SESSION_ID, HeaderValue::from_str(&key).unwrap());
    }
    Ok(response)
}

async fn handle_delete(State(registry): State<Arc<Registry>>, request: Request<Body>) -> Response {
    let Ok(Some(id)) = header_value(request.headers(), HEADER_CONNECTION_ID) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let connection = registry.connections.lock().unwrap().get(&id).cloned();
    let Some(connection) = connection else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if let Some(deadline) = registry.graceful_delete {
        connection.begin_drain();
        return match tokio::time::timeout(deadline, connection.drain_result()).await {
            Ok(true) => StatusCode::ACCEPTED,
            Ok(false) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
        .into_response();
    }
    connection.terminate();
    StatusCode::ACCEPTED.into_response()
}

#[cfg(test)]
#[path = "graceful_delete_tests.rs"]
mod graceful_delete_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, timeout};

    fn registry(limits: ServerLimits) -> (Arc<Registry>, BoundedChannel) {
        let (agent, transport) = BoundedChannel::duplex(limits.channel_limits).unwrap();
        let transport = Mutex::new(Some(transport));
        let server =
            BoundedAcpHttpServer::new(move || transport.lock().unwrap().take().unwrap(), limits)
                .unwrap();
        (server.state, agent)
    }

    fn post_request(body: impl Into<Body>, id: Option<&str>) -> Request<Body> {
        let mut request = Request::builder()
            .method("POST")
            .header(header::CONTENT_TYPE, JSON_MIME_TYPE);
        if let Some(id) = id {
            request = request.header(HEADER_CONNECTION_ID, id);
        }
        request.body(body.into()).unwrap()
    }

    async fn initialize(registry: Arc<Registry>, agent: &mut BoundedChannel) -> (String, Response) {
        let task = tokio::spawn(handle_post(
            State(registry),
            post_request(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
                None,
            ),
        ));
        let received = timeout(Duration::from_secs(2), agent.rx.next())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(received.decode(), TransportFrame::Single(_)));
        drop(received);
        agent
            .tx
            .try_send_serialized(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#)
            .unwrap();
        let response = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let id = response.headers()[HEADER_CONNECTION_ID]
            .to_str()
            .unwrap()
            .to_owned();
        (id, response)
    }

    #[test]
    fn validates_finite_limits_and_capped_encoding() {
        let limits = ServerLimits {
            max_connections: 0,
            ..ServerLimits::default()
        };
        assert!(limits.validate().is_err());
        let frame = TransportFrame::parse_json(r#"{"jsonrpc":"2.0","method":"ping"}"#);
        let encoded = encode_frame(&frame, 1024).unwrap();
        assert!(encode_frame(&frame, encoded.len()).is_ok());
        assert_eq!(
            encode_frame(&frame, encoded.len() - 1),
            Err(StatusCode::PAYLOAD_TOO_LARGE)
        );
    }

    #[tokio::test]
    async fn post_and_connection_admission_precede_body_poll_and_factory() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = calls.clone();
        let state = BoundedAcpHttpServer::new(
            move || -> BoundedChannel {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                panic!("factory must not run");
            },
            ServerLimits {
                max_connections: 1,
                max_in_flight_posts: 1,
                ..ServerLimits::default()
            },
        )
        .unwrap()
        .state;
        let polls = Arc::new(AtomicUsize::new(0));
        for hold_connection in [false, true] {
            let held = if hold_connection {
                state.connection_slots.clone()
            } else {
                state.posts.clone()
            }
            .try_acquire_owned()
            .unwrap();
            let body_polls = polls.clone();
            let body = Body::from_stream(futures::stream::poll_fn(move |_| {
                body_polls.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Ready(Some(Ok::<_, Infallible>("{}")))
            }));
            let response = handle_post(State(state.clone()), post_request(body, None)).await;
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            drop(held);
        }
        assert_eq!(state.posts.available_permits(), 1);
        assert_eq!(
            state.bodies.available_permits(),
            state.limits.max_body_bytes
        );
    }

    #[tokio::test]
    async fn dropping_unpolled_initialize_body_terminates_connection() {
        let (state, mut agent) = registry(ServerLimits::default());
        let (id, response) = initialize(state.clone(), &mut agent).await;
        assert!(state.connections.lock().unwrap().contains_key(&id));
        drop(response);
        assert!(!state.connections.lock().unwrap().contains_key(&id));
        tokio::task::yield_now().await;
        assert_eq!(
            state.connection_slots.available_permits(),
            state.limits.max_connections
        );
    }

    #[tokio::test]
    async fn dropped_initialization_request_cleans_up_without_detached_cleanup() {
        let (state, mut agent) = registry(ServerLimits::default());
        let task = tokio::spawn(handle_post(
            State(state.clone()),
            post_request(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
                None,
            ),
        ));
        let received = timeout(Duration::from_secs(2), agent.rx.next())
            .await
            .unwrap()
            .unwrap();
        drop(received);
        task.abort();
        drop(task.await);
        tokio::task::yield_now().await;
        assert!(state.connections.lock().unwrap().is_empty());
        assert_eq!(
            state.posts.available_permits(),
            state.limits.max_in_flight_posts
        );
        assert_eq!(
            state.bodies.available_permits(),
            state.limits.max_body_bytes
        );
    }

    #[tokio::test]
    async fn sse_lease_and_egress_charge_survive_body_handoff_until_drop() {
        let (state, mut agent) = registry(ServerLimits::default());
        let (id, response) = initialize(state.clone(), &mut agent).await;
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let connection = state.connections.lock().unwrap().get(&id).unwrap().clone();
        agent
            .tx
            .try_send_serialized(r#"{"jsonrpc":"2.0","method":"notice"}"#)
            .unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            connection.frame_slots.available_permits(),
            state.limits.max_egress_frames - 1
        );
        let get = || {
            Request::builder()
                .header(HEADER_CONNECTION_ID, &id)
                .body(Body::empty())
                .unwrap()
        };
        let response = get_inner(state.clone(), get()).unwrap();
        assert_eq!(
            get_inner(state.clone(), get()).unwrap_err(),
            StatusCode::CONFLICT
        );
        let mut body = response.into_body().into_data_stream();
        let bytes = timeout(Duration::from_secs(2), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(std::str::from_utf8(&bytes).unwrap().starts_with("data: "));
        assert_eq!(
            connection.frame_slots.available_permits(),
            state.limits.max_egress_frames - 1
        );
        drop(body);
        assert_eq!(
            connection.frame_slots.available_permits(),
            state.limits.max_egress_frames
        );
        assert_eq!(
            connection.byte_slots.available_permits(),
            state.limits.max_egress_bytes
        );
        assert_eq!(
            connection.stream_slots.available_permits(),
            state.limits.max_active_streams
        );
        assert!(get_inner(state.clone(), get()).is_ok());
        connection.terminate();
    }

    #[tokio::test]
    async fn duplicate_route_cap_is_atomic_and_unknown_session_gets_are_bounded() {
        let (state, mut agent) = registry(ServerLimits {
            max_pending_routes: 2,
            max_registered_sessions: 1,
            ..ServerLimits::default()
        });
        let (id, response) = initialize(state.clone(), &mut agent).await;
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let request = r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#;
        let response = handle_post(State(state.clone()), post_request(request, Some(&id))).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let response = handle_post(State(state.clone()), post_request(request, Some(&id))).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let connection = state.connections.lock().unwrap().get(&id).unwrap().clone();
        assert_eq!(connection.inner.lock().unwrap().pending.len(), 2);
        let response = handle_post(State(state.clone()), post_request(request, Some(&id))).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(connection.inner.lock().unwrap().pending.len(), 2);
        let get = |session| {
            Request::builder()
                .header(HEADER_CONNECTION_ID, &id)
                .header(HEADER_SESSION_ID, session)
                .body(Body::empty())
                .unwrap()
        };
        let first = get_inner(state.clone(), get("one")).unwrap();
        assert_eq!(
            get_inner(state.clone(), get("two")).unwrap_err(),
            StatusCode::NOT_FOUND
        );
        drop(first);
        drop(get_inner(state.clone(), get("two")).unwrap());
        connection.terminate();
        assert!(connection.inner.lock().unwrap().pending.is_empty());
    }

    #[tokio::test]
    async fn post_for_unknown_session_does_not_allocate_a_mailbox() {
        let (state, mut agent) = registry(ServerLimits::default());
        let (id, response) = initialize(state.clone(), &mut agent).await;
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let connection = state.connections.lock().unwrap().get(&id).unwrap().clone();
        let request = r#"{"jsonrpc":"2.0","id":"unknown","method":"session/prompt","params":{"sessionId":"made-up"}}"#;
        assert_eq!(
            handle_post(State(state.clone()), post_request(request, Some(&id)))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        assert_eq!(connection.inner.lock().unwrap().streams.len(), 1);
        drop(
            timeout(Duration::from_secs(2), agent.rx.next())
                .await
                .unwrap()
                .unwrap(),
        );
        agent.tx.try_send_serialized(r#"{"jsonrpc":"2.0","id":"unknown","error":{"code":-32602,"message":"unknown session"}}"#).unwrap();
        let request = Request::builder()
            .header(HEADER_CONNECTION_ID, id)
            .body(Body::empty())
            .unwrap();
        let mut stream = get_inner(state.clone(), request)
            .unwrap()
            .into_body()
            .into_data_stream();
        let bytes = timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .contains("unknown session")
        );
        assert_eq!(connection.inner.lock().unwrap().streams.len(), 1);
    }

    #[tokio::test]
    async fn post_publishes_route_before_waking_receiver_and_unlocks_state() {
        struct Reenter(Arc<Connection>, AtomicUsize);
        impl std::task::Wake for Reenter {
            fn wake(self: Arc<Self>) {
                let state = self
                    .0
                    .inner
                    .try_lock()
                    .expect("receiver woke under HTTP state lock");
                assert_eq!(state.pending.back().unwrap().id, RawJsonRpcId::Number(42));
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (state, mut agent) = registry(ServerLimits::default());
        let (id, response) = initialize(state.clone(), &mut agent).await;
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let connection = state.connections.lock().unwrap().get(&id).cloned().unwrap();
        let observer = Arc::new(Reenter(connection, AtomicUsize::new(0)));
        let waker = std::task::Waker::from(observer.clone());
        let mut read = agent.rx.next();
        assert!(
            std::future::Future::poll(
                std::pin::Pin::new(&mut read),
                &mut std::task::Context::from_waker(&waker)
            )
            .is_pending()
        );
        let post = r#"{"jsonrpc":"2.0","id":42,"method":"ping"}"#;
        assert_eq!(
            handle_post(State(state.clone()), post_request(post, Some(&id)))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        assert_eq!(observer.1.load(Ordering::SeqCst), 1);
        assert!(read.await.is_some());
    }

    #[tokio::test]
    async fn dropping_unconfirmed_mailbox_releases_charge_outside_state_lock() {
        struct Reenter(Arc<Connection>, AtomicUsize);
        impl std::task::Wake for Reenter {
            fn wake(self: Arc<Self>) {
                let _state = self
                    .0
                    .inner
                    .try_lock()
                    .expect("producer woke under HTTP state lock");
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (state, mut agent) = registry(ServerLimits::default());
        let (id, response) = initialize(state.clone(), &mut agent).await;
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let connection = state.connections.lock().unwrap().get(&id).cloned().unwrap();
        let key = Some("unconfirmed".to_owned());
        let stream = get_inner(
            state.clone(),
            Request::builder()
                .header(HEADER_CONNECTION_ID, id)
                .header(HEADER_SESSION_ID, key.as_deref().unwrap())
                .body(Body::empty())
                .unwrap(),
        )
        .unwrap();
        let limits = ChannelLimits {
            max_buffered_frames: 1,
            ..ChannelLimits::default()
        };
        let (sender, mut receiver) = BoundedChannel::duplex(limits).unwrap();
        let text = r#"{"jsonrpc":"2.0","method":"notice"}"#;
        sender.tx.try_send_serialized(text).unwrap();
        let envelope = connection
            .envelope(receiver.rx.next().await.unwrap())
            .unwrap();
        connection
            .inner
            .lock()
            .unwrap()
            .streams
            .get_mut(&key)
            .unwrap()
            .queue
            .push_back(envelope);
        let observer = Arc::new(Reenter(connection, AtomicUsize::new(0)));
        let waker = std::task::Waker::from(observer.clone());
        let mut send = std::pin::pin!(sender.tx.send(TransportFrame::parse_json(text)));
        assert!(
            std::future::Future::poll(send.as_mut(), &mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        drop(stream);
        assert_eq!(observer.1.load(Ordering::SeqCst), 1);
        assert!(send.await.is_ok());
    }

    #[tokio::test]
    async fn retired_session_stream_stays_terminal_when_id_is_reused() {
        let (state, mut agent) = registry(ServerLimits::default());
        let (id, response) = initialize(state.clone(), &mut agent).await;
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let connection = state.connections.lock().unwrap().get(&id).cloned().unwrap();
        let get = || {
            Request::builder()
                .header(HEADER_CONNECTION_ID, &id)
                .header(HEADER_SESSION_ID, "reused")
                .body(Body::empty())
                .unwrap()
        };
        let old = get_inner(state.clone(), get()).unwrap();
        let close = r#"{"jsonrpc":"2.0","id":"close","method":"session/close","params":{"sessionId":"reused"}}"#;
        assert_eq!(
            handle_post(State(state.clone()), post_request(close, Some(&id)))
                .await
                .status(),
            StatusCode::ACCEPTED
        );
        // Drive routing synchronously to reuse the ID before the old body polls.
        let (sender, mut receiver) = BoundedChannel::duplex(state.limits.channel_limits).unwrap();
        for text in [
            r#"{"jsonrpc":"2.0","id":"close","result":{}}"#,
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"reused"}}"#,
        ] {
            sender.tx.try_send_serialized(text).unwrap();
            let frame = receiver.rx.next().await.unwrap();
            let decoded = frame.decode();
            connection
                .route(connection.envelope(frame).unwrap(), &decoded)
                .unwrap();
        }
        let replacement = get_inner(state.clone(), get()).unwrap();
        let mut old = old.into_body().into_data_stream();
        let bytes = old.next().await.unwrap().unwrap();
        assert!(std::str::from_utf8(&bytes).unwrap().contains("close"));
        assert!(
            timeout(Duration::from_secs(2), old.next())
                .await
                .unwrap()
                .is_none()
        );
        drop(old);
        assert_eq!(
            get_inner(state.clone(), get()).unwrap_err(),
            StatusCode::CONFLICT
        );
        let mut replacement = replacement.into_body().into_data_stream();
        let bytes = timeout(Duration::from_secs(2), replacement.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .contains("session/update")
        );
    }

    #[tokio::test]
    async fn oversized_unknown_length_body_and_rejected_batch_restore_admission() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = calls.clone();
        let state = BoundedAcpHttpServer::new(
            move || -> BoundedChannel {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                panic!("oversized bodies must not reach the factory");
            },
            ServerLimits {
                max_frame_bytes: 128,
                ..ServerLimits::default()
            },
        )
        .unwrap()
        .state;
        let body = Body::from_stream(futures::stream::iter([Ok::<_, Infallible>(vec![
            b' ';
            129
        ])]));
        assert_eq!(
            handle_post(State(state.clone()), post_request(body, None))
                .await
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        let batch = r#"[{"jsonrpc":"2.0","id":1,"method":"initialize"},{"jsonrpc":"2.0","id":2,"method":"ping"}]"#;
        assert_eq!(
            handle_post(State(state.clone()), post_request(batch, None))
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            state.posts.available_permits(),
            state.limits.max_in_flight_posts
        );
        assert_eq!(
            state.bodies.available_permits(),
            state.limits.max_body_bytes
        );
        assert_eq!(
            state.connection_slots.available_permits(),
            state.limits.max_connections
        );
    }

    #[tokio::test]
    async fn accepted_egress_overflow_terminates_and_revokes_escaped_sender() {
        let (state, mut agent) = registry(ServerLimits {
            max_egress_frames: 1,
            ..ServerLimits::default()
        });
        let (id, response) = initialize(state.clone(), &mut agent).await;
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let connection = state.connections.lock().unwrap().get(&id).unwrap().clone();
        for _ in 0..2 {
            agent
                .tx
                .try_send_serialized(r#"{"jsonrpc":"2.0","method":"notice"}"#)
                .unwrap();
        }
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(state.connections.lock().unwrap().is_empty());
        assert!(
            agent
                .tx
                .try_send_serialized(r#"{"jsonrpc":"2.0","method":"notice"}"#)
                .is_err()
        );
        assert_eq!(
            connection.frame_slots.available_permits(),
            state.limits.max_egress_frames
        );
        assert_eq!(
            connection.byte_slots.available_permits(),
            state.limits.max_egress_bytes
        );
        assert!(connection.inner.lock().unwrap().streams.is_empty());
    }

    #[tokio::test]
    async fn response_body_saturation_terminates_instead_of_deadlocking_callback() {
        let (state, mut agent) = registry(ServerLimits {
            max_in_flight_posts: 1,
            ..ServerLimits::default()
        });
        let (id, response) = initialize(state.clone(), &mut agent).await;
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let _held = state.posts.clone().try_acquire_owned().unwrap();
        let response = handle_post(
            State(state.clone()),
            post_request(r#"{"jsonrpc":"2.0","id":2,"result":{}}"#, Some(&id)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(state.connections.lock().unwrap().is_empty());
        assert!(
            agent
                .tx
                .try_send_serialized(r#"{"jsonrpc":"2.0","method":"notice"}"#)
                .is_err()
        );
    }

    #[tokio::test]
    async fn legacy_component_fails_closed_without_a_bridge() {
        let state = BoundedAcpHttpServer::new(
            || {
                let (channel, peer) = agent_client_protocol::Channel::duplex();
                drop(peer);
                channel
            },
            ServerLimits::default(),
        )
        .unwrap()
        .state;
        let response = timeout(
            Duration::from_secs(2),
            handle_post(
                State(state.clone()),
                post_request(r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#, None),
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(state.connections.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bounded_router_rejects_websocket_upgrade() {
        let (state, _agent) = registry(ServerLimits::default());
        let request = Request::builder()
            .header(header::UPGRADE, "websocket")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            get_inner(state, request).unwrap_err(),
            StatusCode::NOT_IMPLEMENTED
        );
    }
}
