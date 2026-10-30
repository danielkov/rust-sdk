# agent-client-protocol-http

HTTP/WebSocket transport for ACP agents.

- **Server**: `AcpHttpServer` exposes agents over HTTP + SSE with optional WebSocket upgrade
- **Client**: `HttpClient` connects over HTTP + SSE or WebSocket, selected by the URL scheme

The crate does not enable either transport side by default. Opt into the
surface you need:

```toml
agent-client-protocol-http = { version = "3", features = ["client"] }
# For a server instead:
# agent-client-protocol-http = { version = "3", features = ["server"] }
```

Cross-origin browser access is disabled by default. Configure `ServerOptions`
with `CorsOptions::allow_origins(...)` to allow specific browser origins.

Core SDK request cancellation support is forwarded through this transport.

Use `HttpClient::builder(url).configure_http(|http| ...).build()` to customize
reqwest headers, TLS, proxies, DNS, and timeouts. WebSocket handshakes use HTTP/1.1,
do not follow redirects, and are validated before sending ACP data. A raw
preconfigured TLS backend must itself use HTTP/1.1 ALPN.

Unconfigured WebSockets now share reqwest's proxy discovery and TLS trust defaults
with HTTP/SSE, rather than direct connections with bundled WebPKI roots. Use
`.configure_http(|http| http.no_proxy())` for direct connections and
`tls_certs_only` for explicit roots. Do not set handshake headers through
`reqwest::Proxy::headers`: plain-WS proxy headers can overwrite SDK request headers
after assembly, and the SDK cannot inspect that opaque configuration.

`HttpClient::from_http_client(exact_endpoint, client)` reuses an existing reqwest
client for HTTP/SSE only. The old `with_client` and `with_endpoint_and_client`
constructors remain as deprecated HTTP/SSE compatibility wrappers. WebSocket
calls to those constructors return a migration error; use the builder instead.

See the [client configuration and migration guide](https://agentclientprotocol.github.io/rust-sdk/http-transport.html#client-configuration).

## Opt-in bounded HTTP/SSE

Use `AcpHttpServer::new_bounded(factory, ServerLimits::default())?` and
`HttpClient::new(url)?.with_limits(HttpClientLimits::default())?` to select the
bounded transport path. Legacy transports remain unbounded. Configure
`ServerOptions` with its fluent setters; the options are non-exhaustive.

The bounded server also offers `.with_graceful_delete(Duration)` before
`into_router()`. It seals POST admission without consuming body/frame capacity,
then waits for actual component completion and clean output EOF. DELETE returns
202 only for clean completion; timeout/failure returns 503. Timeout or canceled
DELETE waiters do not abort accepted work or reopen admission. Concurrent DELETE
waiters join the same drain; after completion removes the connection, later
DELETE returns 404. Without this option, DELETE remains abortive.

The bounded path integrates directly with the core `BoundedChannel` and
producer admission. Finite defaults constrain serialized bytes, frame counts,
pending work, POSTs, sessions, and streams. Exhaustion fails explicitly rather
than retaining arbitrarily many waiters. Reservations survive dequeue and
remain held until the documented HTTP handoff or cancellation/drop.

These limits are **not** peer-delivery acknowledgments or hard process-heap
limits. Application conversion allocations, allocator overhead, and
reqwest/TLS/socket buffers are outside encoded-byte accounting. HTTP/SSE is
supported; bounded WebSocket endpoints are rejected. There is no SSE replay,
cursor recovery, or automatic retry of accepted/uncertain POSTs. See the book's
[HTTP transport chapter](https://agentclientprotocol.github.io/rust-sdk/http-transport.html)
for limits, ownership, and overload semantics.

See the [documentation](https://docs.rs/agent-client-protocol-http) for usage examples.
