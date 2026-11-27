# Proxy protocol contract

Frontline and sidecar use the same HTTP server path for HTTP/1.1, HTTP/2 and
HTTP/1.1 WebSocket upgrades. A later request on a keep-alive connection can
upgrade. Normal upstream HTTP connections remain pooled, including the separate
HTTP/2 prior-knowledge client used for h2c and gRPC. An idle HTTP keep-alive
connection does not hold an active-work drain permit.

## Protocol per hop

The protocol a client speaks to frontline is independent of the protocol the
app serves.

| Hop | Protocol |
| --- | --- |
| Client to frontline | HTTP/1.1 or HTTP/2, chosen by the client. TLS-terminated listeners offer `h2` and `http/1.1`. |
| Frontline to sidecar | The client's HTTP version. The sidecar serves both. |
| Sidecar to app | The protocol declared for the app port: `http1` (the default) or `h2c`. |

The sidecar reads the app protocol from `SLEEPYPODS_APP_PROTOCOL`, which the
control plane renders from the workload class.

- An `http1` port receives every request as HTTP/1.1. A request from an
  HTTP/2 client carries the client's authority as `Host`.
- An `h2c` port receives every request as prior-knowledge HTTP/2, including
  requests from HTTP/1.1 clients. `:authority` is the client's authority.
- gRPC needs HTTP/2 to the app. On an `http1` port the sidecar answers a
  request with content type `application/grpc` (or a `+codec` form) itself:
  HTTP 200 with `grpc-status: 12` (unimplemented) and a `grpc-message`.
  gRPC-Web uses its own content type and travels over HTTP/1.1.
- A request that asked for trailers keeps `TE: trailers` on every HTTP/2 hop,
  which gRPC servers require. An HTTP/1.1 hop removes it.
- WebSocket upgrades use HTTP/1.1 on every hop, on either app protocol. An
  `h2c` app that accepts WebSockets also serves HTTP/1.1 upgrades on its port.

Frontline replaces spoofable `Forwarded` and `X-Forwarded-*` fields using the
peer address, the listener scheme and the client's authority: the `Host` field,
or the request target for an HTTP/2 client. The sidecar preserves that trusted
edge output and takes the client's authority from `Host`, then
`X-Forwarded-Host`, then the request target.

## WebSocket handshakes

Each proxy completes the upstream handshake before returning `101 Switching
Protocols`. A persistently refused connection exhausts the shared setup
deadline and returns 504; other connection failures return 502. Upstream
handshake timeouts return 504. An upstream HTTP rejection retains its status,
end-to-end response headers and complete body, up to 64 KiB. A larger or
incomplete rejection body fails with 502 or 504 instead of returning a
truncated representation. An upstream that switches to a protocol other than
WebSocket returns 502. Upgrade response bodies are empty.

The handshake passes through unchanged in both directions. The upstream
receives the client's `Sec-WebSocket-Key`, version, offered subprotocols and
extensions with Authorization, Cookie, Origin, the original Host and custom
application headers. The client receives the upstream's
`Sec-WebSocket-Accept`, selected subprotocol, accepted extensions and other
response headers, including repeated Set-Cookie. Standard hop-by-hop headers
and headers named by Connection are removed, and each hop writes its own
`Connection: upgrade` and `Upgrade: websocket`. The peers validate the
negotiation; a proxy forwards it.

After the 101 each proxy relays the upgraded bytes and parses no frame.
Extensions such as permessage-deflate therefore work end to end, a message
has no proxy size limit, and a session holds two fixed 16 KiB relay buffers
per hop whatever its peers send. Each direction is relayed independently, so
a peer that stops reading stalls only its own direction. HTTP/2 extended
CONNECT WebSockets and encrypted `wss` backend origins are not supported;
frontline does not advertise the extended CONNECT setting, so HTTP/2 clients
open WebSockets over HTTP/1.1. HTTPS clients can use frontline TLS termination
and the ordinary HTTP/1.1 WebSocket upgrade path.

## Listeners and sockets

Every listener runs one accept loop. A failed `accept` never ends it: an error
that belongs to one connection (aborted, reset, unreachable peer) is skipped,
and any other error, such as an exhausted file-descriptor table, pauses the
loop for 100 ms. When shutdown starts the listening socket closes first, so
new connections are refused while admitted work drains.

Every accepted and dialled socket has Nagle disabled and TCP keepalive enabled:
probes start after 60 seconds of silence and repeat every 10 seconds, and
three unanswered probes end the connection. A peer that vanishes without
closing is found after about 90 seconds on any protocol.

## Setup and progress deadlines

- The first incoming HTTP request must finish headers within 10 seconds,
  including a silent connection or incomplete HTTP/2 preface. HTTP/1.1 headers
  have a 64 KiB buffer limit.
- An HTTP/1.1 server connection waits up to 90 seconds for each later request
  (`SLEEPYPODS_PROXY_KEEPALIVE_IDLE_TIMEOUT_MS`), measured until that request's
  headers are complete. The upstream pool idle timeout must be shorter, so a
  proxy retires a pooled connection before the next hop closes it; startup
  rejects configuration where it is not.
- Ordinary HTTP upstream connection establishment has a 10-second deadline.
  HTTP/1, HTTP/2, WebSocket, TCP and SNI share pre-dispatch connection recovery.
  Completed connect-stage errors containing `ConnectionRefused` or `TimedOut`
  may retry, at most once per 50 ms; other errors return immediately. The
  initial probing window lasts the smaller of one second and a quarter of
  the available setup budget. During that window TCP attempts are short
  probes: the first is capped at 100 ms and each later cap doubles, bounded by
  the remaining probing time. A backend whose address starts answering a
  moment after it turns ready is therefore reached about one short probe
  later. Attempts after the window get the remaining overall budget, so a
  slow path can still complete. This does not depend on which per-address
  error Hyper preserves.
  Attempts retain the same admission/pool permit and total deadline.
  Each old TCP attempt is dropped before its replacement starts. This can
  recover a transient stale Service flow while allowing a slower replacement;
  it does not guarantee recovery from a continuing network outage.
  Raw TCP, WebSocket and SNI resolve DNS once. HTTP uses Hyper's TCP-only
  timeout after DNS completes; a completed resolver refusal/timeout may retry,
  but a slow lookup is never canceled by the short TCP timer or overlapped by
  another attempt. The overall deadline ends setup without another lookup;
  platform blocking DNS work may still outlive caller cancellation until the
  OS resolver returns. Protocol handshakes, HTTP requests, bodies and
  established streams are never replayed by this connection policy.
- HTTP/2 server connections use a 30-second keepalive interval and a 10-second
  acknowledgement timeout to detect an unresponsive peer. Frontline's HTTP/2
  connections to a sidecar use the same interval, also while idle, with the
  setup timeout as the acknowledgement timeout. The sidecar sends no HTTP/2
  pings to the app: gRPC servers close a connection that pings too often, and
  the app shares the pod.
- WebSocket upstream connection and handshake, including a rejection body,
  has a 10-second deadline. Completing the downstream upgrade also has a
  10-second deadline. In an established session a pending write or shutdown
  has the 60-second write deadline. A quiet session has no idle deadline; TCP
  keepalive ends it when a peer is lost. After one peer closes its stream the
  other has 10 seconds to finish before the session ends.
- TCP's stream-idle deadline covers reads, partial writes and shutdown.
  Progress in either direction keeps the session alive. Two blocked writers
  cannot bypass the deadline. The default is one hour. A half-closed TCP
  session stays open until that deadline.

The production listeners allow the validated environment overrides below. There is
no total lifetime deadline on an active HTTP request/body, gRPC stream, TCP
session or WebSocket session. Setup deadlines do not turn a long stream into a
short request. HTTP response headers must arrive within 60 seconds without actual
upload-byte progress. Each nonempty buffer advancement resets that clock; an
empty frame or a stalled upload does not. Long-poll endpoints must return headers
within this budget, then may keep the body open indefinitely. A pooled
connection whose request timed out waiting for headers serves no later
request; streams already on it continue.

The drain permit covers a WebSocket's upstream setup and entire upgraded
session. Cancellation drops the permit and aborts an unfinished upstream HTTP
driver. Production listeners own their upgrade tasks, reap completed tasks and
abort remaining tasks after the shared drain grace period.

## Route resolution

Frontline routes a request by its exact host: the HTTP Host without port, or
the TLS SNI name, in lower case without trailing dots. The first request for a
host fetches the host's complete rule set from the control plane; every later
request for that host, on any path, is matched locally against the cached set.
The first rule whose path prefix covers the request path is the route. An exact
host ranks before a wildcard suffix, a longer suffix before a shorter one, and
a longer path prefix before a shorter one. A prefix matches itself and whole
path segments below it.

| Request | Response |
| --- | --- |
| Missing or malformed Host, or a host over 253 bytes | 400, with no lookup |
| Path over 4,096 bytes | 414, with no lookup |
| No rule covers the host and path | 404 |
| The rule's instance is being deleted | 503 |
| The lookup failed, timed out or found 64 other hosts already resolving | 503 |
| 256 requests are already waiting for a lookup or a wake | 503 |
| The instance did not run within the route timeout | 503 |

A lookup failure affects only requests for its own host; hosts already cached
keep serving. A request for an instance that is not running waits for it under
the route timeout, and the proxy asks the control plane to wake each instance
generation once. Requests waiting on a wake hold no lookup slot, so lookups for
other hosts proceed. A passthrough or terminated TLS connection whose SNI name
has no route, or cannot be routed, is closed.

The control plane sends a host's rule set again whenever a binding or instance
it depends on changes, and the proxy replaces the cached set. A proxy that may
have missed a change discards its whole cache before it serves another request.

## Activation and automatic idle sleep

A generation's persisted Ready transition starts a fixed 190-second activation
floor. Automatic sleep requires both that floor and the resolved class idle
interval to have elapsed since Ready, plus the sidecar's ordinary full interval
with no active work. The floor is unconditional: a quick successful request does
not release protection for another first request still establishing its backend
connection. Restarting a control plane does not restart the floor. A wake with
no application request still becomes eligible after this finite interval; a
300-second class idle interval continues to dominate the default minimum uptime.
The internal explicit `BeginSleep` operation can bypass the automatic-idle floor;
there is currently no public operator Sleep RPC.

Supported frontline listener configuration must satisfy:

```text
route timeout + max(setup timeout, upstream HTTP header idle timeout) <= 190 seconds
```

The defaults are `130 + max(10, 60) = 190` seconds. Both environment parsing and
public listener entry points reject larger combinations, including overflow.
This is a finite activation handoff window, not a lifetime cap on an admitted
request or established stream. It does not replay failed requests.

The sidecar owns temporary drain work from public HTTP socket admission through
the first handler's response headers. This covers a delayed HTTP/1 header or
HTTP/2 preface/first stream, then overlaps briefly with ordinary request/body or
WebSocket ownership so there is no zero-work gap. An incomplete first request
expires under the setup deadline; cancellation, overload and errors release the
guard. Subsequent idle keepalive retains no drain work. Private readiness probes
never enter this accounting. The runtime drain gauge can therefore briefly read
two for one first HTTP request: it counts admitted setup and stream work, not an
exact number of concurrent user streams.

Known activation deferral uses an advisory database-clock age check before
Kubernetes membership reads, followed by an authoritative generation-fenced
transaction when eligible. `FailedPrecondition` may include
`sleepypods-idle-retry-after-ms` in the range `1..=190000`. Updated sidecars defer
that next report without polling Kubernetes; real activity interrupts the wait
and starts a complete new idle interval. Missing, malformed or oversized hints
use the ordinary configured retry policy. A deferral is not an acknowledgement
of Pod membership or permission to sleep.

The sidecar reports for as long as it runs. After any answer, including an
accepted report, it reports again every retry backoff while the pod stays
quiet, and any activity starts a complete new idle interval. An accepted report
changes nothing in the sidecar: it keeps accepting connections until SIGTERM
starts its drain, so an instance that a wake returns to Running during drain
serves on the same pod and can sleep again later. One `ReportIdle` call has the
setup timeout as its deadline, and the control-plane channel uses HTTP/2
keepalive (30-second interval, 10-second acknowledgement timeout, also while
idle) and TCP keepalive, so a control plane that stops answering costs one
retry.

## Data-plane resource limits

These environment variables apply independently to frontline and sidecar. All
capacity values must be in 1..=1000000; timeout values must be in 1..=3600000 ms.
Invalid values fail startup. Frontline shares capacities across HTTP, TLS
termination and SNI passthrough listeners.

| Variable | Default | Scope |
| --- | --- | --- |
| `SLEEPYPODS_PROXY_MAX_CONNECTIONS` | 1024 | Accepted downstream sockets, including keepalive and upgrades |
| `SLEEPYPODS_PROXY_MAX_REQUESTS` | 1024 | Active upload/response delivery, WebSocket or TCP/SNI sessions |
| `SLEEPYPODS_PROXY_MAX_HANDSHAKES` | 128 | Initial HTTP/TLS/SNI setup and WebSocket negotiation |
| `SLEEPYPODS_PROXY_MAX_HTTP2_STREAMS` | 128 | Advertised concurrent streams per h2 connection |
| `SLEEPYPODS_PROXY_MAX_UPSTREAM_CONNECTIONS` | 1024 | Active plus idle pooled HTTP upstream sockets across origins |
| `SLEEPYPODS_PROXY_MAX_IDLE_PER_HOST` | 8 | Idle HTTP sockets per origin in each protocol pool |
| `SLEEPYPODS_PROXY_POOL_IDLE_TIMEOUT_MS` | 30000 | Idle pooled HTTP socket eviction |
| `SLEEPYPODS_PROXY_SETUP_TIMEOUT_MS` | 10000 | Initial protocol setup, the first request's headers, upstream connects and WS negotiation |
| `SLEEPYPODS_PROXY_KEEPALIVE_IDLE_TIMEOUT_MS` | 90000 | Wait for the next request on an HTTP/1.1 server connection; must exceed the pool idle timeout |
| `SLEEPYPODS_PROXY_UPSTREAM_HEADER_IDLE_TIMEOUT_MS` | 60000 | Response-header wait without upload-byte progress |
| `SLEEPYPODS_PROXY_WRITE_IDLE_TIMEOUT_MS` | 60000 | Pending socket/stream delivery writes, including TCP/SNI |

Admission has no wait queue: excess accepted sockets are closed before spawning
connection work; excess HTTP requests return 503 with Retry-After: 1. Hyper also
limits h2 streams before service dispatch. Pooled HTTP upstream saturation returns
503 until an active socket closes or idle eviction frees a slot. WS/TCP/SNI
upstream sockets follow their admitted active sessions and are separate from the
HTTP pool cap.

Both the upload body and produced response buffers retain request admission and
drain activity until their queued bytes are delivered or canceled, even for a
final EOS frame. An early upstream response does not finish its still-active
upload. An idle HTTP keepalive connection retains only socket admission. An established WebSocket
retains both socket and request admission through its upgrade task.

HTTP/2 flow control can stall a stream while the underlying TCP connection remains
responsive. The delivery deadline measures advancement of already-produced
response bytes; waiting for the application to produce its next frame is exempt.
Hyper's public server API requires closing the affected connection on such a
stall, which also cancels other streams sharing that connection. It does not close
other client connections. A stalled upload similarly cancels its selected pooled
upstream HTTP connection; other requests sharing that upstream h2 connection may
also fail. The selected connection is poisoned so the pool does not reuse it.
Application waits with no produced upload bytes are exempt after early response
headers, and there is no universal active-body lifetime limit.

The delivery deadline for an HTTP/1.1 body is its socket's write deadline: a
stalled HTTP/1.1 peer stalls the socket. Only HTTP/2 bodies run the separate
per-stream watchdog described above.

TCP/SNI retain their separate one-hour stream-idle policy. The write override
bounds pending writes and shutdown, not quiet but established sessions. Metrics
listeners use fixed bounds: 32 accepted sockets, one scrape per connection, a
16 KiB header buffer and a five-second total scrape lifetime. Completed metrics
tasks are reaped and remaining tasks are aborted on shutdown.

## Private sidecar readiness

The rendered sidecar uses a separate `/ready` listener after its initial
control-plane connection and actual proxy bind. It checks TCP acceptance at
`127.0.0.1:SLEEPYPODS_APP_PORT` with a 500 ms deadline, sends no application bytes,
and does not affect idle accounting. Initial control-plane setup shares the
configured setup timeout; later control-plane outages do not affect this check.
This supports apps bound only to loopback. TCP acceptance is the readiness
contract: an app requiring semantic readiness must delay its bind until ready,
or use a compatible custom sidecar implementing the stronger check. Structured
templates do not currently expose application semantic probes.

`SLEEPYPODS_SIDECAR_READINESS_LISTEN_ADDR` is optional outside rendered Pods. It
must use a nonzero port distinct from app, proxy, and metrics ports. The renderer
chooses an unused port starting at 15001. The private health listener allows
eight sockets/requests/handshakes, one h2 stream per connection, and a one-second
whole health-connection lifetime. A sibling listener failure signals shutdown;
the binary waits for all listener and idle-report task cleanup before returning
the first error. These health deadlines do not limit user request lifetimes.

Frontline's initial control-plane Channel setup has a fixed 60-second overall
budget, including retry waits. SIGINT/SIGTERM cancellation is active before that
setup. All three binaries complete their existing async shutdown/drain work,
then allow at most one additional second for final Tokio runtime teardown so a
leftover blocking DNS worker cannot prevent process exit. This final teardown
bound does not replace or shorten the configured active-work drain periods.
