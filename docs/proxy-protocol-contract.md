# Proxy protocol contract

Frontline and sidecar use the same HTTP server path for HTTP/1.1, HTTP/2 and
HTTP/1.1 WebSocket upgrades. A later request on a keep-alive connection can
upgrade. Normal upstream HTTP connections remain pooled, including the separate
HTTP/2 prior-knowledge client used for h2c and gRPC. An idle HTTP keep-alive
connection does not hold an active-work drain permit.

## WebSocket handshakes

Each proxy validates the client handshake and completes the upstream handshake
before returning `101 Switching Protocols`. A persistently refused connection
exhausts the shared setup deadline and returns 504; other connection failures
return 502. Upstream handshake timeouts return 504. An upstream HTTP rejection retains its
status, end-to-end response headers and complete body, up to 64 KiB. A larger or
incomplete rejection body fails with 502 or 504 instead of returning a truncated
representation. Upgrade response bodies are empty.

Authorization, Cookie, Origin, the original Host, custom application headers,
offered subprotocols and relevant response headers (including repeated Set-Cookie)
are preserved. Standard hop-by-hop headers and headers named by Connection are
removed. Each hop generates its own challenge and validates its own response;
an upstream can select at most one subprotocol actually offered by the client.
Frontline replaces spoofable Forwarded and X-Forwarded-* fields using the peer
address, listener scheme and original Host. Sidecar preserves that trusted edge
output.

The relay supports plain RFC 6455 frames. Client extension offers are omitted
when dialing upstream, so clients offering compression can negotiate an
uncompressed connection. An upstream claiming any extension is rejected before
101. HTTP/2 extended CONNECT WebSockets and encrypted `wss` backend origins are
not supported. HTTPS clients can use frontline TLS termination and the ordinary
HTTP/1.1 WebSocket upgrade path.

## Setup and progress deadlines

- The first incoming HTTP request must finish headers within 10 seconds,
  including a silent connection or incomplete HTTP/2 preface. HTTP/1.1 headers
  have a 64 KiB buffer limit and a 10-second per-request read deadline.
- Ordinary HTTP upstream connection establishment has a 10-second deadline.
  HTTP/1, HTTP/2, WebSocket, TCP and SNI share pre-dispatch connection recovery.
  Completed connect-stage errors containing `ConnectionRefused` or `TimedOut`
  may retry, at most once per 50 ms; other errors return immediately. The
  initial probing window lasts the smaller of one second and a quarter of
  the available setup budget. During that window each TCP cap is bounded by
  the remaining probing time; later attempts get the remaining overall
  budget. This does not depend on which per-address error Hyper preserves.
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
- HTTP/2 uses a 30-second keepalive interval and a 10-second acknowledgement
  timeout to detect an unresponsive peer.
- WebSocket upstream connection and handshake, including a rejection body,
  has a 10-second deadline. Completing the downstream upgrade also has a
  10-second deadline. Established frame writes have a 60-second deadline;
  close writes, flushes and waiting for a close response use 10 seconds.
- TCP's existing stream-idle deadline now covers reads, partial writes and
  shutdown. Progress in either direction keeps the session alive. Two blocked
  writers cannot bypass the deadline. The default is one hour.

The WebSocket library API allows callers to configure handshake, write and close
durations. The production listeners allow the validated environment overrides below. There is
no total lifetime deadline on an active HTTP request/body, gRPC stream, TCP
session or WebSocket session. Setup deadlines do not turn a long stream into a
short request. HTTP response headers must arrive within 60 seconds without actual
upload-byte progress. Each nonempty buffer advancement resets that clock; an
empty frame or a stalled upload does not. Long-poll endpoints must return headers
within this budget, then may keep the body open indefinitely.

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
| `SLEEPYPODS_PROXY_SETUP_TIMEOUT_MS` | 10000 | Initial protocol setup, HTTP/1 headers, upstream connects and WS negotiation |
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
