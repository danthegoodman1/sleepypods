use std::{error::Error, future::Future, io, time::Duration};

use tokio::{
    net::{lookup_host, TcpStream, ToSocketAddrs},
    time::{timeout, Instant},
};

/// The first TCP attempt's cap. Each later probe doubles it.
const FIRST_PROBE: Duration = Duration::from_millis(100);

// Connect-only recovery: never wrap a protocol handshake, request, or relay.
// The caller MUST retain its existing total setup/handshake deadline. `connect`
// receives a TCP-only timeout and must finish/drop its socket before returning.
// In particular, do not apply that short timeout to an uncancelable DNS lookup.
//
// A backend that just became ready can drop the first SYN, because the Service
// address is programmed a moment after the endpoint turns ready. The first
// probes are therefore short and double from `FIRST_PROBE`, so a connection
// completes about one short probe after the address starts answering. After
// the probing window an attempt gets the remaining budget, which lets a slow
// but working path finish.
pub(crate) async fn retry_connect<F, Fut, T, E>(budget: Duration, mut connect: F) -> Result<T, E>
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Error + 'static,
{
    let started = Instant::now();
    let deadline = started + budget;
    // Bound early probing by wall-clock time, not the error Hyper preserves:
    // for multiple addresses it can report a refusal hiding a later timeout.
    let probing_ends = started + Duration::from_secs(1).min(budget / 4);
    let mut probe = FIRST_PROBE;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let probing_remaining = probing_ends.saturating_duration_since(Instant::now());
        let tcp_timeout = if probing_remaining.is_zero() {
            remaining
        } else {
            probe.min(probing_remaining).min(remaining)
        };
        probe = probe.saturating_mul(2);
        match connect(tcp_timeout).await {
            Err(error) if retryable_connect_error(&error) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            result => return result,
        }
    }
}

fn error_kind(error: &(dyn Error + 'static)) -> Option<io::ErrorKind> {
    if let Some(error) = error.downcast_ref::<io::Error>() {
        return Some(error.kind());
    }
    error.source().and_then(error_kind)
}

fn retryable_connect_error(error: &(dyn Error + 'static)) -> bool {
    matches!(
        error_kind(error),
        Some(io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut)
    )
}

/// Establish TCP before dispatching any protocol bytes, within one total budget.
/// DNS resolves once; only the resolved socket attempts receive the short
/// probes. A timed-out socket is dropped before its replacement starts.
/// After early probing, a slow attempt may use the rest of the total budget.
/// The returned socket has Nagle disabled and TCP keepalive enabled.
pub async fn connect_tcp<A: ToSocketAddrs>(address: A, budget: Duration) -> io::Result<TcpStream> {
    timeout(budget, connect_tcp_attempts(address, budget))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "upstream TCP setup timeout"))?
}

// For a caller (WebSocket) whose single outer deadline also covers the protocol
// handshake. There must not be a second competing total deadline/error mapping.
pub(crate) async fn connect_tcp_attempts<A: ToSocketAddrs>(
    address: A,
    budget: Duration,
) -> io::Result<TcpStream> {
    let deadline = Instant::now() + budget;
    let addresses = lookup_host(address).await?.collect::<Vec<_>>();
    let stream = retry_connect(
        deadline.saturating_duration_since(Instant::now()),
        |tcp_timeout| {
            let addresses = &addresses;
            async move {
                timeout(tcp_timeout, TcpStream::connect(addresses.as_slice()))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "TCP connection attempt timed out")
                    })?
            }
        },
    )
    .await?;
    crate::listener::configure_socket(&stream)?;
    Ok(stream)
}

#[cfg(test)]
mod tests;
