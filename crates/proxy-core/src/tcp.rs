use std::{
    error::Error,
    fmt,
    future::{poll_fn, Future},
    io,
    net::SocketAddr,
    pin::{pin, Pin},
    sync::atomic::{AtomicU64, Ordering},
    task::Poll,
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::{timeout, Instant},
};

use crate::drain::{DrainError, DrainTracker};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpProxyConfig {
    pub connect_timeout: Duration,
    pub stream_idle_timeout: Duration,
    pub write_idle_timeout: Duration,
}

impl Default for TcpProxyConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            stream_idle_timeout: Duration::from_secs(60 * 60),
            write_idle_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TcpProxy {
    drain: DrainTracker,
    config: TcpProxyConfig,
}

#[derive(Debug)]
pub enum TcpProxyError {
    Drain(DrainError),
    ConnectTimeout { timeout: Duration },
    Connect(io::Error),
    Proxy(io::Error),
}

impl TcpProxy {
    pub fn new(drain: DrainTracker, config: TcpProxyConfig) -> Self {
        Self { drain, config }
    }

    #[cfg(test)]
    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    #[cfg(test)]
    pub fn config(&self) -> &TcpProxyConfig {
        &self.config
    }

    pub async fn proxy(
        &self,
        client: TcpStream,
        upstream_addr: SocketAddr,
    ) -> Result<(), TcpProxyError> {
        let _permit = self.drain.try_acquire()?;
        let upstream = crate::connect_tcp(upstream_addr, self.config.connect_timeout)
            .await
            .map_err(|error| {
                if error.kind() == io::ErrorKind::TimedOut {
                    TcpProxyError::ConnectTimeout {
                        timeout: self.config.connect_timeout,
                    }
                } else {
                    TcpProxyError::Connect(error)
                }
            })?;

        proxy_streams_with_timeouts(
            client,
            upstream,
            Some(self.config.stream_idle_timeout),
            self.config.write_idle_timeout,
            None,
        )
        .await
        .map_err(TcpProxyError::Proxy)
    }
}

/// Copies bytes in both directions until each side has closed its half.
///
/// Each direction runs independently, so a stalled reader on one side never
/// stops the other direction. Three deadlines bound a session:
///
/// - `idle_timeout` ends it when both directions stay silent that long. `None`
///   leaves a quiet session open and relies on TCP keepalive to find a lost
///   peer.
/// - `write_timeout` bounds one pending write or shutdown. Progress in the
///   other direction does not extend it.
/// - `half_close_timeout` ends it that long after one side closed while the
///   other still holds its half open. `None` keeps a half-closed session,
///   which TCP protocols may use, until the idle timeout.
pub async fn proxy_streams_with_timeouts<Client, Upstream>(
    client: Client,
    upstream: Upstream,
    idle_timeout: Option<Duration>,
    write_timeout: Duration,
    half_close_timeout: Option<Duration>,
) -> io::Result<()>
where
    Client: AsyncRead + AsyncWrite + Unpin,
    Upstream: AsyncRead + AsyncWrite + Unpin,
{
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let (mut upstream_read, mut upstream_write) = tokio::io::split(upstream);
    let idle = idle_timeout.map(IdleClock::new);
    // Each future holds a 16 KiB buffer, so both are built in place.
    let mut to_upstream = pin!(copy_direction(
        &mut client_read,
        &mut upstream_write,
        write_timeout,
        idle.as_ref(),
    ));
    let mut to_client = pin!(copy_direction(
        &mut upstream_read,
        &mut client_write,
        write_timeout,
        idle.as_ref(),
    ));

    // Poll both directions on every wake, in a fixed order.
    let (mut upstream_closed, mut client_closed) = (false, false);
    let mut linger: Option<Pin<Box<tokio::time::Sleep>>> = None;
    poll_fn(|cx| {
        if !upstream_closed {
            if let Poll::Ready(closed) = to_upstream.as_mut().poll(cx) {
                closed?;
                upstream_closed = true;
            }
        }
        if !client_closed {
            if let Poll::Ready(closed) = to_client.as_mut().poll(cx) {
                closed?;
                client_closed = true;
            }
        }
        match (upstream_closed, client_closed, half_close_timeout) {
            (true, true, _) => Poll::Ready(Ok(())),
            (false, false, _) | (_, _, None) => Poll::Pending,
            // One side closed. The other has `limit` to finish; then the
            // session is over.
            (_, _, Some(limit)) => linger
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)))
                .as_mut()
                .poll(cx)
                .map(Ok),
        }
    })
    .await
}

async fn copy_direction<R, W>(
    reader: &mut R,
    writer: &mut W,
    write_timeout: Duration,
    idle: Option<&IdleClock>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = [0_u8; 16 * 1024];

    loop {
        let read = wait_for_progress(reader.read(&mut buffer), idle).await?;
        if read == 0 {
            return timeout(write_timeout, wait_for_progress(writer.shutdown(), idle))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "TCP shutdown write stalled")
                })?;
        }

        record_activity(idle);
        let mut written = 0;
        while written < read {
            let count = timeout(
                write_timeout,
                wait_for_progress(writer.write(&buffer[written..read]), idle),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TCP write stalled"))??;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "stream stopped accepting bytes",
                ));
            }
            written += count;
            record_activity(idle);
        }
    }
}

fn record_activity(idle: Option<&IdleClock>) {
    if let Some(idle) = idle {
        idle.record_activity();
    }
}

async fn wait_for_progress<F, T>(future: F, idle: Option<&IdleClock>) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    let Some(idle) = idle else {
        return future.await;
    };
    tokio::pin!(future);
    loop {
        match timeout(idle.remaining(), &mut future).await {
            Ok(result) => return result,
            Err(_) if idle.remaining().is_zero() => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "stream idle timeout elapsed",
                ));
            }
            Err(_) => {}
        }
    }
}

/// Idleness shared by both directions: progress in either keeps the session.
#[derive(Debug)]
struct IdleClock {
    timeout: Duration,
    started_at: Instant,
    last_activity_nanos: AtomicU64,
}

impl IdleClock {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            started_at: Instant::now(),
            last_activity_nanos: AtomicU64::new(0),
        }
    }

    fn record_activity(&self) {
        self.last_activity_nanos
            .store(self.elapsed_nanos(), Ordering::Relaxed);
    }

    fn remaining(&self) -> Duration {
        let idle_for = Duration::from_nanos(
            self.elapsed_nanos()
                .saturating_sub(self.last_activity_nanos.load(Ordering::Relaxed)),
        );
        self.timeout.saturating_sub(idle_for)
    }

    fn elapsed_nanos(&self) -> u64 {
        self.started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }
}

impl From<DrainError> for TcpProxyError {
    fn from(error: DrainError) -> Self {
        Self::Drain(error)
    }
}

impl fmt::Display for TcpProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drain(error) => write!(f, "{error}"),
            Self::ConnectTimeout { timeout } => {
                write!(f, "upstream connect timed out after {timeout:?}")
            }
            Self::Connect(error) => write!(f, "upstream connect failed: {error}"),
            Self::Proxy(error) => write!(f, "tcp proxy failed: {error}"),
        }
    }
}

impl Error for TcpProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Drain(error) => Some(error),
            Self::ConnectTimeout { .. } => None,
            Self::Connect(error) | Self::Proxy(error) => Some(error),
        }
    }
}
