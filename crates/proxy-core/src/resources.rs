//! Process-local data-plane bounds. Limits are acquired without wait queues.
use crate::{AdmissionError, AdmissionLimiter, AdmissionPermit};
use std::{
    error::Error,
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Sleep,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyResourceConfig {
    max_connections: usize,
    max_requests: usize,
    max_handshakes: usize,
    max_http2_streams: u32,
    max_upstream_connections: usize,
    max_idle_per_host: usize,
    pool_idle_timeout: Duration,
    setup_timeout: Duration,
    upstream_header_idle_timeout: Duration,
    write_idle_timeout: Duration,
    keepalive_idle_timeout: Duration,
}

impl Default for ProxyResourceConfig {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            max_requests: 1024,
            max_handshakes: 128,
            max_http2_streams: 128,
            max_upstream_connections: 1024,
            max_idle_per_host: 8,
            pool_idle_timeout: Duration::from_secs(30),
            setup_timeout: Duration::from_secs(10),
            upstream_header_idle_timeout: Duration::from_secs(60),
            write_idle_timeout: Duration::from_secs(60),
            keepalive_idle_timeout: Duration::from_secs(90),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidProxyResourceConfig(pub String);
impl fmt::Display for InvalidProxyResourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl Error for InvalidProxyResourceConfig {}

impl ProxyResourceConfig {
    pub fn with_limits(
        mut self,
        connections: usize,
        requests: usize,
        handshakes: usize,
        http2_streams: u32,
    ) -> Result<Self, InvalidProxyResourceConfig> {
        self.max_connections = connections;
        self.max_requests = requests;
        self.max_handshakes = handshakes;
        self.max_http2_streams = http2_streams;
        self.validate()?;
        Ok(self)
    }
    pub fn with_upstream_pool(
        mut self,
        connections: usize,
        idle_per_host: usize,
        idle_timeout: Duration,
    ) -> Result<Self, InvalidProxyResourceConfig> {
        self.max_upstream_connections = connections;
        self.max_idle_per_host = idle_per_host;
        self.pool_idle_timeout = idle_timeout;
        self.validate()?;
        Ok(self)
    }
    pub fn max_upstream_connections(self) -> usize {
        self.max_upstream_connections
    }
    pub fn max_idle_per_host(self) -> usize {
        self.max_idle_per_host
    }
    pub fn pool_idle_timeout(self) -> Duration {
        self.pool_idle_timeout
    }
    pub fn with_timeouts(
        mut self,
        setup: Duration,
        upstream_header_idle: Duration,
        write_idle: Duration,
    ) -> Result<Self, InvalidProxyResourceConfig> {
        self.setup_timeout = setup;
        self.upstream_header_idle_timeout = upstream_header_idle;
        self.write_idle_timeout = write_idle;
        self.validate()?;
        Ok(self)
    }
    /// Shared environment names apply independently to each data-plane process.
    pub fn from_vars(
        mut get: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, InvalidProxyResourceConfig> {
        let defaults = Self::default();
        let mut value = |name: &str, default: u64| -> Result<u64, InvalidProxyResourceConfig> {
            get(name)
                .map(|v| {
                    v.parse().map_err(|_| {
                        InvalidProxyResourceConfig(format!("{name} must be a positive integer"))
                    })
                })
                .unwrap_or(Ok(default))
        };
        let connections = value(
            "SLEEPYPODS_PROXY_MAX_CONNECTIONS",
            defaults.max_connections as u64,
        )?;
        let requests = value(
            "SLEEPYPODS_PROXY_MAX_REQUESTS",
            defaults.max_requests as u64,
        )?;
        let handshakes = value(
            "SLEEPYPODS_PROXY_MAX_HANDSHAKES",
            defaults.max_handshakes as u64,
        )?;
        let streams = value(
            "SLEEPYPODS_PROXY_MAX_HTTP2_STREAMS",
            defaults.max_http2_streams as u64,
        )?;
        if [connections, requests, handshakes, streams]
            .iter()
            .any(|v| !(1..=1_000_000).contains(v))
        {
            return Err(InvalidProxyResourceConfig(
                "proxy capacity limits must be in 1..=1000000".into(),
            ));
        }
        let upstream = value(
            "SLEEPYPODS_PROXY_MAX_UPSTREAM_CONNECTIONS",
            defaults.max_upstream_connections as u64,
        )?;
        let idle = value(
            "SLEEPYPODS_PROXY_MAX_IDLE_PER_HOST",
            defaults.max_idle_per_host as u64,
        )?;
        if [upstream, idle]
            .iter()
            .any(|v| !(1..=1_000_000).contains(v))
        {
            return Err(InvalidProxyResourceConfig(
                "upstream pool capacities must be in 1..=1000000".into(),
            ));
        }
        let pool_timeout = value(
            "SLEEPYPODS_PROXY_POOL_IDLE_TIMEOUT_MS",
            defaults.pool_idle_timeout.as_millis() as u64,
        )?;
        let setup = value(
            "SLEEPYPODS_PROXY_SETUP_TIMEOUT_MS",
            defaults.setup_timeout.as_millis() as u64,
        )?;
        let header = value(
            "SLEEPYPODS_PROXY_UPSTREAM_HEADER_IDLE_TIMEOUT_MS",
            defaults.upstream_header_idle_timeout.as_millis() as u64,
        )?;
        let write = value(
            "SLEEPYPODS_PROXY_WRITE_IDLE_TIMEOUT_MS",
            defaults.write_idle_timeout.as_millis() as u64,
        )?;
        let keepalive = value(
            "SLEEPYPODS_PROXY_KEEPALIVE_IDLE_TIMEOUT_MS",
            defaults.keepalive_idle_timeout.as_millis() as u64,
        )?;
        // Set both ends of the keep-alive relation before validating either.
        let mut config = defaults;
        config.keepalive_idle_timeout = Duration::from_millis(keepalive);
        config
            .with_upstream_pool(
                upstream as usize,
                idle as usize,
                Duration::from_millis(pool_timeout),
            )?
            .with_limits(
                connections as usize,
                requests as usize,
                handshakes as usize,
                streams as u32,
            )?
            .with_timeouts(
                Duration::from_millis(setup),
                Duration::from_millis(header),
                Duration::from_millis(write),
            )
    }
    fn validate(&self) -> Result<(), InvalidProxyResourceConfig> {
        if [
            self.max_connections,
            self.max_requests,
            self.max_handshakes,
            self.max_http2_streams as usize,
            self.max_upstream_connections,
            self.max_idle_per_host,
        ]
        .iter()
        .any(|v| !(1..=1_000_000).contains(v))
        {
            return Err(InvalidProxyResourceConfig(
                "proxy capacity limits must be in 1..=1000000".into(),
            ));
        }
        if [
            self.setup_timeout,
            self.upstream_header_idle_timeout,
            self.write_idle_timeout,
            self.pool_idle_timeout,
            self.keepalive_idle_timeout,
        ]
        .iter()
        .any(|v| *v < Duration::from_millis(1) || *v > Duration::from_secs(3600))
        {
            return Err(InvalidProxyResourceConfig(
                "proxy timeouts must be in 1ms..=1h".into(),
            ));
        }
        // Both proxies read the same settings. A pooled upstream connection
        // must retire before the server it reaches stops waiting for a request.
        if self.pool_idle_timeout >= self.keepalive_idle_timeout {
            return Err(InvalidProxyResourceConfig(
                "upstream pool idle timeout must be shorter than the HTTP keep-alive idle timeout"
                    .into(),
            ));
        }
        Ok(())
    }
    pub fn max_connections(self) -> usize {
        self.max_connections
    }
    pub fn max_requests(self) -> usize {
        self.max_requests
    }
    pub fn max_handshakes(self) -> usize {
        self.max_handshakes
    }
    pub fn max_http2_streams(self) -> u32 {
        self.max_http2_streams
    }
    pub fn setup_timeout(self) -> Duration {
        self.setup_timeout
    }
    pub fn upstream_header_idle_timeout(self) -> Duration {
        self.upstream_header_idle_timeout
    }
    pub fn write_idle_timeout(self) -> Duration {
        self.write_idle_timeout
    }
    pub fn keepalive_idle_timeout(self) -> Duration {
        self.keepalive_idle_timeout
    }
}

#[derive(Clone, Debug)]
pub struct ProxyAdmission {
    pub connections: AdmissionLimiter,
    pub requests: AdmissionLimiter,
    pub handshakes: AdmissionLimiter,
    config: ProxyResourceConfig,
}
impl ProxyAdmission {
    pub fn new(config: ProxyResourceConfig) -> Self {
        Self {
            connections: AdmissionLimiter::new(config.max_connections),
            requests: AdmissionLimiter::new(config.max_requests),
            handshakes: AdmissionLimiter::new(config.max_handshakes),
            config,
        }
    }
    pub fn config(&self) -> ProxyResourceConfig {
        self.config
    }
    pub fn admit_io<IO>(&self, io: IO) -> Result<AdmittedIo<IO>, AdmissionError> {
        Ok(AdmittedIo::new(
            io,
            self.connections.try_acquire()?,
            self.config.write_idle_timeout,
        ))
    }
}

/// The socket permit stays with the I/O after HTTP upgrades and TLS wrapping.
pub struct AdmittedIo<IO> {
    inner: IO,
    _permit: AdmissionPermit,
    write_timeout: Duration,
    write_deadline: Option<Pin<Box<Sleep>>>,
    flush_deadline: Option<Pin<Box<Sleep>>>,
    shutdown: Option<crate::Shutdown>,
    cancellation: Option<Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>>,
}
impl<IO> AdmittedIo<IO> {
    pub(crate) fn new(inner: IO, permit: AdmissionPermit, write_timeout: Duration) -> Self {
        Self {
            inner,
            _permit: permit,
            write_timeout,
            write_deadline: None,
            flush_deadline: None,
            shutdown: None,
            cancellation: None,
        }
    }
    pub(crate) fn cancellable(mut self) -> Self {
        let shutdown = crate::Shutdown::new();
        self.cancellation = Some(Box::pin(shutdown.clone().cancelled_owned()));
        self.shutdown = Some(shutdown);
        self
    }
    fn check_cancel(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self
            .cancellation
            .as_mut()
            .is_some_and(|cancel| cancel.as_mut().poll(cx).is_ready())
        {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "upstream HTTP delivery canceled",
            ))
        } else {
            Ok(())
        }
    }
    fn bound_write<T>(
        slot: &mut Option<Pin<Box<Sleep>>>,
        duration: Duration,
        cx: &mut Context<'_>,
        result: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match result {
            Poll::Ready(result) => {
                *slot = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                let deadline = slot.get_or_insert_with(|| Box::pin(tokio::time::sleep(duration)));
                match deadline.as_mut().poll(cx) {
                    Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "proxy socket write stalled",
                    ))),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}
impl<IO: AsyncRead + Unpin> AsyncRead for AdmittedIo<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check_cancel(cx)?;
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl<IO: AsyncWrite + Unpin> AsyncWrite for AdmittedIo<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check_cancel(cx)?;
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        let duration = self.write_timeout;
        Self::bound_write(&mut self.write_deadline, duration, cx, result)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.check_cancel(cx)?;
        let result = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        let duration = self.write_timeout;
        Self::bound_write(&mut self.write_deadline, duration, cx, result)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_cancel(cx)?;
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        let duration = self.write_timeout;
        Self::bound_write(&mut self.flush_deadline, duration, cx, result)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_cancel(cx)?;
        let result = Pin::new(&mut self.inner).poll_shutdown(cx);
        let duration = self.write_timeout;
        Self::bound_write(&mut self.flush_deadline, duration, cx, result)
    }
}

impl<IO: hyper_util::client::legacy::connect::Connection>
    hyper_util::client::legacy::connect::Connection for AdmittedIo<IO>
{
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        let connected = self.inner.connected();
        match &self.shutdown {
            Some(shutdown) => connected.extra(UpstreamShutdown(shutdown.clone())),
            None => connected,
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct UpstreamShutdown(pub crate::Shutdown);
impl<IO: fmt::Debug> fmt::Debug for AdmittedIo<IO> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdmittedIo")
            .field("inner", &self.inner)
            .field("permit", &self._permit)
            .field("write_timeout", &self.write_timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[test]
    fn validates_finite_capacities_and_deadlines_from_environment() {
        for name in [
            "SLEEPYPODS_PROXY_MAX_CONNECTIONS",
            "SLEEPYPODS_PROXY_MAX_REQUESTS",
            "SLEEPYPODS_PROXY_MAX_HANDSHAKES",
            "SLEEPYPODS_PROXY_MAX_HTTP2_STREAMS",
            "SLEEPYPODS_PROXY_MAX_UPSTREAM_CONNECTIONS",
            "SLEEPYPODS_PROXY_MAX_IDLE_PER_HOST",
            "SLEEPYPODS_PROXY_POOL_IDLE_TIMEOUT_MS",
            "SLEEPYPODS_PROXY_SETUP_TIMEOUT_MS",
            "SLEEPYPODS_PROXY_UPSTREAM_HEADER_IDLE_TIMEOUT_MS",
            "SLEEPYPODS_PROXY_WRITE_IDLE_TIMEOUT_MS",
            "SLEEPYPODS_PROXY_KEEPALIVE_IDLE_TIMEOUT_MS",
        ] {
            for invalid in ["0", "-1", "invalid", "18446744073709551615"] {
                assert!(
                    ProxyResourceConfig::from_vars(|key| (key == name).then(|| invalid.into()))
                        .is_err(),
                    "{name}={invalid}"
                );
            }
        }
        assert_eq!(
            ProxyResourceConfig::from_vars(|_| None).unwrap(),
            ProxyResourceConfig::default()
        );
    }
    #[test]
    fn upstream_pool_idle_must_stay_below_the_keepalive_idle_timeout() {
        let vars = |pool: &'static str, keepalive: &'static str| {
            ProxyResourceConfig::from_vars(move |key| match key {
                "SLEEPYPODS_PROXY_POOL_IDLE_TIMEOUT_MS" => Some(pool.into()),
                "SLEEPYPODS_PROXY_KEEPALIVE_IDLE_TIMEOUT_MS" => Some(keepalive.into()),
                _ => None,
            })
        };
        assert!(vars("30000", "30000").is_err());
        assert!(vars("90000", "60000").is_err());
        let config = vars("120000", "180000").unwrap();
        assert_eq!(config.pool_idle_timeout(), Duration::from_secs(120));
        assert_eq!(config.keepalive_idle_timeout(), Duration::from_secs(180));
        assert!(ProxyResourceConfig::default()
            .with_upstream_pool(1024, 8, Duration::from_secs(90))
            .is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn successful_flush_does_not_reset_a_blocked_write_deadline() {
        let admission = ProxyAdmission::new(ProxyResourceConfig::default());
        let (writer, _reader) = tokio::io::duplex(1);
        let mut writer = admission.admit_io(writer).unwrap();
        writer.write_all(b"a").await.unwrap();
        let error = std::future::poll_fn(|cx| {
            let pending_write = Pin::new(&mut writer).poll_write(cx, b"b");
            assert!(Pin::new(&mut writer).poll_flush(cx).is_ready());
            pending_write
        })
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        drop(writer);
        assert_eq!(admission.connections.in_flight(), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn inactive_io_has_no_lifetime_deadline_and_progress_recovers() {
        let admission = ProxyAdmission::new(ProxyResourceConfig::default());
        let (writer, mut reader) = tokio::io::duplex(1);
        let mut writer = admission.admit_io(writer).unwrap();
        tokio::time::sleep(Duration::from_secs(3600)).await;
        writer.write_all(b"a").await.unwrap();
        assert_eq!(reader.read_u8().await.unwrap(), b'a');
        writer.write_all(b"b").await.unwrap();
        assert_eq!(reader.read_u8().await.unwrap(), b'b');
    }
}
