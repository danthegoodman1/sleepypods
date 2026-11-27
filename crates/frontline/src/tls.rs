use std::{
    error::Error,
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

use http::Uri;
use proxy_core::{
    read_tls_client_hello_prefix, TcpProxyConfig, TlsClientHelloError, TlsClientHelloSni,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{
    rustls::{self},
    LazyConfigAcceptor,
};

use crate::{ReadyBackend, RequestIdentityError, RouteRequestIdentity};

pub use crate::certificates::TlsCertificateStore;

#[derive(Clone, Debug)]
pub struct FrontlineTlsAdapter {
    certificates: TlsCertificateStore,
    resources: proxy_core::ProxyResourceConfig,
}

#[derive(Debug)]
pub struct TerminatedTls<IO> {
    pub identity: RouteRequestIdentity,
    pub stream: tokio_rustls::server::TlsStream<IO>,
}

#[derive(Debug)]
pub struct TlsPassthroughClientHello {
    identity: RouteRequestIdentity,
    bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum TlsTerminationError {
    ClientHello(io::Error),
    MissingSni,
    InvalidSni(RequestIdentityError),
    UnknownCertificate,
    Certificate(crate::certificates::CertificateLookupError),
    Handshake(io::Error),
}

#[derive(Debug)]
pub enum TlsPassthroughError {
    Backend(TlsPassthroughBackendError),
    ClientHello(io::Error),
    NotTls(TlsClientHelloError),
    MissingSni,
    InvalidSni(RequestIdentityError),
    Connect(io::Error),
    Proxy(io::Error),
}

#[derive(Debug)]
pub enum TlsPassthroughBackendError {
    InvalidUri(http::uri::InvalidUri),
    MissingScheme,
    MissingAuthority,
    UnsupportedScheme(String),
    InvalidAuthority(String),
}

impl FrontlineTlsAdapter {
    pub fn new(certificates: TlsCertificateStore) -> Self {
        Self {
            certificates,
            resources: proxy_core::ProxyResourceConfig::default(),
        }
    }

    pub fn with_resource_config(mut self, config: proxy_core::ProxyResourceConfig) -> Self {
        self.resources = config;
        self
    }

    pub async fn terminate<IO>(&self, io: IO) -> Result<TerminatedTls<IO>, TlsTerminationError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        tokio::time::timeout(
            crate::certificates::CERTIFICATE_HANDSHAKE_TIMEOUT,
            self.terminate_inner(io),
        )
        .await
        .map_err(|_| {
            TlsTerminationError::Certificate(crate::certificates::CertificateLookupError::Deadline)
        })?
    }

    async fn terminate_inner<IO>(&self, io: IO) -> Result<TerminatedTls<IO>, TlsTerminationError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let acceptor = LazyConfigAcceptor::new(rustls::server::Acceptor::default(), io);
        let start = acceptor.await.map_err(TlsTerminationError::ClientHello)?;
        let client_hello = start.client_hello();
        let sni = client_hello
            .server_name()
            .ok_or(TlsTerminationError::MissingSni)?;
        let identity = RouteRequestIdentity::sni(sni).map_err(TlsTerminationError::InvalidSni)?;
        let config = self
            .certificates
            .resolve(sni)
            .await
            .map_err(TlsTerminationError::Certificate)?
            .ok_or(TlsTerminationError::UnknownCertificate)?;
        let stream = start
            .into_stream(config)
            .await
            .map_err(TlsTerminationError::Handshake)?;

        Ok(TerminatedTls { identity, stream })
    }

    pub async fn read_passthrough_client_hello<Client>(
        &self,
        client: &mut Client,
    ) -> Result<TlsPassthroughClientHello, TlsPassthroughError>
    where
        Client: AsyncRead + Unpin,
    {
        let prefix = read_tls_client_hello_prefix(client)
            .await
            .map_err(TlsPassthroughError::ClientHello)?;
        let sni = match &prefix.outcome {
            Ok(TlsClientHelloSni::Sni { hostname }) => hostname.clone(),
            Ok(TlsClientHelloSni::NoSni) | Ok(TlsClientHelloSni::Incomplete { .. }) => {
                return Err(TlsPassthroughError::MissingSni);
            }
            Err(error) => return Err(TlsPassthroughError::NotTls(error.clone())),
        };
        let identity = RouteRequestIdentity::sni(&sni).map_err(TlsPassthroughError::InvalidSni)?;

        Ok(TlsPassthroughClientHello {
            identity,
            bytes: prefix.bytes,
        })
    }

    /// Relays the connection to the backend, starting with the ClientHello
    /// bytes already read from the client.
    pub async fn passthrough_prefixed<Client>(
        &self,
        ready: &ReadyBackend,
        client: Client,
        client_hello: TlsPassthroughClientHello,
    ) -> Result<(), TlsPassthroughError>
    where
        Client: AsyncRead + AsyncWrite + Unpin,
    {
        let upstream = passthrough_backend_addr(ready)?;
        let upstream = proxy_core::connect_tcp(upstream, self.resources.setup_timeout())
            .await
            .map_err(TlsPassthroughError::Connect)?;
        let client = PrefixedStream::new(client_hello.bytes, client);
        proxy_core::proxy_streams_with_timeouts(
            client,
            upstream,
            Some(TcpProxyConfig::default().stream_idle_timeout),
            self.resources.write_idle_timeout(),
            None,
        )
        .await
        .map_err(TlsPassthroughError::Proxy)
    }
}

impl TlsPassthroughClientHello {
    pub fn identity(&self) -> &RouteRequestIdentity {
        &self.identity
    }
}

pub fn passthrough_backend_addr(
    ready: &ReadyBackend,
) -> Result<String, TlsPassthroughBackendError> {
    let uri: Uri = ready
        .backend
        .uri()
        .parse()
        .map_err(TlsPassthroughBackendError::InvalidUri)?;
    let scheme = uri
        .scheme_str()
        .ok_or(TlsPassthroughBackendError::MissingScheme)?;
    if scheme != "tcp" {
        return Err(TlsPassthroughBackendError::UnsupportedScheme(
            scheme.to_owned(),
        ));
    }
    let authority = uri
        .authority()
        .ok_or(TlsPassthroughBackendError::MissingAuthority)?
        .clone();
    if authority.port_u16().is_none() {
        return Err(TlsPassthroughBackendError::InvalidAuthority(
            authority.as_str().to_owned(),
        ));
    }

    Ok(authority.as_str().to_owned())
}

struct PrefixedStream<IO> {
    prefix: Vec<u8>,
    prefix_pos: usize,
    inner: IO,
}

impl<IO> PrefixedStream<IO> {
    fn new(prefix: Vec<u8>, inner: IO) -> Self {
        Self {
            prefix,
            prefix_pos: 0,
            inner,
        }
    }
}

impl<IO> AsyncRead for PrefixedStream<IO>
where
    IO: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.prefix_pos < self.prefix.len() {
            let remaining = &self.prefix[self.prefix_pos..];
            let len = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..len]);
            self.prefix_pos += len;
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<IO> AsyncWrite for PrefixedStream<IO>
where
    IO: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl From<TlsPassthroughBackendError> for TlsPassthroughError {
    fn from(error: TlsPassthroughBackendError) -> Self {
        Self::Backend(error)
    }
}

impl fmt::Display for TlsTerminationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientHello(error) => write!(f, "TLS ClientHello read failed: {error}"),
            Self::MissingSni => f.write_str("TLS ClientHello is missing SNI"),
            Self::InvalidSni(error) => write!(f, "TLS SNI is invalid: {error}"),
            Self::UnknownCertificate => f.write_str("no TLS certificate is authorized for SNI"),
            Self::Certificate(error) => write!(f, "TLS certificate lookup failed: {error}"),
            Self::Handshake(error) => write!(f, "TLS handshake failed: {error}"),
        }
    }
}

impl Error for TlsTerminationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ClientHello(error) | Self::Handshake(error) => Some(error),
            Self::InvalidSni(error) => Some(error),
            Self::MissingSni | Self::UnknownCertificate => None,
            Self::Certificate(error) => Some(error),
        }
    }
}

impl fmt::Display for TlsPassthroughError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "TLS passthrough backend is invalid: {error}"),
            Self::ClientHello(error) => write!(f, "TLS ClientHello read failed: {error}"),
            Self::NotTls(error) => write!(f, "TLS ClientHello is invalid: {error}"),
            Self::MissingSni => f.write_str("TLS ClientHello is missing SNI"),
            Self::InvalidSni(error) => write!(f, "TLS SNI is invalid: {error}"),
            Self::Connect(error) => write!(f, "TLS passthrough connect failed: {error}"),
            Self::Proxy(error) => write!(f, "TLS passthrough proxy failed: {error}"),
        }
    }
}

impl Error for TlsPassthroughError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Backend(error) => Some(error),
            Self::ClientHello(error) | Self::Connect(error) | Self::Proxy(error) => Some(error),
            Self::NotTls(error) => Some(error),
            Self::InvalidSni(error) => Some(error),
            Self::MissingSni => None,
        }
    }
}

impl fmt::Display for TlsPassthroughBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUri(error) => write!(f, "backend URI is invalid: {error}"),
            Self::MissingScheme => write!(f, "backend URI is missing a scheme"),
            Self::MissingAuthority => write!(f, "backend URI is missing an authority"),
            Self::UnsupportedScheme(scheme) => {
                write!(
                    f,
                    "backend URI scheme {scheme:?} is not supported for TLS passthrough"
                )
            }
            Self::InvalidAuthority(authority) => {
                write!(f, "backend authority {authority:?} is not a socket address")
            }
        }
    }
}

impl Error for TlsPassthroughBackendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUri(error) => Some(error),
            Self::MissingScheme
            | Self::MissingAuthority
            | Self::UnsupportedScheme(_)
            | Self::InvalidAuthority(_) => None,
        }
    }
}

#[cfg(test)]
mod tests;
