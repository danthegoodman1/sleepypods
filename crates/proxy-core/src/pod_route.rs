//! Connections that reach a sidecar at its Pod address.
//!
//! Frontline knows two addresses for a ready sidecar: its Service and the Pod
//! address the control plane recorded when the instance turned ready. The
//! Service turns routable only once kube-proxy programs it, a moment after
//! readiness; the Pod address answers at once. Pod addresses are reused, and
//! every sidecar of a class listens on the same port, so a stale Pod address
//! can reach another instance's sidecar.
//!
//! A connection to a Pod address therefore opens with an instance claim, and
//! frontline sends no request until the sidecar confirms it:
//!
//! ```text
//! frontline -> sidecar   \0sleepypods-instance <instance-id>\n
//! sidecar -> frontline   the same line, when it names the sidecar's instance
//! ```
//!
//! A sidecar closes a connection whose claim names another instance, so its
//! app never sees a request meant for another instance. Frontline then uses
//! the Service, which reaches only the instance's own Pods, and stops trying
//! that Pod address until the control plane certifies a new backend
//! generation. A connection whose first byte is anything else is plain HTTP.
//!
//! The pooled HTTP client keys connections by URI authority, so the Pod route
//! travels in the userinfo of the Service origin: connections for different
//! instances, Pod addresses or backend generations never share a pool.
use std::{
    collections::HashSet,
    fmt, io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use http::{uri::Authority, Uri};
use sleepypods_api::ids::{is_dns_label, DNS_LABEL_MAX_LEN};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};

/// What a Pod connection may take, TCP connect and claim together, before
/// frontline uses the Service.
pub const POD_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(250);

/// The first byte of a claim. No HTTP/1.1 request or HTTP/2 preface starts
/// with it.
const CLAIM_MARK: u8 = 0;
const CLAIM_PREFIX: &[u8] = b"\0sleepypods-instance ";
/// A claim carries an instance ID, which is a DNS label.
const MAX_CLAIM_BYTES: usize = CLAIM_PREFIX.len() + DNS_LABEL_MAX_LEN + 1;
/// Failed Pod routes one proxy remembers. A full memory starts over.
const MAX_FAILED_POD_ROUTES: usize = 4096;

/// A Pod address frontline tries before the Service: the instance the sidecar
/// there must serve and the backend generation that recorded the address.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PodRoute {
    instance_id: String,
    backend_generation: u64,
    address: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidPodRoute {
    /// The instance ID is not a DNS label.
    InstanceId(String),
    /// The Service origin has no authority, or cannot carry the route.
    InvalidOrigin,
}

impl PodRoute {
    pub fn new(
        instance_id: impl Into<String>,
        backend_generation: u64,
        address: SocketAddr,
    ) -> Result<Self, InvalidPodRoute> {
        let instance_id = instance_id.into();
        if !is_dns_label(&instance_id) {
            return Err(InvalidPodRoute::InstanceId(instance_id));
        }
        Ok(Self {
            instance_id,
            backend_generation,
            address,
        })
    }

    #[cfg(test)]
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// `service` with this route in its userinfo:
    /// `<instance>.<generation>.<address>@<service authority>`. The address is
    /// written with `-` for `:` and `_` before the port, which userinfo allows.
    pub fn origin(&self, service: &Uri) -> Result<Uri, InvalidPodRoute> {
        let authority = service.authority().ok_or(InvalidPodRoute::InvalidOrigin)?;
        let host = authority_without_userinfo(authority);
        let ip = self.address.ip().to_string().replace(':', "-");
        let routed = format!(
            "{}.{}.{ip}_{}@{host}",
            self.instance_id,
            self.backend_generation,
            self.address.port()
        );
        let mut parts = service.clone().into_parts();
        parts.authority =
            Some(Authority::try_from(routed).map_err(|_| InvalidPodRoute::InvalidOrigin)?);
        Uri::from_parts(parts).map_err(|_| InvalidPodRoute::InvalidOrigin)
    }

    /// The route an origin carries, if any.
    pub(crate) fn from_origin(uri: &Uri) -> Option<Self> {
        let (userinfo, _) = uri.authority()?.as_str().rsplit_once('@')?;
        let mut fields = userinfo.splitn(3, '.');
        let instance_id = fields.next()?;
        let backend_generation = fields.next()?.parse().ok()?;
        let (ip, port) = fields.next()?.rsplit_once('_')?;
        let ip: IpAddr = ip.replace('-', ":").parse().ok()?;
        let address = SocketAddr::new(ip, port.parse().ok()?);
        Self::new(instance_id, backend_generation, address).ok()
    }

    fn claim(&self) -> Vec<u8> {
        claim_line(&self.instance_id)
    }
}

/// The Pod routes one proxy has seen fail, so a stale Pod address costs one
/// attempt per backend generation.
#[derive(Clone, Debug, Default)]
pub(crate) struct PodRoutes {
    failed: Arc<Mutex<HashSet<PodRoute>>>,
}

impl PodRoutes {
    /// Connects to the Pod and has its sidecar confirm the instance, within
    /// [`POD_ATTEMPT_TIMEOUT`] or `budget`, whichever is shorter. `None` means
    /// the caller uses the Service.
    pub(crate) async fn connect(&self, route: &PodRoute, budget: Duration) -> Option<TcpStream> {
        if self.failed().contains(route) {
            return None;
        }
        let attempt = async {
            let mut stream = TcpStream::connect(route.address).await?;
            crate::listener::configure_socket(&stream)?;
            claim(&mut stream, &route.claim()).await?;
            Ok::<_, io::Error>(stream)
        };
        match tokio::time::timeout(POD_ATTEMPT_TIMEOUT.min(budget), attempt).await {
            Ok(Ok(stream)) => Some(stream),
            _ => {
                let mut failed = self.failed();
                if failed.len() >= MAX_FAILED_POD_ROUTES {
                    failed.clear();
                }
                failed.insert(route.clone());
                None
            }
        }
    }

    fn failed(&self) -> std::sync::MutexGuard<'_, HashSet<PodRoute>> {
        self.failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Sends the claim and waits for the sidecar to echo it.
async fn claim(stream: &mut TcpStream, line: &[u8]) -> io::Result<()> {
    stream.write_all(line).await?;
    let mut echo = [0; MAX_CLAIM_BYTES];
    stream.read_exact(&mut echo[..line.len()]).await?;
    if echo[..line.len()] != *line {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "the sidecar at the Pod address serves another instance",
        ));
    }
    Ok(())
}

/// Reads a connection's instance claim, if it opens with one, and echoes it
/// when it names `instance_id`. Returns `None` after the echo, or the first
/// byte of a connection that opens with anything else, which is plain HTTP;
/// [`SidecarIo::new`] then serves the connection. A claim for another
/// instance, or a malformed one, is an error, and the caller closes the
/// connection without serving a request.
pub async fn read_instance_claim<IO>(io: &mut IO, instance_id: &str) -> io::Result<Option<u8>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut line = [0; MAX_CLAIM_BYTES];
    if io.read(&mut line[..1]).await? == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    if line[0] != CLAIM_MARK {
        return Ok(Some(line[0]));
    }
    let mut length = 1;
    // The client sends nothing after its claim until the echo arrives.
    loop {
        if length == line.len() {
            return Err(invalid_claim("the instance claim is too long"));
        }
        let read = io.read(&mut line[length..]).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        length += read;
        if let Some(end) = line[..length].iter().position(|byte| *byte == b'\n') {
            if end + 1 != length {
                return Err(invalid_claim("bytes followed the instance claim"));
            }
            break;
        }
    }
    let expected = claim_line(instance_id);
    if line[..length] != *expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "the instance claim names another instance",
        ));
    }
    io.write_all(&expected).await?;
    io.flush().await?;
    Ok(None)
}

fn claim_line(instance_id: &str) -> Vec<u8> {
    let mut line = Vec::with_capacity(MAX_CLAIM_BYTES);
    line.extend_from_slice(CLAIM_PREFIX);
    line.extend_from_slice(instance_id.as_bytes());
    line.push(b'\n');
    line
}

fn invalid_claim(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// `host[:port]` of an authority.
pub(crate) fn authority_without_userinfo(authority: &Authority) -> &str {
    let value = authority.as_str();
    value.rsplit_once('@').map_or(value, |(_, host)| host)
}

/// A sidecar connection after [`read_instance_claim`]: a plain HTTP
/// connection yields its first byte again.
#[derive(Debug)]
pub struct SidecarIo<IO> {
    first: Option<u8>,
    io: IO,
}

impl<IO> SidecarIo<IO> {
    /// `io` after [`read_instance_claim`] returned `first`.
    pub fn new(io: IO, first: Option<u8>) -> Self {
        Self { first, io }
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for SidecarIo<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() > 0 {
            if let Some(first) = self.first.take() {
                buf.put_slice(&[first]);
                return Poll::Ready(Ok(()));
            }
        }
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for SidecarIo<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl fmt::Display for InvalidPodRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InstanceId(value) => write!(f, "instance ID {value:?} is not a DNS label"),
            Self::InvalidOrigin => f.write_str("the Service origin cannot carry a Pod route"),
        }
    }
}

impl std::error::Error for InvalidPodRoute {}

#[cfg(test)]
mod tests;
