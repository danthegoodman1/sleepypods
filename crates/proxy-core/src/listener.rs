//! The accept loop and socket options shared by every proxy listener.
use std::{
    future::Future,
    io::{self, Read},
    net::SocketAddr,
    time::Duration,
};

use socket2::{SockRef, TcpKeepalive};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};

use crate::Shutdown;

/// A peer that stops answering is detected after about 90 seconds.
pub(crate) const KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
pub(crate) const KEEPALIVE_RETRIES: u32 = 3;
/// Pause after an accept failure that needs capacity to return, such as an
/// exhausted file-descriptor table.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);
/// The input a refused socket discards before it closes: enough for a
/// ClientHello or the largest request head the proxy reads. Input beyond it
/// resets the connection after the FIN.
const REFUSED_INPUT_LIMIT: usize = 64 * 1024;

/// A source of accepted connections. Production listeners are `TcpListener`s.
pub trait Accept {
    fn accept(&mut self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send;
}

impl Accept for TcpListener {
    async fn accept(&mut self) -> io::Result<(TcpStream, SocketAddr)> {
        TcpListener::accept(self).await
    }
}

/// Accepts connections until shutdown and returns the tasks still running.
///
/// `connection` admits each socket and returns the task that serves it, or
/// hands the socket back to refuse it; the loop closes a refused socket so
/// that the client reads end of stream. A failed accept never ends the loop:
/// an error that belongs to one connection is skipped, and any other error
/// waits briefly for capacity. The listening socket closes before this
/// returns, so the kernel refuses new connections while the caller drains.
pub async fn serve_listener<L, F, Fut>(
    mut listener: L,
    shutdown: &Shutdown,
    mut connection: F,
) -> JoinSet<()>
where
    L: Accept,
    F: FnMut(TcpStream, SocketAddr) -> Result<Fut, TcpStream>,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut connections = JoinSet::new();
    loop {
        while connections.try_join_next().is_some() {}
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((stream, peer)) => {
                let _ = configure_socket(&stream);
                match connection(stream, peer) {
                    Ok(task) => {
                        connections.spawn(task);
                    }
                    Err(stream) => close_refused(stream),
                }
            }
            Err(error) if belongs_to_one_connection(&error) => {}
            Err(_) => {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(ACCEPT_BACKOFF) => {}
                }
            }
        }
    }
    drop(listener);
    connections
}

/// Disables Nagle and enables TCP keepalive on an accepted or dialled socket.
pub fn configure_socket(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let keepalive = TcpKeepalive::new()
        .with_time(KEEPALIVE_IDLE)
        .with_interval(KEEPALIVE_INTERVAL)
        .with_retries(KEEPALIVE_RETRIES);
    SockRef::from(stream).set_tcp_keepalive(&keepalive)
}

/// Closes a refused socket so that the client reads end of stream.
///
/// Linux resets a connection that closes with unread input, and a client's
/// first bytes, such as a TLS ClientHello, often arrive before the loop
/// refuses its socket. A relay such as `kubectl port-forward` fails on that
/// reset. The socket therefore sends FIN first, so the client reads end of
/// stream even if more input arrives later, and then discards the input
/// already buffered, so the close sends nothing more. Neither step waits.
fn close_refused(stream: TcpStream) {
    let socket = SockRef::from(&stream);
    let _ = socket.shutdown(std::net::Shutdown::Write);
    let mut buffer = [0; 4096];
    let mut remaining = REFUSED_INPUT_LIMIT;
    while remaining > 0 {
        let limit = remaining.min(buffer.len());
        match (&*socket).read(&mut buffer[..limit]) {
            Ok(0) | Err(_) => break,
            Ok(read) => remaining -= read,
        }
    }
}

/// Linux reports a network error already pending on the new socket through
/// `accept`. The connection is gone; the listener is healthy.
fn belongs_to_one_connection(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::Interrupted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// Fails `accept` with the queued errors, then accepts real connections.
    struct FailingAccept {
        errors: VecDeque<io::Error>,
        inner: TcpListener,
    }

    impl Accept for FailingAccept {
        async fn accept(&mut self) -> io::Result<(TcpStream, SocketAddr)> {
            match self.errors.pop_front() {
                Some(error) => Err(error),
                None => self.inner.accept().await,
            }
        }
    }

    async fn echo_one_byte(mut stream: TcpStream) {
        let byte = stream.read_u8().await.expect("served connection reads");
        stream
            .write_u8(byte)
            .await
            .expect("served connection writes");
    }

    /// Echoes every byte until the client closes its side.
    async fn echo_until_closed(mut stream: TcpStream) {
        while let Ok(byte) = stream.read_u8().await {
            if stream.write_u8(byte).await.is_err() {
                return;
            }
        }
    }

    #[tokio::test]
    async fn accept_errors_leave_the_listener_serving() {
        let inner = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = inner.local_addr().unwrap();
        let listener = FailingAccept {
            errors: [libc::EMFILE, libc::ECONNABORTED, libc::ENFILE, libc::ENOMEM]
                .into_iter()
                .map(io::Error::from_raw_os_error)
                .collect(),
            inner,
        };
        let shutdown = Shutdown::new();
        let server = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                serve_listener(listener, &shutdown, |stream, _| Ok(echo_one_byte(stream))).await
            }
        });

        for byte in [7_u8, 9] {
            let mut client = TcpStream::connect(addr).await.unwrap();
            client.write_u8(byte).await.unwrap();
            assert_eq!(client.read_u8().await.unwrap(), byte);
        }

        shutdown.shutdown();
        let mut connections = server.await.unwrap();
        while connections.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn shutdown_closes_the_listener_while_connections_drain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Shutdown::new();
        let server = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                serve_listener(listener, &shutdown, |stream, _| {
                    Ok(echo_until_closed(stream))
                })
                .await
            }
        });
        let mut draining = TcpStream::connect(addr).await.unwrap();
        // The server accepted this connection once its echo arrives.
        draining.write_u8(1).await.unwrap();
        assert_eq!(draining.read_u8().await.unwrap(), 1);

        shutdown.shutdown();
        let mut connections = server.await.unwrap();

        let refused = TcpStream::connect(addr).await.unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);
        // The accepted connection keeps being served while it drains.
        draining.write_u8(2).await.unwrap();
        assert_eq!(draining.read_u8().await.unwrap(), 2);
        drop(draining);
        let task = connections
            .join_next()
            .await
            .expect("draining connection task");
        task.unwrap();
        assert!(connections.join_next().await.is_none());
    }

    #[tokio::test]
    async fn accepted_sockets_disable_nagle_and_enable_keepalive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Shutdown::new();
        let (options, observed) = tokio::sync::oneshot::channel();
        let mut options = Some(options);
        let server = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                serve_listener(listener, &shutdown, move |stream, _| {
                    let socket = SockRef::from(&stream);
                    let observed = (
                        stream.nodelay().unwrap(),
                        socket.keepalive().unwrap(),
                        socket.tcp_keepalive_time().unwrap(),
                    );
                    if let Some(options) = options.take() {
                        let _ = options.send(observed);
                    }
                    Err::<std::future::Ready<()>, _>(stream)
                })
                .await
            }
        });

        let _client = TcpStream::connect(addr).await.unwrap();
        assert_eq!(observed.await.unwrap(), (true, true, KEEPALIVE_IDLE));
        shutdown.shutdown();
        server.await.unwrap();
    }

    /// Hands over each connection once the client's first bytes have arrived,
    /// as a busy accept loop does.
    struct AcceptAfterInput(TcpListener);

    impl Accept for AcceptAfterInput {
        async fn accept(&mut self) -> io::Result<(TcpStream, SocketAddr)> {
            let (stream, peer) = self.0.accept().await?;
            stream.readable().await?;
            Ok((stream, peer))
        }
    }

    #[tokio::test]
    async fn refused_sockets_with_unread_input_close_with_fin() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Shutdown::new();
        let server = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                serve_listener(AcceptAfterInput(listener), &shutdown, |stream, _| {
                    Err::<std::future::Ready<()>, _>(stream)
                })
                .await
            }
        });

        for _ in 0..8 {
            let mut client = TcpStream::connect(addr).await.unwrap();
            client.write_all(&[0x16, 0x03, 0x01, 0, 42]).await.unwrap();
            let read = tokio::time::timeout(Duration::from_secs(2), client.read(&mut [0; 1]))
                .await
                .unwrap();
            assert_eq!(read.unwrap(), 0, "a refused socket ends with FIN");
        }
        shutdown.shutdown();
        server.await.unwrap();
    }
}
