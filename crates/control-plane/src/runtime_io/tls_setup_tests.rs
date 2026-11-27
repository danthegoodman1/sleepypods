//! The controlled duplex proves Tonic/runtime-config handshake ownership. The
//! actual TCP case separately proves composition with BoundedIncoming capacity.
use super::*;
use futures_util::{FutureExt, StreamExt};
use std::{fs, path::PathBuf, sync::atomic::AtomicUsize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

struct IdentityFiles {
    directory: PathBuf,
    security: crate::runtime_security::RuntimeSecurityConfig,
    certificate: rcgen::Certificate,
}
impl IdentityFiles {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let directory = std::env::temp_dir().join(format!(
            "sleepypods-server-tls-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let mut fixture = Self {
            directory,
            security: Default::default(),
            certificate: cert,
        };
        for (name, bytes) in [
            ("certificate.pem", fixture.certificate.pem()),
            ("key.pem", signing_key.serialize_pem()),
        ] {
            let path = fixture.directory.join(name);
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            use std::io::Write;
            options
                .open(&path)
                .unwrap()
                .write_all(bytes.as_bytes())
                .unwrap();
            if name == "key.pem" {
                fixture.security.private_key_file = Some(path);
            } else {
                fixture.security.certificate_file = Some(path);
            }
        }
        fixture
    }
    fn client(&self) -> rustls::ClientConfig {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.certificate.der().clone()).unwrap();
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![b"h2".to_vec()];
        config
    }
}
impl Drop for IdentityFiles {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.directory.join("key.pem"));
        let _ = fs::remove_file(self.directory.join("certificate.pem"));
        let _ = fs::remove_dir(&self.directory);
    }
}

struct ControlledIo {
    stream: DuplexStream,
    _permit: OwnedSemaphorePermit,
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    blocked: Arc<AtomicBool>,
}
impl Connected for ControlledIo {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}
impl AsyncRead for ControlledIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for ControlledIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.stream).poll_write(cx, buf);
        match result {
            Poll::Pending => {
                self.blocked.store(true, Ordering::SeqCst);
            }
            Poll::Ready(Ok(n)) => {
                self.writes.fetch_add(n, Ordering::SeqCst);
            }
            _ => {}
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
// Tokio's paused clock must not auto-advance or make a failed barrier hang.
async fn until(mut condition: impl FnMut() -> bool, stage: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(
            std::time::Instant::now() < deadline,
            "causal barrier did not arrive: {stage}"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn server_tls_total_setup_expires_during_trickled_write_flight() {
    let identity = IdentityFiles::new();
    let capacity = Arc::new(Semaphore::new(1));
    let reads = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let blocked = Arc::new(AtomicBool::new(false));
    let (stream, mut peer) = tokio::io::duplex(64);
    let incoming = ControlledIo {
        stream,
        _permit: capacity.clone().acquire_owned().await.unwrap(),
        reads: reads.clone(),
        writes: writes.clone(),
        blocked: blocked.clone(),
    };
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let mut jobs = tokio::task::JoinSet::new();
    let setup = Duration::from_millis(100);
    let router = tonic::transport::Server::builder()
        .tls_config(identity.security.tls(setup).unwrap().unwrap())
        .unwrap()
        .add_service(crate::runtime::tests::noop_operator_service());
    jobs.spawn(
        router.serve_with_incoming_shutdown(
            tonic::codegen::tokio_stream::iter([Ok::<_, io::Error>(incoming)])
                .chain(tonic::codegen::tokio_stream::pending()),
            async {
                let _ = stopped.await;
            },
        ),
    );
    let result = std::panic::AssertUnwindSafe(async {
        let start = Instant::now();
        let mut client = rustls::ClientConnection::new(
            Arc::new(identity.client()),
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut hello = Vec::new();
        client.write_tls(&mut hello).unwrap();
        peer.write_all(&hello).await.unwrap();
        until(|| blocked.load(Ordering::SeqCst), "server flight blocked").await;
        assert_eq!(
            Instant::now(),
            start,
            "setup barrier precedes clock movement"
        );
        let read_polls = reads.load(Ordering::SeqCst);
        for _ in 0..9 {
            tokio::time::advance(Duration::from_millis(10)).await;
            let before = writes.load(Ordering::SeqCst);
            peer.read_exact(&mut [0; 1]).await.unwrap();
            until(
                || writes.load(Ordering::SeqCst) > before,
                "server flight progressed",
            )
            .await;
            assert_eq!(
                reads.load(Ordering::SeqCst),
                read_polls,
                "server flight does not poll the read-side setup timer"
            );
            assert_eq!(capacity.available_permits(), 0);
        }
        assert_eq!(Instant::now() - start, Duration::from_millis(90));
        tokio::time::advance(Duration::from_millis(11)).await;
        until(
            || capacity.available_permits() == 1,
            "setup expiry released transport capacity",
        )
        .await;
        assert_eq!(reads.load(Ordering::SeqCst), read_polls);
        // The peer never completed TLS or closed its socket to release capacity.
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining).await.unwrap();
        assert!(remaining.len() <= 64);
    })
    .catch_unwind()
    .await;
    let _ = shutdown.send(());
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn actual_native_tls_silent_peer_releases_capacity_and_healthy_rpc_recovers() {
    let identity = IdentityFiles::new();
    let capacity = Arc::new(Semaphore::new(1));
    let setup = Duration::from_millis(100);
    let incoming = BoundedIncoming::bind(
        "127.0.0.1:0".parse().unwrap(),
        capacity.clone(),
        setup,
        Duration::from_millis(50),
    )
    .await
    .unwrap();
    let address = incoming.local_addr().unwrap();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let mut jobs = tokio::task::JoinSet::new();
    let router = tonic::transport::Server::builder()
        .tls_config(identity.security.tls(setup).unwrap().unwrap())
        .unwrap()
        .add_service(crate::runtime::tests::noop_operator_service());
    jobs.spawn(router.serve_with_incoming_shutdown(incoming, async {
        let _ = stopped.await;
    }));
    let result = std::panic::AssertUnwindSafe(async {
        let mut silent = TcpStream::connect(address).await.unwrap();
        until(|| capacity.available_permits() == 0, "silent peer admitted").await;
        let result = tokio::time::timeout(Duration::from_secs(2), silent.read(&mut [0]))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0)));
        until(
            || capacity.available_permits() == 1,
            "setup expiry released transport capacity",
        )
        .await;
        let channel = sleepypods_api::transport::native_endpoint(
            format!("https://localhost:{}", address.port()),
            Some(&identity.certificate.pem()),
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
        let mut client =
            crate::api::pb::operator_control_plane_client::OperatorControlPlaneClient::new(channel);
        let status = tokio::time::timeout(
            Duration::from_secs(2),
            client.get_instance(crate::api::pb::GetInstanceRequest {
                instance_id: "recovered".into(),
            }),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
        drop(client);
    })
    .catch_unwind()
    .await;
    let _ = shutdown.send(());
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
