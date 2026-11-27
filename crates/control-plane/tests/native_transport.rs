//! Retained native Channel recovery: caller cancellation must not leave an
//! unbounded connection attempt blocking every subsequent request.
use control_plane::api::pb::{
    sidecar_control_plane_client::SidecarControlPlaneClient,
    sidecar_control_plane_server::{SidecarControlPlane, SidecarControlPlaneServer},
    SidecarReportIdleRequest, SidecarReportIdleResponse,
};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::{
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinSet,
    time::timeout,
};
use tonic::{
    transport::{Identity, Server, ServerTlsConfig},
    Code, Request, Response, Status,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Answers every call with one definite status, so a completed RPC proves the
/// transport reached the server.
struct RefusingSidecarService;

#[tonic::async_trait]
impl SidecarControlPlane for RefusingSidecarService {
    async fn report_idle(
        &self,
        _request: Request<SidecarReportIdleRequest>,
    ) -> Result<Response<SidecarReportIdleResponse>, Status> {
        Err(Status::unimplemented("transport fixture"))
    }
}

#[tokio::test]
async fn retained_native_channel_recovers_after_canceled_tls_reconnect() -> TestResult<()> {
    control_plane::install_rustls_crypto_provider();
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let backend_addr = backend.local_addr()?;
    let relay = TcpListener::bind("127.0.0.1:0").await?;
    let relay_addr = relay.local_addr()?;
    let (close_warm, warm_closed) = oneshot::channel();
    let (held_sender, held) = oneshot::channel();
    let (closed_sender, closed) = oneshot::channel();
    let accepts = Arc::new(AtomicUsize::new(0));
    let mut tasks: JoinSet<TestResult<()>> = JoinSet::new();
    let identity = Identity::from_pem(cert.pem(), signing_key.serialize_pem());
    tasks.spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .add_service(SidecarControlPlaneServer::new(RefusingSidecarService))
            .serve_with_incoming(
                tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(backend),
            )
            .await?;
        Ok(())
    });
    let accepted = accepts.clone();
    tasks.spawn(async move {
        let mut connections: FuturesUnordered<BoxFuture<'static,TestResult<()>>> = FuturesUnordered::new();
        let mut warm_closed = Some(warm_closed);
        let mut held_sender = Some(held_sender);
        let mut closed_sender = Some(closed_sender);
        // This fixture owns at most four accepted sockets and their upstreams.
        let mut index=0;
        loop {
            let (mut socket, _) = tokio::select! {
                value=relay.accept(), if index<4 => value?,
                result=connections.next(), if !connections.is_empty() => {result.ok_or("relay work missing")??;continue;},
                else=>break,
            };
            let current=index;index+=1;
            accepted.fetch_add(1, Ordering::SeqCst);
            if current == 1 {
                let held_sender = held_sender.take().ok_or("held sender missing")?;
                let closed_sender = closed_sender.take().ok_or("closed sender missing")?;
                connections.push(Box::pin(async move {
                    let mut header = [0;5];
                    socket.read_exact(&mut header).await?;
                    if header[0] != 22 { return Err("expected a real TLS ClientHello".into()); }
                    let _ = held_sender.send(());
                    // Never release this handshake to the healthy backend. Only
                    // the client attempt's own deadline can close this socket.
                    let result = tokio::io::copy(&mut socket, &mut tokio::io::sink()).await;
                    let _ = closed_sender.send(result.is_ok());
                    Ok(())
                }));
            } else {
                let stop = if current == 0 { warm_closed.take() } else { None };
                connections.push(Box::pin(async move {
                    let mut upstream = TcpStream::connect(backend_addr).await?;
                    if let Some(stop) = stop {
                        tokio::select! { _ = stop => {}, result = tokio::io::copy_bidirectional(&mut socket, &mut upstream) => { result?; } }
                    } else {
                        tokio::io::copy_bidirectional(&mut socket, &mut upstream).await?;
                    }
                    Ok(())
                }));
            }
        }
        while let Some(result) = connections.next().await { result?; }
        Ok(())
    });
    let result = timeout(Duration::from_secs(10), async {
        let channel = sleepypods_api::transport::native_endpoint(
            format!("https://localhost:{}", relay_addr.port()),
            Some(&cert.pem()),
        )?
        .connect()
        .await?;
        let mut client = SidecarControlPlaneClient::new(channel);
        let request = || SidecarReportIdleRequest {
            instance_id: "reconnect-proof".into(),
            ..Default::default()
        };
        let warm = match client.report_idle(request()).await {
            Err(status) => status,
            Ok(_) => return Err("refusing fixture unexpectedly succeeded".into()),
        };
        if warm.code() != Code::Unimplemented {
            return Err("initial verified native call failed".into());
        }
        close_warm
            .send(())
            .map_err(|_| "warm relay already closed")?;
        // The old physical connection can first report its closure. The next
        // safe read drives this exact same Channel into its held TLS attempt.
        let mut canceled = Box::pin(async {
            loop {
                let _ = client.report_idle(request()).await;
                tokio::task::yield_now().await;
            }
        });
        tokio::select! {
            result = held => { result.map_err(|_| "held handshake did not start")?; }
            _ = &mut canceled => return Err("request driver returned".into()),
        }
        drop(canceled);
        // Cancel one concrete pending RPC, not the driver loop above: an
        // immediately returned status must fail this observation.
        if timeout(Duration::from_millis(50), client.report_idle(request()))
            .await
            .is_ok()
        {
            return Err("held TLS RPC unexpectedly completed".into());
        }
        // The listener accepts healthy new connections now; the old held
        // connection remains untouched. No endpoint or Channel is recreated.
        timeout(Duration::from_secs(4), async {
            loop {
                match client.report_idle(request()).await {
                    Err(status) if status.code() == Code::Unimplemented => {
                        return Ok::<_, Box<dyn Error + Send + Sync>>(())
                    }
                    _ => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .map_err(|_| "same Channel did not recover after a canceled TLS reconnect")??;
        if accepts.load(Ordering::SeqCst) < 3 {
            return Err("recovery did not establish a new physical connection".into());
        }
        if !timeout(Duration::from_secs(1), closed).await?? {
            return Err("held connection did not reach EOF".into());
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result?
}
