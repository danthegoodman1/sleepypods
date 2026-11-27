//! Host-local actual-listener comparison. Also builds unchanged against dc7cac2.
use bytes::Bytes;
use http::{Request, Response, Version};
use http_body_util::{BodyExt, Full};
use hyper::{
    server::conn::{http1, http2},
    service::service_fn,
};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use proxy_core::{
    observability::{
        prometheus::PrometheusMetricsSink,
        recorder::{CompositeObservabilitySink, ObservabilityRecorder, StderrObservabilitySink},
    },
    Shutdown,
};
use sidecar::{
    runtime::{serve_http_listener_with_idle, SidecarRuntimeConfig},
    IdleReportConfig, ReportIdleClient, ReportIdleFuture, ReportIdleRequest, ReportIdleResponse,
};
use sleepypods_api::{Generation, InstanceId};
use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, task::JoinSet};

struct NoIdle;
impl ReportIdleClient for NoIdle {
    type Error = Infallible;
    fn report_idle(
        &mut self,
        _: ReportIdleRequest,
    ) -> ReportIdleFuture<'_, ReportIdleResponse, Self::Error> {
        Box::pin(std::future::pending())
    }
}

#[tokio::main]
async fn main() {
    let _ = ObservabilityRecorder::install_global(Arc::new(CompositeObservabilitySink::new(vec![
        Arc::new(StderrObservabilitySink),
        Arc::new(PrometheusMetricsSink::new()),
    ])));
    let requests: usize = std::env::var("BENCH_REQUESTS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(10_000);
    let concurrency: usize = std::env::var("BENCH_CONCURRENCY")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(16);
    for h2 in [false, true] {
        let app = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let app_addr = app.local_addr().unwrap();
        let backend = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                let (stream, _) = app.accept().await.unwrap();
                let _ = stream.set_nodelay(true);
                while tasks.try_join_next().is_some() {}
                tasks.spawn(async move {
                    let service = service_fn(|_| async {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                            b"actual production listener response",
                        ))))
                    });
                    if h2 {
                        let _ = http2::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    } else {
                        let _ = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    }
                });
            }
        });
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = SidecarRuntimeConfig::new(
            addr,
            app_addr.port(),
            InstanceId::new("bench").unwrap(),
            Generation::new(1),
            IdleReportConfig::new(
                Duration::from_secs(3600),
                Duration::from_secs(1),
                Duration::from_secs(10),
            )
            .unwrap(),
            Duration::from_secs(1),
        )
        .unwrap();
        let shutdown = Shutdown::new();
        let sidecar = tokio::spawn(serve_http_listener_with_idle(
            listener,
            config,
            NoIdle,
            shutdown.clone(),
        ));
        let mut results = Vec::new();
        for (path, target) in [("direct", app_addr), ("sidecar", addr)] {
            let client = Client::builder(TokioExecutor::new())
                .http2_only(h2)
                .build_http::<Full<Bytes>>();
            let uri = format!("http://{target}/bench")
                .parse::<http::Uri>()
                .unwrap();
            let version = if h2 {
                Version::HTTP_2
            } else {
                Version::HTTP_11
            };
            for _ in 0..100 {
                client
                    .request(
                        Request::builder()
                            .uri(uri.clone())
                            .version(version)
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .into_body()
                    .collect()
                    .await
                    .unwrap();
            }
            let start = Instant::now();
            let mut workers = JoinSet::new();
            for worker in 0..concurrency {
                let client = client.clone();
                let uri = uri.clone();
                workers.spawn(async move {
                    let mut samples = Vec::new();
                    for _ in (worker..requests).step_by(concurrency) {
                        let start = Instant::now();
                        let response = client
                            .request(
                                Request::builder()
                                    .uri(uri.clone())
                                    .version(version)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(response.status(), 200);
                        assert_eq!(
                            response.into_body().collect().await.unwrap().to_bytes(),
                            "actual production listener response"
                        );
                        samples.push(start.elapsed().as_nanos() as u64);
                    }
                    samples
                });
            }
            let mut samples = Vec::with_capacity(requests);
            while let Some(result) = workers.join_next().await {
                samples.extend(result.unwrap());
            }
            let elapsed = start.elapsed().as_secs_f64();
            samples.sort_unstable();
            let rps = requests as f64 / elapsed;
            let p99 = samples[samples.len() * 99 / 100] as f64 / 1e6;
            println!("protocol={} path={} requests={} concurrency={} rps={:.3} p50_ms={:.4} p99_ms={:.4} elapsed_s={:.4}", if h2 { "h2" } else { "h1" }, path, requests, concurrency, rps, samples[samples.len()/2] as f64 / 1e6, p99, elapsed);
            results.push((rps, p99));
        }
        println!(
            "protocol={} sidecar_direct_ratio={:.4} added_p99_ms={:.4}",
            if h2 { "h2" } else { "h1" },
            results[1].0 / results[0].0,
            results[1].1 - results[0].1
        );
        shutdown.shutdown();
        sidecar.await.unwrap().unwrap();
        backend.abort();
        let _ = backend.await;
    }
}
