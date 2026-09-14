use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Semaphore;
use tonic::{body::Body, codegen::http, Status};
use tower::{Layer, Service};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiLimits {
    pub rpc_concurrency: usize,
    pub accepted_connections: usize,
    pub setup_timeout: Duration,
    pub write_timeout: Duration,
    pub unary_delivery_timeout: Duration,
    pub subscription_streams: usize,
    pub subscriptions_per_stream: usize,
    pub subscription_lifetime: Duration,
    pub lookup_timeout: Duration,
    pub response_timeout: Duration,
    /// Positive route cache lifetime handed to proxies. Subscription
    /// invalidations are the primary freshness mechanism; this is the bound on
    /// how long a dropped event can go unnoticed. It outlives
    /// `subscription_lifetime`, because a proxy carries cached answers across an
    /// orderly stream rotation and registers them on the replacement stream.
    pub positive_route_cache_ttl: Duration,
}
impl Default for ApiLimits {
    fn default() -> Self {
        Self {
            rpc_concurrency: 128,
            accepted_connections: 256,
            setup_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            unary_delivery_timeout: Duration::from_secs(5),
            subscription_streams: 64,
            subscriptions_per_stream: 256,
            subscription_lifetime: Duration::from_secs(60),
            lookup_timeout: Duration::from_secs(3),
            response_timeout: Duration::from_secs(1),
            positive_route_cache_ttl: Duration::from_secs(60),
        }
    }
}

/// Acquire before tonic decodes/allocates the protobuf request body.
#[derive(Clone, Debug)]
pub struct RpcAdmissionLayer {
    semaphore: Arc<Semaphore>,
    certificate_semaphore: Arc<Semaphore>,
    certificate_watch_semaphore: Arc<Semaphore>,
    delivery_timeout: Duration,
}
impl RpcAdmissionLayer {
    pub fn new(limit: usize) -> Self {
        Self::with_delivery_timeout(limit, Duration::from_secs(5))
    }
    pub fn with_delivery_timeout(limit: usize, delivery_timeout: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            certificate_semaphore: Arc::new(Semaphore::new(8)),
            certificate_watch_semaphore: Arc::new(Semaphore::new(
                super::certificate_watch::WATCH_STREAMS,
            )),
            delivery_timeout,
        }
    }
}
#[derive(Clone)]
pub struct RpcAdmissionService<S> {
    inner: S,
    semaphore: Arc<Semaphore>,
    certificate_semaphore: Arc<Semaphore>,
    certificate_watch_semaphore: Arc<Semaphore>,
    delivery_timeout: Duration,
}
impl<S> Layer<S> for RpcAdmissionLayer {
    type Service = RpcAdmissionService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RpcAdmissionService {
            inner,
            semaphore: self.semaphore.clone(),
            certificate_semaphore: self.certificate_semaphore.clone(),
            certificate_watch_semaphore: self.certificate_watch_semaphore.clone(),
            delivery_timeout: self.delivery_timeout,
        }
    }
}
impl<S> Service<http::Request<Body>> for RpcAdmissionService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        if let Some(progress) = connection_progress(request.extensions()) {
            progress.request_started();
        }
        let certificate = certificate_method(request.uri().path());
        if certificate
            && request
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("application/grpc-web"))
        {
            return Box::pin(async {
                Ok(
                    Status::permission_denied("certificate operations require native TLS")
                        .into_http(),
                )
            });
        }
        let certificate_watch = request.uri().path()
            == "/sleepypods.controlplane.v1.ProxyControlPlane/WatchTlsCertificates";
        let capacity = if certificate_watch {
            &self.certificate_watch_semaphore
        } else if certificate {
            &self.certificate_semaphore
        } else {
            &self.semaphore
        };
        let Ok(permit) = capacity.clone().try_acquire_owned() else {
            let mut response =
                Status::resource_exhausted("control-plane RPC capacity exhausted").into_http();
            if request
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("application/grpc-web"))
            {
                let text = matches!(
                    request
                        .headers()
                        .get(http::header::ACCEPT)
                        .and_then(|value| value.to_str().ok()),
                    Some("application/grpc-web-text" | "application/grpc-web-text+proto")
                );
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static(if text {
                        "application/grpc-web-text+proto"
                    } else {
                        "application/grpc-web+proto"
                    }),
                );
            }
            return Box::pin(async move { Ok(response) });
        };
        let subscription = certificate_watch
            || request
                .uri()
                .path()
                .ends_with("/ProxyControlPlane/Subscribe");
        let connection = connection_progress(request.extensions()).cloned();
        let delivery_timeout = self.delivery_timeout;
        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response = if certificate {
                match tokio::time::timeout(Duration::from_secs(10), future).await {
                    Ok(result) => result?,
                    Err(_) => {
                        return Ok(
                            Status::deadline_exceeded("certificate RPC deadline exceeded")
                                .into_http(),
                        )
                    }
                }
            } else {
                future.await?
            };
            let (permit, delivery_timeout) = if subscription {
                drop(permit);
                let Some(lease) = response.extensions_mut().remove::<SubscriptionLease>() else {
                    return Ok(response);
                };
                (lease.permit, lease.lifetime)
            } else {
                (Arc::new(permit), delivery_timeout)
            };
            let state = Arc::new(UnaryDelivery {
                _permit: permit,
                connection,
                timer: std::sync::Mutex::new(None),
            });
            let weak = Arc::downgrade(&state);
            let queued_task_permit = state._permit.clone();
            *state.timer.lock().unwrap() = Some(tokio::spawn(async move {
                let _queued_task_permit = queued_task_permit;
                tokio::time::sleep(delivery_timeout).await;
                if let Some(state) = weak.upgrade() {
                    if let Some(connection) = &state.connection {
                        connection.cancel();
                    }
                }
            }));
            Ok(response.map(|inner| Body::new(UnaryResponseBody { inner, state })))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn completed_response_capacity_includes_owned_watchdog_teardown() {
        let inner = tower::service_fn(|_: http::Request<Body>| async {
            Ok::<_, std::convert::Infallible>(http::Response::new(Body::empty()))
        });
        let mut service = RpcAdmissionLayer::new(1).layer(inner);
        let response = service
            .call(http::Request::new(Body::empty()))
            .await
            .unwrap();
        assert_eq!(service.semaphore.available_permits(), 0);
        drop(response);
        assert_eq!(
            service.semaphore.available_permits(),
            0,
            "aborted watchdog still owns its slot until task teardown"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while service.semaphore.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rpc_capacity_precedes_handler_and_recovers_after_cancel() {
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = started.clone();
        let inner = tower::service_fn(move |_request: http::Request<Body>| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async {
                std::future::pending::<()>().await;
                Ok::<_, std::convert::Infallible>(http::Response::new(Body::empty()))
            }
        });
        let mut service = RpcAdmissionLayer::new(1).layer(inner);
        let first = service.call(http::Request::new(Body::empty()));
        let rejected = service
            .call(http::Request::new(Body::empty()))
            .await
            .unwrap();
        assert_eq!(rejected.headers()["grpc-status"], "8");
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 1);
        drop(first);
        let second = service.call(http::Request::new(Body::empty()));
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 2);
        drop(second);
        assert_eq!(service.semaphore.available_permits(), 1);
    }
}

// Tonic erases Buf progress, so this is a finite unary delivery deadline, not
// an inactivity timer. Public Hyper cannot reset the stalled stream after EOS
// was queued; cancelling its connection is the conservative bounded fallback.
struct UnaryDelivery {
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    connection: Option<crate::runtime_io::ConnectionProgress>,
    timer: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Drop for UnaryDelivery {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.get_mut().unwrap().take() {
            timer.abort();
        }
    }
}
struct OwnedResponseBytes {
    bytes: bytes::Bytes,
    _delivery: Arc<UnaryDelivery>,
}
impl AsRef<[u8]> for OwnedResponseBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct UnaryResponseBody {
    inner: Body,
    state: Arc<UnaryDelivery>,
}
impl http_body::Body for UnaryResponseBody {
    type Data = bytes::Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx).map(|frame| {
            frame.map(|result| {
                result.map(|frame| {
                    frame.map_data(|bytes| {
                        bytes::Bytes::from_owner(OwnedResponseBytes {
                            bytes,
                            _delivery: self.state.clone(),
                        })
                    })
                })
            })
        })
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Clone)]
pub(crate) struct SubscriptionLease {
    pub permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    pub lifetime: Duration,
}

fn connection_progress(
    extensions: &http::Extensions,
) -> Option<&crate::runtime_io::ConnectionProgress> {
    extensions.get::<crate::runtime_io::ConnectionProgress>().or_else(||
        extensions.get::<tonic::transport::server::TlsConnectInfo<crate::runtime_io::ConnectionProgress>>()
            .map(|info|info.get_ref()))
}
fn certificate_method(path: &str) -> bool {
    matches!(
        path,
        "/sleepypods.controlplane.v1.OperatorControlPlane/PublishCertificate"
            | "/sleepypods.controlplane.v1.OperatorControlPlane/GetCertificateMetadata"
            | "/sleepypods.controlplane.v1.OperatorControlPlane/SetTlsBinding"
            | "/sleepypods.controlplane.v1.OperatorControlPlane/GetTlsBinding"
            | "/sleepypods.controlplane.v1.OperatorControlPlane/RemoveCertificate"
            | "/sleepypods.controlplane.v1.OperatorControlPlane/ReencryptCertificate"
            | "/sleepypods.controlplane.v1.ProxyControlPlane/ResolveTlsCertificate"
            | "/sleepypods.controlplane.v1.ProxyControlPlane/WatchTlsCertificates"
    )
}

#[cfg(test)]
mod certificate_tests {
    use super::*;
    #[tokio::test]
    async fn certificate_decode_flood_does_not_consume_ordinary_admission() {
        let inner = tower::service_fn(|_: http::Request<Body>| async {
            std::future::pending::<()>().await;
            Ok::<_, std::convert::Infallible>(http::Response::new(Body::empty()))
        });
        let mut service = RpcAdmissionLayer::new(1).layer(inner);
        let request = || {
            http::Request::builder()
                .uri("/sleepypods.controlplane.v1.ProxyControlPlane/ResolveTlsCertificate")
                .body(Body::empty())
                .unwrap()
        };
        let mut pending = Vec::new();
        for _ in 0..8 {
            pending.push(service.call(request()));
        }
        assert_eq!(service.certificate_semaphore.available_permits(), 0);
        assert_eq!(
            service.call(request()).await.unwrap().headers()["grpc-status"],
            "8"
        );
        assert_eq!(service.semaphore.available_permits(), 1);
        let ordinary = service.call(http::Request::new(Body::empty()));
        assert_eq!(service.semaphore.available_permits(), 0);
        drop(pending);
        drop(ordinary);
        assert_eq!(service.certificate_semaphore.available_permits(), 8);
        assert_eq!(service.semaphore.available_permits(), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn slow_certificate_body_owns_one_slot_until_fixed_deadline() {
        let inner = tower::service_fn(|_: http::Request<Body>| async {
            std::future::pending::<()>().await;
            Ok::<_, std::convert::Infallible>(http::Response::new(Body::empty()))
        });
        let mut service = RpcAdmissionLayer::new(1).layer(inner);
        let response = service.call(
            http::Request::builder()
                .uri("/sleepypods.controlplane.v1.OperatorControlPlane/PublishCertificate")
                .body(Body::empty())
                .unwrap(),
        );
        assert_eq!(service.certificate_semaphore.available_permits(), 7);
        let started = tokio::time::Instant::now();
        assert_eq!(response.await.unwrap().headers()["grpc-status"], "4");
        assert!(started.elapsed() >= Duration::from_secs(10));
        assert_eq!(service.certificate_semaphore.available_permits(), 8);
    }
}
