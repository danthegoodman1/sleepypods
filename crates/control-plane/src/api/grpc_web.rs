//! Browser access to the operator service over gRPC-Web: which origins may
//! call it, and the CORS answers a browser needs before it will.
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tonic::{
    body::Body,
    codegen::http::{self, header, HeaderName, HeaderValue, Method, StatusCode},
};
use tower::{Layer, Service};
use tower_http::cors::{AllowOrigin, CorsLayer};

/// Parses one configured origin: `scheme://host[:port]` in lowercase, the form
/// a browser sends in its `Origin` header.
pub fn parse_allowed_origin(value: &str) -> Option<HeaderValue> {
    let uri = value.parse::<http::Uri>().ok()?;
    let scheme = uri
        .scheme_str()
        .filter(|s| matches!(*s, "http" | "https"))?;
    let authority = uri.authority()?.as_str();
    (!authority.contains('@')
        && value == value.to_ascii_lowercase()
        && value == format!("{scheme}://{authority}"))
    .then(|| HeaderValue::from_str(value).ok())
    .flatten()
}

/// Answers preflights and labels responses for the allowed origins.
pub fn operator_grpc_web_cors_layer(allowed_origins: &[HeaderValue]) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(allowed_origins.iter().cloned()))
        .allow_methods([Method::POST, Method::OPTIONS])
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            HeaderName::from_static("grpc-timeout"),
            HeaderName::from_static("x-grpc-web"),
            HeaderName::from_static("x-sleepypods-operator"),
            HeaderName::from_static("x-user-agent"),
        ])
        .expose_headers([
            HeaderName::from_static("grpc-status"),
            HeaderName::from_static("grpc-message"),
            HeaderName::from_static("grpc-status-details-bin"),
        ])
}

/// Refuses a request whose `Origin` is outside the allowed list. A browser
/// names the page's origin on every gRPC-Web call, so a page from another
/// origin gets 403 before the operator service sees its request. Clients that
/// send no `Origin` are not browsers and pass through to authentication.
#[derive(Clone, Debug)]
pub struct OriginGuardLayer {
    allowed_origins: Arc<[HeaderValue]>,
}

impl OriginGuardLayer {
    pub fn new(allowed_origins: &[HeaderValue]) -> Self {
        Self {
            allowed_origins: allowed_origins.into(),
        }
    }
}

impl<S> Layer<S> for OriginGuardLayer {
    type Service = OriginGuard<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OriginGuard {
            inner,
            allowed_origins: self.allowed_origins.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OriginGuard<S> {
    inner: S,
    allowed_origins: Arc<[HeaderValue]>,
}

impl<S> Service<http::Request<Body>> for OriginGuard<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let refused = request
            .headers()
            .get(header::ORIGIN)
            .is_some_and(|origin| !self.allowed_origins.contains(origin));
        if refused {
            return Box::pin(async {
                let mut response = http::Response::new(Body::empty());
                *response.status_mut() = StatusCode::FORBIDDEN;
                Ok(response)
            });
        }
        Box::pin(self.inner.call(request))
    }
}

#[cfg(test)]
mod tests {
    use super::parse_allowed_origin;

    #[test]
    fn allowed_origins_take_the_form_a_browser_sends() {
        for origin in [
            "https://console.example",
            "https://console.example:8443",
            "http://localhost:3000",
        ] {
            assert_eq!(parse_allowed_origin(origin).expect("valid origin"), origin);
        }
        for origin in [
            "",
            "*",
            "console.example",
            "https://console.example/",
            "https://console.example/app",
            "https://Console.Example",
            "https://user@console.example",
            "ftp://console.example",
            "null",
        ] {
            assert!(parse_allowed_origin(origin).is_none(), "{origin:?}");
        }
    }
}
