use std::{convert::Infallible, error::Error, fmt, future::Future, pin::Pin};

use bytes::Bytes;
use http::{
    header::{CONTENT_TYPE, HOST},
    Request, Response, StatusCode,
};
use sleepypods_api::{
    http01::InvalidHttp01Challenge, Http01ChallengeKey, Http01ChallengeRecord, RouteIdentity,
};

use crate::{RequestIdentityError, RouteRequestIdentity};

pub const HTTP01_CHALLENGE_PREFIX: &str = "/.well-known/acme-challenge/";
pub const HTTP01_CONTENT_TYPE: &str = "text/plain";

pub type Http01ChallengeResolveFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Option<Http01ChallengeRecord>, E>> + Send + 'a>>;

pub trait Http01ChallengeResolver {
    type Error;

    fn resolve_http01_challenge(
        &mut self,
        key: Http01ChallengeKey,
    ) -> Http01ChallengeResolveFuture<'_, Self::Error>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoopHttp01ChallengeResolver;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Http01InterceptDecision {
    PassThrough,
    Miss {
        key: Http01ChallengeKey,
    },
    Serve {
        key: Http01ChallengeKey,
        response: Http01ChallengeResponse,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Http01ChallengeResponse {
    key_authorization: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Http01InterceptError<E> {
    MissingHost,
    InvalidHostHeader,
    InvalidTokenSegment,
    InvalidHost(RequestIdentityError),
    InvalidChallenge(InvalidHttp01Challenge),
    Resolve(E),
}

pub async fn intercept_http01_challenge<B, F, Fut, E>(
    request: &Request<B>,
    resolver: F,
) -> Result<Http01InterceptDecision, Http01InterceptError<E>>
where
    F: FnOnce(Http01ChallengeKey) -> Fut,
    Fut: Future<Output = Result<Option<Http01ChallengeRecord>, E>>,
{
    let path = request.uri().path();
    let Some(token) = path.strip_prefix(HTTP01_CHALLENGE_PREFIX) else {
        return Ok(Http01InterceptDecision::PassThrough);
    };
    if token.contains('/') {
        return Err(Http01InterceptError::InvalidTokenSegment);
    }

    let key = http01_challenge_key_for_resolver(request_host(request)?, token)?;
    match resolver(key.clone())
        .await
        .map_err(Http01InterceptError::Resolve)?
    {
        Some(record) => Ok(Http01InterceptDecision::Serve {
            key,
            response: Http01ChallengeResponse::new(record.key_authorization()),
        }),
        None => Ok(Http01InterceptDecision::Miss { key }),
    }
}

#[cfg(test)]
pub fn http01_challenge_token(path: &str) -> Option<&str> {
    let token = path.strip_prefix(HTTP01_CHALLENGE_PREFIX)?;
    if token.is_empty() || token.contains('/') {
        return None;
    }

    Some(token)
}

pub fn is_http01_challenge_candidate_path(path: &str) -> bool {
    path.starts_with(HTTP01_CHALLENGE_PREFIX)
}

impl Http01ChallengeResolver for NoopHttp01ChallengeResolver {
    type Error = Infallible;

    fn resolve_http01_challenge(
        &mut self,
        _key: Http01ChallengeKey,
    ) -> Http01ChallengeResolveFuture<'_, Self::Error> {
        Box::pin(async { Ok(None) })
    }
}

#[cfg(test)]
pub fn http01_challenge_key(
    host: &str,
    token: &str,
) -> Result<Http01ChallengeKey, Http01InterceptError<Infallible>> {
    http01_challenge_key_for_resolver(host, token)
}

fn http01_challenge_key_for_resolver<E>(
    host: &str,
    token: &str,
) -> Result<Http01ChallengeKey, Http01InterceptError<E>> {
    if token.contains('/') {
        return Err(Http01InterceptError::InvalidTokenSegment);
    }
    if token.is_empty() {
        return Err(Http01InterceptError::InvalidChallenge(
            InvalidHttp01Challenge::EmptyToken,
        ));
    }

    let host = canonical_http_host(host).map_err(Http01InterceptError::InvalidHost)?;
    Http01ChallengeKey::new(host, token).map_err(Http01InterceptError::InvalidChallenge)
}

impl Http01ChallengeResponse {
    pub fn new(key_authorization: impl Into<String>) -> Self {
        Self {
            key_authorization: key_authorization.into(),
        }
    }

    #[cfg(test)]
    pub fn key_authorization(&self) -> &str {
        &self.key_authorization
    }

    pub fn into_http_response(self) -> Response<Bytes> {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, HTTP01_CONTENT_TYPE)
            .body(Bytes::from(self.key_authorization))
            .expect("static HTTP-01 response shape is valid")
    }
}

fn request_host<B, E>(request: &Request<B>) -> Result<&str, Http01InterceptError<E>> {
    request
        .headers()
        .get(HOST)
        .ok_or(Http01InterceptError::MissingHost)?
        .to_str()
        .map_err(|_| Http01InterceptError::InvalidHostHeader)
}

fn canonical_http_host(host: &str) -> Result<String, RequestIdentityError> {
    match RouteRequestIdentity::http(host, Some("/"))?.into_identity() {
        RouteIdentity::Http { host, .. } => Ok(host.as_str().to_owned()),
        RouteIdentity::Sni { .. } => unreachable!("HTTP constructor returns HTTP identity"),
    }
}

impl<E> fmt::Display for Http01InterceptError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHost => f.write_str("HTTP-01 request is missing Host"),
            Self::InvalidHostHeader => f.write_str("HTTP-01 request Host is not valid ASCII"),
            Self::InvalidTokenSegment => f.write_str("HTTP-01 challenge token is not one segment"),
            Self::InvalidHost(error) => write!(f, "HTTP-01 request Host is invalid: {error}"),
            Self::InvalidChallenge(error) => write!(f, "HTTP-01 challenge key is invalid: {error}"),
            Self::Resolve(error) => write!(f, "HTTP-01 challenge resolve failed: {error}"),
        }
    }
}

impl<E> Error for Http01InterceptError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidHost(error) => Some(error),
            Self::InvalidChallenge(error) => Some(error),
            Self::Resolve(error) => Some(error),
            Self::MissingHost | Self::InvalidHostHeader | Self::InvalidTokenSegment => None,
        }
    }
}

#[cfg(test)]
mod tests;
