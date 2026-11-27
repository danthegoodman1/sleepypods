use std::{
    error::Error,
    fmt,
    time::{Duration, SystemTime},
};

use crate::route::{InvalidRouteHost, RouteHost};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Http01ChallengeKey {
    host: RouteHost,
    token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Http01ChallengeRecord {
    key: Http01ChallengeKey,
    key_authorization: String,
    expires_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutHttp01ChallengeRequest {
    key: Http01ChallengeKey,
    key_authorization: String,
    expires_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteHttp01ChallengeRequest {
    key: Http01ChallengeKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidHttp01Challenge {
    Host(InvalidRouteHost),
    EmptyToken,
    EmptyKeyAuthorization,
    AlreadyExpired,
}

impl Http01ChallengeKey {
    pub fn new(
        host: impl AsRef<str>,
        token: impl Into<String>,
    ) -> Result<Self, InvalidHttp01Challenge> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(InvalidHttp01Challenge::EmptyToken);
        }

        Ok(Self {
            host: RouteHost::exact(host).map_err(InvalidHttp01Challenge::Host)?,
            token,
        })
    }

    pub fn host(&self) -> &RouteHost {
        &self.host
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

impl Http01ChallengeRecord {
    pub fn new(
        key: Http01ChallengeKey,
        key_authorization: impl Into<String>,
        expires_at: SystemTime,
        now: SystemTime,
    ) -> Result<Self, InvalidHttp01Challenge> {
        let request = PutHttp01ChallengeRequest::new(key, key_authorization, expires_at, now)?;

        Ok(Self {
            key: request.key,
            key_authorization: request.key_authorization,
            expires_at: request.expires_at,
        })
    }

    pub fn key(&self) -> &Http01ChallengeKey {
        &self.key
    }

    pub fn key_authorization(&self) -> &str {
        &self.key_authorization
    }

    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }
}

impl PutHttp01ChallengeRequest {
    pub fn new(
        key: Http01ChallengeKey,
        key_authorization: impl Into<String>,
        expires_at: SystemTime,
        now: SystemTime,
    ) -> Result<Self, InvalidHttp01Challenge> {
        let key_authorization = key_authorization.into();
        if key_authorization.trim().is_empty() {
            return Err(InvalidHttp01Challenge::EmptyKeyAuthorization);
        }

        if expires_at <= now {
            return Err(InvalidHttp01Challenge::AlreadyExpired);
        }

        Ok(Self {
            key,
            key_authorization,
            expires_at,
        })
    }

    pub fn with_ttl(
        key: Http01ChallengeKey,
        key_authorization: impl Into<String>,
        ttl: Duration,
        now: SystemTime,
    ) -> Result<Self, InvalidHttp01Challenge> {
        Self::new(key, key_authorization, now + ttl, now)
    }

    pub fn key(&self) -> &Http01ChallengeKey {
        &self.key
    }

    pub fn key_authorization(&self) -> &str {
        &self.key_authorization
    }

    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }
}

impl DeleteHttp01ChallengeRequest {
    pub fn new(key: Http01ChallengeKey) -> Self {
        Self { key }
    }

    pub fn key(&self) -> &Http01ChallengeKey {
        &self.key
    }
}

impl fmt::Display for InvalidHttp01Challenge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Host(error) => write!(f, "{error}"),
            Self::EmptyToken => f.write_str("HTTP-01 challenge token must not be empty"),
            Self::EmptyKeyAuthorization => {
                f.write_str("HTTP-01 key authorization must not be empty")
            }
            Self::AlreadyExpired => f.write_str("HTTP-01 challenge expiry must be in the future"),
        }
    }
}

impl Error for InvalidHttp01Challenge {}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::{Http01ChallengeKey, InvalidHttp01Challenge, PutHttp01ChallengeRequest};

    #[test]
    fn challenge_key_normalizes_host_and_requires_token() {
        let key = Http01ChallengeKey::new("Example.COM.", "token").expect("valid challenge key");

        assert_eq!(key.host().as_str(), "example.com");
        assert_eq!(key.token(), "token");
        assert!(matches!(
            Http01ChallengeKey::new("example.com", " "),
            Err(InvalidHttp01Challenge::EmptyToken)
        ));
    }

    #[test]
    fn put_request_requires_future_expiry() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let key = Http01ChallengeKey::new("example.com", "token").expect("valid key");

        assert!(matches!(
            PutHttp01ChallengeRequest::new(key, "authorization", now, now),
            Err(InvalidHttp01Challenge::AlreadyExpired)
        ));
    }

    #[test]
    fn put_request_exposes_validated_fields_by_accessor() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let key = Http01ChallengeKey::new("example.com", "token").expect("valid key");
        let request = PutHttp01ChallengeRequest::with_ttl(
            key.clone(),
            "authorization",
            Duration::from_secs(60),
            now,
        )
        .expect("valid put request");

        assert_eq!(request.key(), &key);
        assert_eq!(request.key_authorization(), "authorization");
        assert_eq!(request.expires_at(), now + Duration::from_secs(60));
    }
}
