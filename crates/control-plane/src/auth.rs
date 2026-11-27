pub use sleepypods_api::auth::{BearerToken, InvalidBearerToken, OptionalBearerTokenInterceptor};

use std::{error::Error, fmt, sync::Arc};

use sleepypods_observability::{
    metrics::CONTROL_PLANE_AUTH_DECISIONS_TOTAL,
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder,
        EVENT_CONTROL_PLANE_AUTH,
    },
    Outcome,
};
use tonic::{metadata::MetadataMap, service::Interceptor, Request, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CallerRole {
    Operator,
    Proxy,
    Sidecar,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthConfig {
    NoAuth,
    StaticBearerTokens(StaticBearerTokens),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaticBearerTokens {
    operator: BearerToken,
    proxy: BearerToken,
    sidecar: BearerToken,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidStaticBearerTokens {
    InvalidToken {
        role: CallerRole,
        source: InvalidBearerToken,
    },
    DuplicateToken {
        first: CallerRole,
        second: CallerRole,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthFailureReason {
    Missing,
    Malformed,
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationFailureReason {
    Authentication(AuthFailureReason),
    WrongRole {
        actual: CallerRole,
        required: CallerRole,
    },
}

pub trait AuthProvider: Send + Sync {
    fn authenticate(&self, metadata: &MetadataMap) -> Result<CallerRole, AuthFailureReason>;
}

#[derive(Clone)]
pub struct ControlPlaneAuth {
    mode: AuthMode,
    observability: ObservabilityRecorder,
}

#[derive(Clone)]
enum AuthMode {
    NoAuth,
    Provider(Arc<dyn AuthProvider>),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthenticatedRole(pub CallerRole);

#[derive(Clone)]
pub struct ControlPlaneAuthInterceptor {
    auth: ControlPlaneAuth,
    service_name: &'static str,
    required_role: CallerRole,
}

#[derive(Debug)]
pub struct StaticBearerAuthProvider {
    tokens: StaticBearerTokens,
}

impl CallerRole {
    pub const ALL: &'static [Self] = &[Self::Operator, Self::Proxy, Self::Sidecar];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Proxy => "proxy",
            Self::Sidecar => "sidecar",
        }
    }
}

impl AuthConfig {
    pub fn static_bearer_tokens(tokens: StaticBearerTokens) -> Self {
        Self::StaticBearerTokens(tokens)
    }

    pub(crate) fn sidecar_bearer_token(&self) -> Option<&BearerToken> {
        match self {
            Self::NoAuth => None,
            Self::StaticBearerTokens(tokens) => Some(tokens.token_for(CallerRole::Sidecar)),
        }
    }
}

impl StaticBearerTokens {
    pub fn new(
        operator: impl Into<String>,
        proxy: impl Into<String>,
        sidecar: impl Into<String>,
    ) -> Result<Self, InvalidStaticBearerTokens> {
        let operator = BearerToken::new("operator_token", operator).map_err(|source| {
            InvalidStaticBearerTokens::InvalidToken {
                role: CallerRole::Operator,
                source,
            }
        })?;
        let proxy = BearerToken::new("proxy_token", proxy).map_err(|source| {
            InvalidStaticBearerTokens::InvalidToken {
                role: CallerRole::Proxy,
                source,
            }
        })?;
        let sidecar = BearerToken::new("sidecar_token", sidecar).map_err(|source| {
            InvalidStaticBearerTokens::InvalidToken {
                role: CallerRole::Sidecar,
                source,
            }
        })?;

        ensure_distinct(&operator, &proxy, CallerRole::Operator, CallerRole::Proxy)?;
        ensure_distinct(
            &operator,
            &sidecar,
            CallerRole::Operator,
            CallerRole::Sidecar,
        )?;
        ensure_distinct(&proxy, &sidecar, CallerRole::Proxy, CallerRole::Sidecar)?;

        Ok(Self {
            operator,
            proxy,
            sidecar,
        })
    }

    pub fn token_for(&self, role: CallerRole) -> &BearerToken {
        match role {
            CallerRole::Operator => &self.operator,
            CallerRole::Proxy => &self.proxy,
            CallerRole::Sidecar => &self.sidecar,
        }
    }
}

impl ControlPlaneAuth {
    pub fn from_config(config: AuthConfig, observability: ObservabilityRecorder) -> Self {
        let mode = match config {
            AuthConfig::NoAuth => AuthMode::NoAuth,
            AuthConfig::StaticBearerTokens(tokens) => {
                AuthMode::Provider(Arc::new(StaticBearerAuthProvider::new(tokens)))
            }
        };

        Self {
            mode,
            observability,
        }
    }

    pub fn interceptor(
        &self,
        service_name: &'static str,
        required_role: CallerRole,
    ) -> ControlPlaneAuthInterceptor {
        ControlPlaneAuthInterceptor {
            auth: self.clone(),
            service_name,
            required_role,
        }
    }

    fn authorize(
        &self,
        metadata: &MetadataMap,
        service_name: &'static str,
        required_role: CallerRole,
    ) -> Result<CallerRole, AuthorizationFailureReason> {
        match &self.mode {
            AuthMode::NoAuth => {
                self.record_decision(service_name, required_role, Some(required_role), "accepted");
                Ok(required_role)
            }
            AuthMode::Provider(provider) => match provider.authenticate(metadata) {
                Ok(actual) if actual == required_role => {
                    self.record_decision(service_name, required_role, Some(actual), "accepted");
                    Ok(actual)
                }
                Ok(actual) => {
                    self.record_decision(service_name, required_role, Some(actual), "wrong_role");
                    Err(AuthorizationFailureReason::WrongRole {
                        actual,
                        required: required_role,
                    })
                }
                Err(reason) => {
                    self.record_decision(service_name, required_role, None, reason.as_str());
                    Err(AuthorizationFailureReason::Authentication(reason))
                }
            },
        }
    }

    /// Counts every decision and logs the rejected ones: an accepted call is
    /// the common case and its handler reports anything worth a line.
    fn record_decision(
        &self,
        service_name: &'static str,
        required_role: CallerRole,
        caller_role: Option<CallerRole>,
        reason: &'static str,
    ) {
        let accepted = reason == "accepted";
        let outcome = if accepted {
            Outcome::Success
        } else {
            Outcome::Rejected
        };
        self.observability.record_metric(MetricObservation::new(
            CONTROL_PLANE_AUTH_DECISIONS_TOTAL,
            vec![outcome.metric_label()],
            1.0,
        ));
        if accepted {
            return;
        }

        let mut fields = vec![
            LogField::auth_decision("rejected"),
            LogField::auth_reason(reason),
            LogField::auth_required_role(required_role),
            LogField::grpc_service(service_name),
        ];
        if let Some(caller_role) = caller_role {
            fields.push(LogField::auth_caller_role(caller_role));
        }
        self.observability
            .record_log(LifecycleLogEvent::new(EVENT_CONTROL_PLANE_AUTH, fields));
    }
}

impl Interceptor for ControlPlaneAuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        self.auth
            .authorize(request.metadata(), self.service_name, self.required_role)
            .map(|role| {
                if matches!(self.auth.mode, AuthMode::Provider(_)) {
                    request.extensions_mut().insert(AuthenticatedRole(role));
                }
                request
            })
            .map_err(auth_failure_to_status)
    }
}

impl StaticBearerAuthProvider {
    pub fn new(tokens: StaticBearerTokens) -> Self {
        Self { tokens }
    }
}

impl AuthProvider for StaticBearerAuthProvider {
    fn authenticate(&self, metadata: &MetadataMap) -> Result<CallerRole, AuthFailureReason> {
        let credential = bearer_credential(metadata)?;

        for role in CallerRole::ALL {
            if token_eq(
                credential.as_bytes(),
                self.tokens.token_for(*role).as_secret_str().as_bytes(),
            ) {
                return Ok(*role);
            }
        }

        Err(AuthFailureReason::Invalid)
    }
}

impl AuthFailureReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Malformed => "malformed",
            Self::Invalid => "invalid",
        }
    }
}

fn bearer_credential(metadata: &MetadataMap) -> Result<&str, AuthFailureReason> {
    let value = metadata
        .get("authorization")
        .ok_or(AuthFailureReason::Missing)?;
    let value = value.to_str().map_err(|_| AuthFailureReason::Malformed)?;
    let (scheme, credential) = value.split_once(' ').ok_or(AuthFailureReason::Malformed)?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || credential.is_empty()
        || credential.chars().any(char::is_whitespace)
    {
        return Err(AuthFailureReason::Malformed);
    }

    Ok(credential)
}

fn token_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

fn ensure_distinct(
    first_token: &BearerToken,
    second_token: &BearerToken,
    first: CallerRole,
    second: CallerRole,
) -> Result<(), InvalidStaticBearerTokens> {
    if token_eq(
        first_token.as_secret_str().as_bytes(),
        second_token.as_secret_str().as_bytes(),
    ) {
        Err(InvalidStaticBearerTokens::DuplicateToken { first, second })
    } else {
        Ok(())
    }
}

fn auth_failure_to_status(reason: AuthorizationFailureReason) -> Status {
    match reason {
        AuthorizationFailureReason::Authentication(AuthFailureReason::Missing) => {
            Status::unauthenticated("authorization bearer token is required")
        }
        AuthorizationFailureReason::Authentication(AuthFailureReason::Malformed) => {
            Status::unauthenticated("authorization bearer token is malformed")
        }
        AuthorizationFailureReason::Authentication(AuthFailureReason::Invalid) => {
            Status::unauthenticated("authorization bearer token is invalid")
        }
        AuthorizationFailureReason::WrongRole { .. } => {
            Status::permission_denied("credentials are not authorized for this service")
        }
    }
}

impl fmt::Debug for ControlPlaneAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneAuth").finish_non_exhaustive()
    }
}

impl fmt::Display for CallerRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for InvalidStaticBearerTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidToken { role, source } => {
                write!(f, "{role} bearer token is invalid: {source}")
            }
            Self::DuplicateToken { first, second } => {
                write!(f, "{first} and {second} bearer tokens must be distinct")
            }
        }
    }
}

impl Error for InvalidStaticBearerTokens {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidToken { source, .. } => Some(source),
            Self::DuplicateToken { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use sleepypods_observability::recorder::{
        InMemoryObservability, ObservabilityEvent, FIELD_AUTH_CALLER_ROLE, FIELD_AUTH_DECISION,
        FIELD_AUTH_REASON, FIELD_AUTH_REQUIRED_ROLE, FIELD_GRPC_SERVICE,
    };
    use tonic::{metadata::MetadataValue, Code};

    use super::*;

    const SERVICE: &str = "sleepypods.controlplane.v1.ProxyControlPlane";

    #[test]
    fn static_provider_accepts_distinct_role_tokens() {
        let provider = StaticBearerAuthProvider::new(tokens());

        assert_eq!(
            provider.authenticate(&metadata("Bearer operator-secret")),
            Ok(CallerRole::Operator)
        );
        assert_eq!(
            provider.authenticate(&metadata("Bearer proxy-secret")),
            Ok(CallerRole::Proxy)
        );
        assert_eq!(
            provider.authenticate(&metadata("Bearer sidecar-secret")),
            Ok(CallerRole::Sidecar)
        );
    }

    #[test]
    fn static_provider_classifies_missing_malformed_and_invalid_credentials() {
        let provider = StaticBearerAuthProvider::new(tokens());

        assert_eq!(
            provider.authenticate(&MetadataMap::new()),
            Err(AuthFailureReason::Missing)
        );
        assert_eq!(
            provider.authenticate(&metadata("Basic proxy-secret")),
            Err(AuthFailureReason::Malformed)
        );
        assert_eq!(
            provider.authenticate(&metadata("Bearer proxy-secret extra")),
            Err(AuthFailureReason::Malformed)
        );
        assert_eq!(
            provider.authenticate(&metadata("Bearer wrong-secret")),
            Err(AuthFailureReason::Invalid)
        );
    }

    #[test]
    fn interceptor_maps_wrong_role_to_permission_denied_before_handler() {
        let sink = InMemoryObservability::default();
        let auth = ControlPlaneAuth::from_config(
            AuthConfig::static_bearer_tokens(tokens()),
            sink.recorder(),
        );
        let mut interceptor = auth.interceptor(SERVICE, CallerRole::Proxy);

        let error = interceptor
            .call(request_with_metadata(metadata("Bearer operator-secret")))
            .expect_err("operator credentials cannot call proxy service");

        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(!error.message().contains("operator-secret"));
    }

    #[test]
    fn accepted_decisions_are_counted_and_write_no_log_event() {
        for config in [
            AuthConfig::NoAuth,
            AuthConfig::static_bearer_tokens(tokens()),
        ] {
            let sink = InMemoryObservability::default();
            let auth = ControlPlaneAuth::from_config(config, sink.recorder());
            let mut interceptor = auth.interceptor(SERVICE, CallerRole::Sidecar);

            interceptor
                .call(request_with_metadata(metadata("Bearer sidecar-secret")))
                .expect("the sidecar credential is accepted");

            assert_eq!(decision_outcomes(&sink.events()), ["success"]);
            assert!(logged_reasons(&sink.events()).is_empty());
        }
    }

    #[test]
    fn rejected_decisions_are_counted_and_logged_with_their_context() {
        let sink = InMemoryObservability::default();
        let auth = ControlPlaneAuth::from_config(
            AuthConfig::static_bearer_tokens(tokens()),
            sink.recorder(),
        );

        let _ = auth
            .interceptor(SERVICE, CallerRole::Proxy)
            .call(request_with_metadata(metadata("Bearer operator-secret")));

        let events = sink.events();
        assert_eq!(decision_outcomes(&events), ["rejected"]);
        let event = events
            .iter()
            .find_map(|event| match event {
                ObservabilityEvent::Log(event) => Some(event),
                ObservabilityEvent::Metric(_) => None,
            })
            .expect("a rejected decision is logged");
        assert_eq!(event.name(), EVENT_CONTROL_PLANE_AUTH);
        assert_eq!(event.field_value(FIELD_AUTH_DECISION), Some("rejected"));
        assert_eq!(event.field_value(FIELD_AUTH_REASON), Some("wrong_role"));
        assert_eq!(event.field_value(FIELD_AUTH_REQUIRED_ROLE), Some("proxy"));
        assert_eq!(event.field_value(FIELD_AUTH_CALLER_ROLE), Some("operator"));
        assert_eq!(event.field_value(FIELD_GRPC_SERVICE), Some(SERVICE));
    }

    #[test]
    fn observability_records_rejections_without_credential_material() {
        let sink = InMemoryObservability::default();
        let auth = ControlPlaneAuth::from_config(
            AuthConfig::static_bearer_tokens(tokens()),
            sink.recorder(),
        );
        let mut interceptor = auth.interceptor(SERVICE, CallerRole::Proxy);

        let _ = interceptor.call(request_with_metadata(metadata(
            "Bearer very-secret-wrong-token",
        )));

        let debug = format!("{:?}", sink.events());
        assert!(debug.contains("invalid"));
        assert!(!debug.contains("very-secret-wrong-token"));
        assert!(!debug.contains("proxy-secret"));
    }

    #[test]
    fn observability_records_all_auth_decisions_without_secrets() {
        let sink = InMemoryObservability::default();
        let auth = ControlPlaneAuth::from_config(
            AuthConfig::static_bearer_tokens(tokens()),
            sink.recorder(),
        );

        let _ = auth
            .interceptor(SERVICE, CallerRole::Proxy)
            .call(request_with_metadata(metadata("Bearer proxy-secret")));
        let _ = auth
            .interceptor(SERVICE, CallerRole::Proxy)
            .call(Request::new(()));
        let _ = auth
            .interceptor(SERVICE, CallerRole::Proxy)
            .call(request_with_metadata(metadata(
                "Bearer very-secret-wrong-token",
            )));
        let _ = auth
            .interceptor(SERVICE, CallerRole::Proxy)
            .call(request_with_metadata(metadata("Bearer operator-secret")));

        let events = sink.events();
        assert_eq!(
            decision_outcomes(&events),
            ["success", "rejected", "rejected", "rejected"]
        );
        assert_eq!(
            logged_reasons(&events),
            ["missing", "invalid", "wrong_role"]
        );

        let debug = format!("{events:?}");
        for secret in [
            "operator-secret",
            "proxy-secret",
            "sidecar-secret",
            "very-secret-wrong-token",
        ] {
            assert!(!debug.contains(secret), "auth logs leaked {secret}");
        }
    }

    #[test]
    fn static_token_config_rejects_malformed_or_duplicate_tokens() {
        assert!(matches!(
            BearerToken::new("test", "has space"),
            Err(InvalidBearerToken::ContainsWhitespace { field: "test" })
        ));
        assert!(matches!(
            BearerToken::new("test", "unicodé"),
            Err(InvalidBearerToken::ContainsNonAscii { field: "test" })
        ));
        assert!(matches!(
            StaticBearerTokens::new("same", "same", "other"),
            Err(InvalidStaticBearerTokens::DuplicateToken {
                first: CallerRole::Operator,
                second: CallerRole::Proxy
            })
        ));
        assert!(!format!("{:?}", tokens()).contains("operator-secret"));
    }

    fn decision_outcomes(events: &[ObservabilityEvent]) -> Vec<&'static str> {
        events
            .iter()
            .filter_map(|event| match event {
                ObservabilityEvent::Metric(metric) => {
                    assert_eq!(metric.name(), CONTROL_PLANE_AUTH_DECISIONS_TOTAL.name());
                    assert_eq!(metric.value(), 1.0);
                    Some(metric.labels()[0].value())
                }
                ObservabilityEvent::Log(_) => None,
            })
            .collect()
    }

    fn logged_reasons(events: &[ObservabilityEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|event| match event {
                ObservabilityEvent::Log(event) => Some(
                    event
                        .field_value(FIELD_AUTH_REASON)
                        .expect("auth reason")
                        .to_owned(),
                ),
                ObservabilityEvent::Metric(_) => None,
            })
            .collect()
    }

    fn tokens() -> StaticBearerTokens {
        StaticBearerTokens::new("operator-secret", "proxy-secret", "sidecar-secret")
            .expect("tokens are valid")
    }

    fn metadata(authorization: &'static str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", MetadataValue::from_static(authorization));
        metadata
    }

    fn request_with_metadata(metadata: MetadataMap) -> Request<()> {
        let mut request = Request::new(());
        *request.metadata_mut() = metadata;
        request
    }
}
