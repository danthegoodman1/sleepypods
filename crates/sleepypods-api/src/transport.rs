//! Independently provisioned native control-plane transport. Tenant certificate
//! resolution never supplies this trust configuration.
use std::time::Duration;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

// Bound each background reconnect stage even when the requesting RPC is
// canceled. Tonic keeps its connecting future in the shared Channel; an RPC
// deadline alone does not cancel that attempt. These are per-stage bounds,
// independent of callers' overall request and startup budgets.
const CONNECT_STAGE_TIMEOUT: Duration = Duration::from_secs(2);
// A peer that vanished without closing fails the channel after one interval
// plus the acknowledgement timeout. Pings continue while the channel is idle:
// a frontline holds a quiet stream and a sidecar waits minutes between calls.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
pub const CONTROL_PLANE_TLS_CA_PEM_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM";
pub const MAX_CONTROL_PLANE_CA_BYTES: usize = 64 * 1024;

/// HTTPS always verifies the endpoint hostname and certificate chain. An
/// optional private CA augments public roots. Plain HTTP cannot carry CA config.
pub fn native_endpoint(
    uri: String,
    ca_pem: Option<&str>,
) -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = Endpoint::from_shared(uri)?
        .connect_timeout(CONNECT_STAGE_TIMEOUT)
        .http2_keep_alive_interval(HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(HTTP2_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(TCP_KEEPALIVE));
    match endpoint.uri().scheme_str() {
        Some("https") => {
            let mut tls = ClientTlsConfig::new()
                .with_webpki_roots()
                .timeout(CONNECT_STAGE_TIMEOUT);
            if let Some(pem) = ca_pem {
                if pem.is_empty() || pem.len() > MAX_CONTROL_PLANE_CA_BYTES {
                    return Err("invalid control-plane CA size".into());
                }
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            Ok(endpoint.tls_config(tls)?)
        }
        Some("http") if ca_pem.is_none() => Ok(endpoint),
        _ => Err(
            "control-plane endpoint requires http or verified https; CA trust requires https"
                .into(),
        ),
    }
}
