use super::test_support::CertificateFixture;
use super::*;
use crate::FrontlineTlsAdapter;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{
    rustls::{
        self,
        client::{
            ClientSessionMemoryCache, ClientSessionStore, Tls12ClientSessionValue,
            Tls13ClientSessionValue,
        },
        pki_types::{PrivatePkcs8KeyDer, ServerName},
        ClientConfig, HandshakeKind, NamedGroup, RootCertStore,
    },
    TlsAcceptor, TlsConnector,
};

#[derive(Debug)]
struct ObservedSessions {
    inner: ClientSessionMemoryCache,
    offered: AtomicUsize,
}
impl ClientSessionStore for ObservedSessions {
    fn set_kx_hint(&self, name: ServerName<'static>, group: NamedGroup) {
        self.inner.set_kx_hint(name, group)
    }
    fn kx_hint(&self, name: &ServerName<'_>) -> Option<NamedGroup> {
        self.inner.kx_hint(name)
    }
    fn set_tls12_session(&self, name: ServerName<'static>, value: Tls12ClientSessionValue) {
        self.inner.set_tls12_session(name, value)
    }
    fn tls12_session(&self, name: &ServerName<'_>) -> Option<Tls12ClientSessionValue> {
        let value = self.inner.tls12_session(name);
        if value.is_some() {
            self.offered.fetch_add(1, Ordering::SeqCst);
        }
        value
    }
    fn remove_tls12_session(&self, name: &ServerName<'static>) {
        self.inner.remove_tls12_session(name)
    }
    fn insert_tls13_ticket(&self, name: ServerName<'static>, value: Tls13ClientSessionValue) {
        self.inner.insert_tls13_ticket(name, value)
    }
    fn take_tls13_ticket(&self, name: &ServerName<'static>) -> Option<Tls13ClientSessionValue> {
        let value = self.inner.take_tls13_ticket(name);
        if value.is_some() {
            self.offered.fetch_add(1, Ordering::SeqCst);
        }
        value
    }
}

#[tokio::test]
async fn tls12_and_tls13_offered_sessions_require_current_authorization_and_full_handshake() {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let cert = rcgen::generate_simple_self_signed(vec!["app.example".into()]).unwrap();
        let der = cert.cert.der().clone();
        let key = cert.signing_key.serialize_der();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut seed = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[version])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![der.clone()],
                PrivatePkcs8KeyDer::from(key.clone()).into(),
            )
            .unwrap();
        seed.alpn_protocols = vec![b"h2".to_vec()];
        seed.session_storage = rustls::server::ServerSessionMemoryCache::new(8);
        seed.send_tls13_tickets = 4;
        seed.ticketer = rustls::crypto::ring::Ticketer::new().unwrap();
        let seed = TlsAcceptor::from(Arc::new(seed));
        let mut roots = RootCertStore::empty();
        roots.add(der.clone()).unwrap();
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[version])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let sessions = Arc::new(ObservedSessions {
            // Rustls rounds this to hostname buckets and leaves a spare slot.
            // 8 means one bucket, which immediately evicts its only hostname.
            inner: ClientSessionMemoryCache::new(32),
            offered: AtomicUsize::new(0),
        });
        config.resumption = rustls::client::Resumption::store(sessions.clone());
        config.alpn_protocols = vec![b"h2".to_vec()];
        let connector = TlsConnector::from(Arc::new(config));
        let fixture = CertificateFixture::new();
        fixture
            .publish(
                "app.example",
                vec![der.clone()],
                PrivatePkcs8KeyDer::from(key).into(),
            )
            .unwrap();
        let adapter = FrontlineTlsAdapter::new(fixture.cache.clone());
        for round in 0..4 {
            let (client_io, server_io) = tokio::io::duplex(64 * 1024);
            let authorized = round < 3;
            if round == 3 {
                fixture.api.responses.lock().unwrap().clear();
                evict(
                    &mut fixture.cache.shared.state.lock().unwrap(),
                    "app.example",
                );
            }
            let old_offers = sessions.offered.load(Ordering::SeqCst);
            let server = async {
                if round == 0 || round == 2 {
                    let mut stream = seed.accept(server_io).await.unwrap();
                    stream.write_all(&[42]).await.unwrap();
                    stream.flush().await.unwrap();
                    stream.shutdown().await.unwrap();
                } else if authorized {
                    let mut stream = adapter.terminate(server_io).await.unwrap().stream;
                    assert_eq!(
                        stream.get_ref().1.handshake_kind(),
                        Some(HandshakeKind::Full)
                    );
                    stream.write_all(&[42]).await.unwrap();
                    stream.flush().await.unwrap();
                    stream.shutdown().await.unwrap();
                } else {
                    assert!(adapter.terminate(server_io).await.is_err());
                }
            };
            let client = async {
                let result = connector
                    .connect(ServerName::try_from("app.example").unwrap(), client_io)
                    .await;
                if !authorized {
                    assert!(result.is_err());
                    return;
                }
                let mut stream = result.unwrap();
                assert_eq!(stream.read_u8().await.unwrap(), 42);
                let mut tail = Vec::new();
                stream.read_to_end(&mut tail).await.unwrap();
                assert_eq!(stream.get_ref().1.peer_certificates().unwrap()[0], der);
                assert_eq!(stream.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
                if round == 1 {
                    assert_eq!(
                        stream.get_ref().1.handshake_kind(),
                        Some(HandshakeKind::Full)
                    );
                }
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(server, client);
            })
            .await
            .unwrap();
            if round == 1 || round == 3 {
                assert!(
                    sessions.offered.load(Ordering::SeqCst) > old_offers,
                    "client must retrieve and offer a seeded session before authorization decision: {version:?}, round {round}"
                );
            }
        }
        assert_eq!(
            fixture.api.requests.lock().unwrap().len(),
            2,
            "seed server is test-only; dynamic authorized+removed each resolve once"
        );
        fixture.finish().await;
    }
}
