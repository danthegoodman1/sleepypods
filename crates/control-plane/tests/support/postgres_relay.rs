//! A TCP relay in front of the test database.
//!
//! It counts round trips: a round trip starts when a session sends a request
//! after the server last answered it. It can add latency to every round trip,
//! as a network hop does, and it can cut every session, as an outage does. It
//! can also lose one answer: the server runs the request, and its session
//! resets before the client reads the outcome.
#![allow(dead_code)]

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
};

type RelayResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct Relay {
    address: SocketAddr,
    shared: Arc<Shared>,
    task: tokio::task::JoinHandle<()>,
}

struct Shared {
    latency: Duration,
    round_trips: AtomicU64,
    cut: watch::Sender<bool>,
    /// Bytes whose next appearance in a request cuts the relay. That request
    /// is held, so it is in flight on its session when the reset arrives.
    trigger: Mutex<Option<Vec<u8>>>,
    /// Bytes whose next appearance in a request loses that request's answer.
    answer_trigger: Mutex<Option<Vec<u8>>>,
    /// True while a lost answer waits for the test to reset its session.
    held: watch::Sender<bool>,
    answers_lost: AtomicU64,
}

/// The single TCP server a test database URL names.
pub fn database_server(base: &str) -> RelayResult<String> {
    let config: tokio_postgres::Config = base.parse()?;
    let [tokio_postgres::config::Host::Tcp(host)] = config.get_hosts() else {
        return Err("the test database URL must name one TCP host".into());
    };
    let server = format!(
        "{host}:{}",
        config.get_ports().first().copied().unwrap_or(5432)
    );
    if !base.contains(&server) {
        return Err(format!("the test database URL must spell out {server}").into());
    }
    Ok(server)
}

impl Relay {
    pub async fn start(base: &str) -> RelayResult<Self> {
        Self::with_latency(base, Duration::ZERO).await
    }

    /// A relay that holds each request for `latency` before it forwards it.
    pub async fn with_latency(base: &str, latency: Duration) -> RelayResult<Self> {
        let upstream = tokio::net::lookup_host(database_server(base)?)
            .await?
            .next()
            .ok_or("the test database host does not resolve")?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shared = Arc::new(Shared {
            latency,
            round_trips: AtomicU64::new(0),
            cut: watch::channel(false).0,
            trigger: Mutex::new(None),
            answer_trigger: Mutex::new(None),
            held: watch::channel(false).0,
            answers_lost: AtomicU64::new(0),
        });
        let relay = shared.clone();
        let task = tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                if *relay.cut.borrow() {
                    let _ = inbound.set_zero_linger();
                    continue;
                }
                let _ = inbound.set_nodelay(true);
                let relay = relay.clone();
                tokio::spawn(async move {
                    if let Ok(outbound) = TcpStream::connect(upstream).await {
                        let _ = outbound.set_nodelay(true);
                        relay_session(inbound, outbound, &relay).await;
                    }
                });
            }
        });
        Ok(Self {
            address,
            shared,
            task,
        })
    }

    /// The connection URL `base` names, with this relay as its server.
    pub fn url(&self, base: &str) -> RelayResult<String> {
        Ok(base.replacen(&database_server(base)?, &self.address.to_string(), 1))
    }

    /// Round trips every session has started so far.
    pub fn round_trips(&self) -> u64 {
        self.shared.round_trips.load(Ordering::SeqCst)
    }

    /// Cuts the relay at the next request that carries `bytes`.
    pub fn cut_at(&self, bytes: &[u8]) {
        *self.shared.trigger.lock().unwrap() = Some(bytes.to_vec());
    }

    pub async fn wait_until_cut(&self) {
        let _ = self.shared.cut.subscribe().wait_for(|cut| *cut).await;
    }

    pub fn restore(&self) {
        self.shared.cut.send_replace(false);
    }

    /// Forwards the next request that carries `bytes`, and holds the server's
    /// answer to it. Empty `bytes` match the next request that carries work:
    /// an empty query, which a session sends to drain itself, never matches.
    pub fn lose_answer_to(&self, bytes: &[u8]) {
        *self.shared.answer_trigger.lock().unwrap() = Some(bytes.to_vec());
    }

    /// Waits until the server has answered the request `lose_answer_to`
    /// named, and the relay holds that answer.
    pub async fn wait_until_answer_held(&self) {
        let _ = self.shared.held.subscribe().wait_for(|held| *held).await;
    }

    /// Resets the session whose answer the relay holds, so its client never
    /// reads the answer.
    pub fn drop_held_answer(&self) {
        self.shared.held.send_replace(false);
    }

    /// Answers the relay has lost so far.
    pub fn answers_lost(&self) -> u64 {
        self.shared.answers_lost.load(Ordering::SeqCst)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Copies one session in both directions until either side closes or the
/// relay is cut, and then resets both connections.
async fn relay_session(mut inbound: TcpStream, mut outbound: TcpStream, relay: &Shared) {
    let mut watched = relay.cut.subscribe();
    // True once the server has answered the session's last request.
    let answered = AtomicBool::new(true);
    // True once this session sent the request whose answer the relay loses.
    let losing = AtomicBool::new(false);
    {
        let (mut client_read, mut client_write) = inbound.split();
        let (mut server_read, mut server_write) = outbound.split();
        let requests = async {
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let read = client_read.read(&mut buffer).await?;
                if read == 0 {
                    return Ok::<_, std::io::Error>(());
                }
                let request = &buffer[..read];
                if relay.fires(request) {
                    relay.cut.send_replace(true);
                    std::future::pending::<()>().await;
                }
                if relay.answer_fires(request) {
                    losing.store(true, Ordering::SeqCst);
                }
                if answered.swap(false, Ordering::SeqCst) {
                    relay.round_trips.fetch_add(1, Ordering::SeqCst);
                    if !relay.latency.is_zero() {
                        // The thread sleep keeps sub-millisecond latency,
                        // which the runtime's millisecond timer rounds up.
                        let latency = relay.latency;
                        let _ =
                            tokio::task::spawn_blocking(move || std::thread::sleep(latency)).await;
                    }
                }
                server_write.write_all(request).await?;
            }
        };
        let responses = async {
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let read = server_read.read(&mut buffer).await?;
                if read == 0 {
                    return Ok::<_, std::io::Error>(());
                }
                if losing.load(Ordering::SeqCst) {
                    relay.answers_lost.fetch_add(1, Ordering::SeqCst);
                    relay.held.send_replace(true);
                    let _ = relay.held.subscribe().wait_for(|held| !*held).await;
                    return Ok(());
                }
                // Marked before the client can see the answer and reply.
                answered.store(true, Ordering::SeqCst);
                client_write.write_all(&buffer[..read]).await?;
            }
        };
        tokio::select! {
            _ = requests => {}
            _ = responses => {}
            _ = watched.wait_for(|cut| *cut) => {}
        }
    }
    let _ = inbound.set_zero_linger();
    let _ = outbound.set_zero_linger();
}

impl Shared {
    fn fires(&self, request: &[u8]) -> bool {
        take_match(&self.trigger, request)
    }

    fn answer_fires(&self, request: &[u8]) -> bool {
        take_match(&self.answer_trigger, request)
    }
}

/// The simple-protocol message for an empty query.
const EMPTY_QUERY: &[u8] = b"Q\0\0\0\x05\0";

/// Disarms and reports a trigger that `request` carries.
fn take_match(trigger: &Mutex<Option<Vec<u8>>>, request: &[u8]) -> bool {
    let mut armed = trigger.lock().unwrap();
    let fires = armed.as_ref().is_some_and(|bytes| {
        if bytes.is_empty() {
            request != EMPTY_QUERY
        } else {
            request.windows(bytes.len()).any(|window| window == bytes)
        }
    });
    if fires {
        *armed = None;
    }
    fires
}
