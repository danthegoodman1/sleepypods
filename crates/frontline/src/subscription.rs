//! The Subscribe stream as the route coordinator sees it.
//!
//! A proxy subscribes to one exact host at a time and chooses the ID that
//! names the subscription. The control plane answers with the host's complete
//! rule set, sends the set again whenever it changes, and ends a single
//! subscription with an error when it cannot keep it current.

use std::{future::Future, pin::Pin};

use sleepypods_api::{CachePolicy, RouteHost, RouteKind, RouteRule};
use tokio::sync::mpsc;

/// Chosen by the proxy and never reused within a process.
pub type SubscriptionId = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteStreamRequest {
    Subscribe {
        id: SubscriptionId,
        kind: RouteKind,
        host: RouteHost,
    },
    Unsubscribe {
        id: SubscriptionId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteStreamReply {
    /// The complete rule set for the subscribed host. An empty set means no
    /// binding applies, and the control plane holds no subscription for it.
    Rules {
        id: SubscriptionId,
        rules: Vec<RouteRule>,
        cache_policy: CachePolicy,
    },
    /// The subscription ended. The stream and its other subscriptions go on.
    Error {
        id: SubscriptionId,
        code: SubscribeErrorCode,
        message: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscribeErrorCode {
    InvalidArgument,
    ResourceExhausted,
    Unavailable,
}

/// What a stream delivers to the coordinator, in the order it arrived.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteStreamEvent {
    Reply(RouteStreamReply),
    /// The control plane ended the stream in order. Every message it sent was
    /// delivered first.
    Ended,
    /// The stream failed or carried a message the proxy could not read.
    /// Messages may have been lost.
    Failed,
}

/// One open Subscribe stream. Dropping it closes the stream.
#[derive(Debug)]
pub struct RouteStream {
    pub requests: mpsc::Sender<RouteStreamRequest>,
    /// Closing without `Ended` counts as `Failed`.
    pub events: mpsc::Receiver<RouteStreamEvent>,
}

pub type RouteSubscriptionFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Opens Subscribe streams. The route coordinator holds one stream at a time
/// and opens the next when it ends.
pub trait RouteSubscriptionClient {
    type Error: Send;

    fn connect(&mut self) -> RouteSubscriptionFuture<'static, RouteStream, Self::Error>;
}
