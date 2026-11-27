use super::admission::ApiLimits;
use crate::{InstanceId, RouteBindingId, RouteBindingRecord, RouteIdentity};
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore};

const ROUTE_EVENT_BUFFER: usize = 4096;

#[derive(Clone, Debug)]
pub struct RouteSubscriptionBroker {
    sender: broadcast::Sender<RouteBindingChange>,
    pub(crate) streams: Arc<Semaphore>,
    pub(crate) certificate_streams: Arc<Semaphore>,
    pub(crate) admission: super::admission::RpcAdmissionLayer,
    pub(crate) limits: ApiLimits,
    pub(crate) cancellation: crate::runtime_work::Cancellation,
}

#[derive(Clone, Debug)]
pub enum RouteBindingChange {
    Route {
        route_binding_id: RouteBindingId,
        removed: bool,
        identity: Option<RouteIdentity>,
    },
    Instance(InstanceId),
    Reset,
}

impl RouteSubscriptionBroker {
    pub fn new() -> Self {
        Self::with_limits(ApiLimits::default())
    }
    pub fn with_limits(limits: ApiLimits) -> Self {
        let (sender, _) = broadcast::channel(ROUTE_EVENT_BUFFER);
        Self {
            sender,
            admission: super::admission::RpcAdmissionLayer::with_delivery_timeout(
                limits.rpc_concurrency,
                limits.unary_delivery_timeout,
            ),
            streams: Arc::new(Semaphore::new(limits.subscription_streams)),
            certificate_streams: Arc::new(Semaphore::new(super::certificate_watch::WATCH_STREAMS)),
            limits,
            cancellation: crate::runtime_work::Cancellation::new(),
        }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<RouteBindingChange> {
        self.sender.subscribe()
    }
    pub fn reset(&self) {
        let _ = self.sender.send(RouteBindingChange::Reset);
    }
    pub fn shutdown(&self) {
        self.cancellation.cancel();
    }
    pub fn notify_instance_changed(&self, id: InstanceId) {
        let _ = self.sender.send(RouteBindingChange::Instance(id));
    }
    pub fn notify_route_removed(&self, route_binding_id: RouteBindingId) {
        let _ = self.sender.send(RouteBindingChange::Route {
            route_binding_id,
            removed: true,
            identity: None,
        });
    }
    pub fn notify_routes_removed(&self, bindings: &[RouteBindingRecord]) {
        for binding in bindings {
            self.notify_route_removed(binding.id.clone());
        }
    }
    pub fn notify_route_changed(&self, route_binding_id: RouteBindingId, identity: RouteIdentity) {
        let _ = self.sender.send(RouteBindingChange::Route {
            route_binding_id,
            removed: false,
            identity: Some(identity),
        });
    }
    pub fn notify_routes_changed(&self, bindings: &[RouteBindingRecord]) {
        for binding in bindings {
            self.notify_route_changed(binding.id.clone(), binding.identity.clone());
        }
    }
    pub(crate) fn publish_durable(
        &self,
        payload: &serde_json::Value,
    ) -> Result<(), crate::store::StoreError> {
        let field = |name| {
            payload[name]
                .as_str()
                .ok_or_else(|| crate::store::StoreError::internal("invalid durable route event"))
        };
        if field("kind")? == "instance" {
            self.notify_instance_changed(
                InstanceId::new(field("instance_id")?)
                    .map_err(|error| crate::store::StoreError::internal(error.to_string()))?,
            );
        } else {
            let id = RouteBindingId::new(field("route_binding_id")?)
                .map_err(|error| crate::store::StoreError::internal(error.to_string()))?;
            if payload["removed"].as_bool() == Some(true) {
                self.notify_route_removed(id);
                return Ok(());
            }
            let host = match field("host_kind")? {
                "exact" => crate::RouteHost::exact(field("host")?),
                _ => crate::RouteHost::wildcard_suffix(field("host")?),
            }
            .map_err(|error| crate::store::StoreError::internal(error.to_string()))?;
            let identity = if field("identity_kind")? == "http" {
                let path = payload["path_prefix"]
                    .as_str()
                    .map(crate::PathPrefix::new)
                    .transpose()
                    .map_err(|error| crate::store::StoreError::internal(error.to_string()))?;
                RouteIdentity::Http { host, path }
            } else {
                RouteIdentity::Sni { host }
            };
            self.notify_route_changed(id, identity);
        }
        Ok(())
    }
}
impl Default for RouteSubscriptionBroker {
    fn default() -> Self {
        Self::new()
    }
}
