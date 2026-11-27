//! What a request does with the route its host and path selected, and the
//! wake call that starts a sleeping instance.

use sleepypods_api::{
    BackendEndpoint, BackendGeneration, Generation, InstanceId, InstanceState, RouteEntry,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WakeInstanceRequest {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
}

/// The control plane's answer to a wake call. Acceptance is not readiness:
/// the proxy learns that the instance runs from its host's rule set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeInstanceResponse {
    AlreadyRunning {
        instance_id: InstanceId,
        generation: Generation,
        backend: BackendEndpoint,
        backend_generation: Option<BackendGeneration>,
    },
    StillWaking {
        instance_id: InstanceId,
        generation: Generation,
    },
    Unavailable {
        instance_id: InstanceId,
        generation: Generation,
        reason: String,
    },
    GenerationConflict {
        instance_id: InstanceId,
        expected_generation: Generation,
        actual_generation: Generation,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyBackend {
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    /// The Service address.
    pub backend: BackendEndpoint,
    /// The Pod address, which HTTP forwarding tries before the Service.
    pub pod_backend: Option<BackendEndpoint>,
    pub backend_generation: Option<BackendGeneration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WakeUnavailable {
    pub instance_id: InstanceId,
    pub generation: Generation,
    pub reason: WakeUnavailableReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeUnavailableReason {
    Deleting,
    Deleted,
}

/// What a request does with a selected route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteDisposition {
    /// Forward to the backend.
    Ready(ReadyBackend),
    /// Wait for the instance to run. `wake` asks the control plane to start
    /// it; an instance that is already waking needs no call.
    Pending {
        instance_id: InstanceId,
        generation: Generation,
        wake: bool,
    },
    /// The instance is going away and cannot serve.
    Unavailable(WakeUnavailable),
}

pub fn route_disposition(entry: &RouteEntry) -> RouteDisposition {
    let pending = |wake| RouteDisposition::Pending {
        instance_id: entry.instance_id.clone(),
        generation: entry.instance_generation,
        wake,
    };
    let unavailable = |reason| {
        RouteDisposition::Unavailable(WakeUnavailable {
            instance_id: entry.instance_id.clone(),
            generation: entry.instance_generation,
            reason,
        })
    };
    match entry.instance_state {
        InstanceState::Running => match &entry.backend {
            Some(backend) => RouteDisposition::Ready(ReadyBackend {
                instance_id: entry.instance_id.clone(),
                instance_generation: entry.instance_generation,
                backend: backend.clone(),
                pod_backend: entry.pod_backend.clone(),
                backend_generation: entry.backend_generation,
            }),
            None => pending(true),
        },
        InstanceState::Cold | InstanceState::Failed | InstanceState::Draining => pending(true),
        InstanceState::Waking => pending(false),
        InstanceState::Deleting => unavailable(WakeUnavailableReason::Deleting),
        InstanceState::Deleted => unavailable(WakeUnavailableReason::Deleted),
    }
}

#[cfg(test)]
mod tests {
    use sleepypods_api::{
        BackendEndpoint, BackendGeneration, Generation, InstanceId, InstanceState, RouteBindingId,
        RouteEntry,
    };

    use super::{route_disposition, RouteDisposition, WakeUnavailableReason};

    fn entry(state: InstanceState, backend: bool) -> RouteEntry {
        RouteEntry {
            route_binding_id: RouteBindingId::new("route").unwrap(),
            instance_id: InstanceId::new("instance").unwrap(),
            instance_state: state,
            instance_generation: Generation::new(7),
            backend: backend.then(|| BackendEndpoint::new("http://10.0.0.1:8080").unwrap()),
            pod_backend: backend.then(|| BackendEndpoint::new("http://10.244.0.7:15000").unwrap()),
            backend_generation: backend.then(|| BackendGeneration::new(3)),
        }
    }

    #[test]
    fn running_with_a_backend_is_ready() {
        match route_disposition(&entry(InstanceState::Running, true)) {
            RouteDisposition::Ready(ready) => {
                assert_eq!(ready.backend.uri(), "http://10.0.0.1:8080");
                assert_eq!(
                    ready.pod_backend.as_ref().map(BackendEndpoint::uri),
                    Some("http://10.244.0.7:15000")
                );
                assert_eq!(ready.instance_generation, Generation::new(7));
                assert_eq!(ready.backend_generation, Some(BackendGeneration::new(3)));
            }
            other => panic!("expected ready, got {other:?}"),
        }
    }

    #[test]
    fn states_that_can_run_again_wait_and_ask_for_a_wake() {
        for (state, backend) in [
            (InstanceState::Cold, false),
            (InstanceState::Failed, false),
            (InstanceState::Draining, true),
            (InstanceState::Running, false),
        ] {
            assert!(
                matches!(
                    route_disposition(&entry(state, backend)),
                    RouteDisposition::Pending { wake: true, .. }
                ),
                "{state:?}"
            );
        }
    }

    #[test]
    fn a_waking_instance_waits_without_another_wake() {
        assert!(matches!(
            route_disposition(&entry(InstanceState::Waking, false)),
            RouteDisposition::Pending { wake: false, .. }
        ));
    }

    #[test]
    fn deleting_and_deleted_instances_are_unavailable() {
        for (state, reason) in [
            (InstanceState::Deleting, WakeUnavailableReason::Deleting),
            (InstanceState::Deleted, WakeUnavailableReason::Deleted),
        ] {
            match route_disposition(&entry(state, false)) {
                RouteDisposition::Unavailable(unavailable) => {
                    assert_eq!(unavailable.reason, reason)
                }
                other => panic!("expected unavailable, got {other:?}"),
            }
        }
    }
}
