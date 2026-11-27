//! Membership fixture observation follows the already accepted operation's
//! deadline. It never retries Delete or enqueues reconciliation.
use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient, DeleteInstanceRequest,
    GetInstanceRequest, Instance, InstanceState, ReconcileMaterializationRequest,
};
use std::{
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{timeout_at, Instant};
use tonic::{transport::Channel, Code};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub async fn delete_with_original_deadline(
    operator: &mut OperatorControlPlaneClient<Channel>,
    current: &Instance,
    materialization_id: &str,
    maximum: Duration,
) -> TestResult<()> {
    let expected_generation = current
        .generation
        .checked_add(1)
        .ok_or("deletion generation overflow")?;
    let cap = Instant::now() + maximum;
    let accepted = timeout_at(
        cap,
        operator.delete_instance(DeleteInstanceRequest {
            instance_id: current.instance_id.clone(),
            expected_generation: Some(current.generation),
        }),
    )
    .await??
    .into_inner();
    if Instant::now() >= cap {
        return Err("membership Delete response exceeded observation cap".into());
    }
    if !accepted.accepted {
        return Err("membership Delete was not accepted".into());
    }

    // Pin once after acceptance. Status-only does not enqueue work, and its
    // inspection latency consumes the same cap that began before Delete.
    let status = timeout_at(
        cap,
        operator.reconcile_materialization(ReconcileMaterializationRequest {
            instance_id: current.instance_id.clone(),
            status_only: true,
        }),
    )
    .await??
    .into_inner();
    if Instant::now() >= cap {
        return Err("membership status response exceeded observation cap".into());
    }
    if status.materialization_id != materialization_id || status.attempted {
        return Err(format!("membership status identity/action changed: {status:?}").into());
    }
    if status.found && status.state != "Deleting" && status.state != "Deleted" {
        return Err(format!("membership cleanup has unexpected state: {status:?}").into());
    }
    // The status API reads the materialization and its work status separately.
    // Cascade may remove the latter after the former was found. A zero deadline
    // in this valid deletion shape still requires authoritative instance absence.
    if !status.found || status.operation_deadline_unix_millis == 0 {
        return match timeout_at(
            cap,
            operator.get_instance(GetInstanceRequest {
                instance_id: current.instance_id.clone(),
            }),
        )
        .await?
        {
            Err(error) if error.code() == Code::NotFound && Instant::now() < cap => Ok(()),
            other => Err(format!(
                "materialization missing without confirmed instance deletion: {other:?}"
            )
            .into()),
        };
    }
    let sampled_at = Instant::now();
    let published = Duration::from_millis(
        u64::try_from(status.operation_deadline_unix_millis)
            .map_err(|_| format!("membership operation deadline is invalid: {status:?}"))?,
    );
    let remaining = published
        .checked_sub(SystemTime::now().duration_since(UNIX_EPOCH)?)
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| format!("membership operation deadline missing or elapsed: {status:?}"))?;
    // Clamp the duration before adding it: even a far-future response cannot
    // overflow Instant or extend the local cap established before Delete.
    let deadline = sampled_at + remaining.min(cap.saturating_duration_since(sampled_at));
    loop {
        if Instant::now() >= deadline {
            return Err(format!(
                "membership deletion exceeded its original deadline; pinned status: {status:?}"
            )
            .into());
        }
        let result = timeout_at(deadline, operator.get_instance(GetInstanceRequest {
            instance_id: current.instance_id.clone(),
        })).await.map_err(|error| format!("membership GetInstance exceeded original deadline: {error}; pinned status: {status:?}"))?;
        if Instant::now() >= deadline {
            return Err(format!("membership deletion response arrived after original deadline; pinned status: {status:?}").into());
        }
        match result {
            Err(error) if error.code() == Code::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
            Ok(response) => {
                let instance = response.into_inner();
                if instance.instance_id != current.instance_id
                    || instance.generation != expected_generation
                    || instance.state != InstanceState::Deleting as i32
                {
                    return Err(format!(
                        "membership deletion changed identity/state: {instance:?}"
                    )
                    .into());
                }
            }
        }
        // A live cleanup lease is normal. Stuck work fails at this original
        // deadline, not an arbitrary earlier sample.
        tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(100))).await;
    }
}
