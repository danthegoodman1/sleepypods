# SleepyPods

SleepyPods is a Rust control plane and proxy system that materializes Kubernetes
workloads on demand. Frontline routes incoming traffic and wakes cold workloads;
a workload-local sidecar proxies application traffic and reports idleness.
Postgres stores durable lifecycle intent so accepted work survives client loss
and controller restart.

The [original validation and independent review](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/final-integration/README.md)
records measured lifecycle/proxy workloads, recovery guarantees and capacity limits.
[Dynamic certificate delivery](docs/dynamic-certificates-plan.md) uses encrypted
Postgres storage, bounded in-memory proxy caches and change notifications for
rotation and removal. Frontline loads application certificates only from the
control plane. The [certificate validation results](https://github.com/danthegoodman1/sleepypods/blob/1a472fd/docs/review-evidence/dynamic-certificates/phase7/README.md)
record the deployed behavior, outage limits and measured resource/performance scope.

Start here:

- [Production north star](docs/production-north-star.md)
- [Development plan](docs/development-plan.md)
- [Dynamic certificate delivery plan](docs/dynamic-certificates-plan.md)
- [Codebase review and remediation plan](docs/review-remediation-plan.md)
- [Improvement plan](docs/improvement-plan.md)
- [Operator guide](docs/operator-guide.md)
- [Operator runbook](docs/operator-runbook.md)
- [Contributor and agent guide](docs/contributor-guide.md)
- [Proxy protocol contract](docs/proxy-protocol-contract.md)
- [Postgres store contract](docs/postgres-store-contract.md)
- [Projection safety](docs/projection-safety.md)
- [Proxy hot-path budgets](docs/proxy-hot-path-budgets.md)

This workspace replaces the Go, DigitalOcean, Terraform and demo-script proof of
concept. Use the operator guide for the Rust deployment and coordinated API/state
upgrade requirements.
