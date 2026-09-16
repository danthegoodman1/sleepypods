# Adversarial review evidence

Working files from the adversarial security review. They sit here as a record
rather than as code: none of them compiles against the current tree, and the
findings they cover are either closed or tracked in the live test suite.

`crates/control-plane/tests/adversarial_repro.rs` carries the reproductions that
still run.

## Files

| File | Finding | State |
| --- | --- | --- |
| `advsec-attack-harness.rs` | F1, F2c, F4 | A kind harness that drove the deployed API. Written as `crates/control-plane/examples/advsec_attack.rs`. |
| `sidecar-token-module.rs` | F1 | Per-instance HMAC credentials for the sidecar, proposed as `crates/control-plane/src/sidecar_token.rs`. |
| `sidecar-impersonation-repro.rs` | F1 | Regression coverage written against that module. |

## Findings

**F1, sidecar impersonation — closed.** A sidecar could report any instance
idle. The two `sidecar-*` files here mint a credential naming one instance. The
platform took a different route: `idle.rs` requires a `pod_uid`, and
`kube_materializer/idle_membership.rs` checks it against the single observed pod
for the instance, so a workload can only put its own instance to sleep.

**F2b, instance values becoming YAML structure — closed.** Raw manifests parse
before their values arrive, so a value lands inside one scalar. See
`crates/control-plane/src/manifest/bind.rs`.

**F2c, hostPath escape — closed.** A hostPath starts with a literal absolute
directory and no rendered segment may be `..`.

**F3, HTTP-01 on the operator surface — closed.** Serving an ACME challenge is a
proxy read.

**F4, one listener carries three roles — open.** `runtime.rs` adds the operator,
proxy, and sidecar services to a single `Server`, so a workload pod that must
reach the sidecar API can also reach the operator API. Distinct per-role tokens
(`auth.rs` rejects duplicates) mean a workload holds no operator credential, so
this is reachability rather than access.

**F6, pod hardening — closed.** A rendered pod drops its ServiceAccount token,
runs under the runtime's default seccomp profile, and gains no privileges.

**F7, value schema — closed.** An instance value stays printable text within a
length bound.
