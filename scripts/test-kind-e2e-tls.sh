#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/kind-image.sh"
source "${repo_root}/scripts/lib/native-tls-fixture.sh"
cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-e2e-tls-test}"
namespace="${SLEEPYPODS_KIND_E2E_NAMESPACE:-sleepypods-e2e-tls}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
keep_namespace="${SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE:-0}"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-kind-e2e-tls}"
app_image="${SLEEPYPODS_KIND_E2E_APP_IMAGE:-${image_prefix}/tls-app:${image_tag}}"
protocol_app_image="${SLEEPYPODS_KIND_E2E_PROTOCOL_APP_IMAGE:-${image_prefix}/protocol-app:${image_tag}}"
postgres_image="${SLEEPYPODS_KIND_E2E_POSTGRES_IMAGE:-postgres:17-alpine}"
operator_port="${SLEEPYPODS_KIND_E2E_OPERATOR_PORT:-19351}"
tls_termination_port="${SLEEPYPODS_KIND_E2E_TLS_TERMINATION_PORT:-19443}"
tls_passthrough_port="${SLEEPYPODS_KIND_E2E_TLS_PASSTHROUGH_PORT:-19444}"
kubeconfig="$(mktemp)"
security_dir="$(mktemp -d)"
artifact_dir="${SLEEPYPODS_E2E_ARTIFACT_DIR:-${repo_root}/.generated/test-kind-e2e-tls-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${artifact_dir}"
namespace_created=0
control_plane_pf_log="$(mktemp)"
frontline_pf_log="$(mktemp)"
created_cluster=0
control_plane_pf=""
frontline_pf=""

require_command() {
  local name="$1"

  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "${name} is required for the TLS kind E2E" >&2
    exit 127
  fi
}

stop_port_forwards() {
  if [[ -n "${control_plane_pf}" ]]; then
    kill "${control_plane_pf}" >/dev/null 2>&1 || true
    wait "${control_plane_pf}" 2>/dev/null || true
    control_plane_pf=""
  fi
  if [[ -n "${frontline_pf}" ]]; then
    kill "${frontline_pf}" >/dev/null 2>&1 || true
    wait "${frontline_pf}" 2>/dev/null || true
    frontline_pf=""
  fi
}

capture_failure_details() {
  # Only this fixture's public status and bounded logs; never Pod specs/env.
  python3 - "${kubeconfig}" "${namespace}" "${artifact_dir}" <<'PY'
import json, pathlib, subprocess, sys
kubeconfig, namespace, directory = sys.argv[1:]
directory = pathlib.Path(directory)
pods = json.loads((directory / "pod-identities.json").read_text())
if len(pods) > 8:
    raise SystemExit("unexpected fixture pod inventory; skipping diagnostic collection")
for pod in pods:
    name = pod["name"]
    commands = [
        ("status", ["get", "pod", name, "-o=jsonpath={.metadata.uid}{'\\n'}{.status}{'\\n'}"]),
        ("current", ["logs", name, "--all-containers=true", "--timestamps", "--tail=80", "--limit-bytes=16384"]),
        ("previous", ["logs", name, "--all-containers=true", "--previous", "--timestamps", "--tail=80", "--limit-bytes=16384"]),
        ("events", ["get", "events", "--field-selector", f"involvedObject.name={name}", "-o=custom-columns=TIME:.lastTimestamp,TYPE:.type,REASON:.reason,MESSAGE:.message", "--no-headers"]),
    ]
    for label, arguments in commands:
        path = directory / f"pod-{name}-{label}.log"
        with path.open("w+b") as output:
            try:
                result = subprocess.run(
                    ["kubectl", "--kubeconfig", kubeconfig, "--request-timeout=3s", "-n", namespace, *arguments],
                    stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, timeout=4,
                )
                output.write(f"\ncollector_exit={result.returncode}\n".encode())
            except subprocess.TimeoutExpired:
                output.write(b"\ncollector_timeout=4s\n")
            length = output.tell()
            output.seek(max(0, length - 65536))
            tail = output.read(65536)
            output.seek(0)
            output.write(tail)
            output.truncate()
PY
}

cleanup() {
  local status=$?

  stop_port_forwards
  if [[ "${namespace_created}" == "1" ]]; then
    if ! capture_native_tls_pods "${kubeconfig}" "${namespace}" "${artifact_dir}/pod-identities.json"; then
      echo "failed to retain exact deployed pod identities" >&2
      if [[ "${status}" == "0" ]]; then status=1; fi
    fi
  fi
  if [[ "${status}" != "0" ]]; then
    if [[ "${namespace_created}" == "1" ]]; then
      capture_failure_details || echo "failed to retain bounded fixture failure details" >&2
    fi
    if [[ -s "${control_plane_pf_log}" ]]; then
      echo "==> Last control-plane port-forward log lines (${control_plane_pf_log})" >&2
      tail -n 80 "${control_plane_pf_log}" >&2 || true
    fi
    if [[ -s "${frontline_pf_log}" ]]; then
      echo "==> Last frontline port-forward log lines (${frontline_pf_log})" >&2
      tail -n 80 "${frontline_pf_log}" >&2 || true
    fi
  fi

  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}" || true
  elif [[ "${keep_namespace}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi

  rm -f "${kubeconfig}" "${control_plane_pf_log}" "${frontline_pf_log}"
  rm -rf -- "${security_dir}"
  exit "${status}"
}
trap cleanup EXIT

for command in kind kubectl docker cargo openssl python3; do
  require_command "${command}"
done

if ! kind get clusters | grep -Fxq "${cluster_name}"; then
  created_cluster=1
  KUBECONFIG="${kubeconfig}" kind create cluster --name "${cluster_name}" --wait 120s
else
  kind get kubeconfig --name "${cluster_name}" >"${kubeconfig}"
fi

if [[ "${SLEEPYPODS_KIND_E2E_SKIP_BUILD:-0}" != "1" ]]; then
components=(control-plane frontline sidecar)
for component in "${components[@]}"; do
  image="${image_prefix}/${component}:${image_tag}"
  echo "==> Building ${image}"
  docker build \
    --build-arg "BIN=${component}" \
    --tag "${image}" \
    --file "${repo_root}/Dockerfile" \
    "${repo_root}"
done

echo "==> Building ${app_image}"
docker build \
  --tag "${app_image}" \
  --file "${repo_root}/scripts/Dockerfile.kind-tls-app" \
  "${repo_root}"

echo "==> Building ${protocol_app_image}"
docker build \
  --tag "${protocol_app_image}" \
  --file "${repo_root}/scripts/Dockerfile.kind-protocol-app" \
  "${repo_root}"

echo "==> Pulling ${postgres_image}"
docker pull "${postgres_image}"

fi

for image in \
  "${image_prefix}/control-plane:${image_tag}" \
  "${image_prefix}/frontline:${image_tag}" \
  "${image_prefix}/sidecar:${image_tag}" \
  "${app_image}" \
  "${protocol_app_image}" \
  "${postgres_image}"; do
  echo "==> Loading ${image} into kind/${cluster_name}"
  kind_load_image "${cluster_name}" "${image}"
done

echo "==> Recreating namespace ${namespace}"
KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true
KUBECONFIG="${kubeconfig}" kubectl create namespace "${namespace}"
namespace_created=1
KUBECONFIG="${kubeconfig}" kubectl label namespace "${namespace}" \
  sleepypods.io/kind-e2e=tls --overwrite

generate_native_tls_fixture "${security_dir}/platform" "sleepypods-control-plane.${namespace}.svc.cluster.local"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" create secret generic sleepypods-native-control-plane \
  --from-file="${security_dir}/platform/cp.crt" \
  --from-file="${security_dir}/platform/cp.key" \
  --from-file="${security_dir}/platform/sealing.json" \
  --from-file="${security_dir}/platform/operator.token" \
  --from-file="${security_dir}/platform/proxy.token" \
  --from-file="${security_dir}/platform/sidecar.token"
platform_ca="$(cat "${security_dir}/platform/cp.crt")"
operator_token="$(cat "${security_dir}/platform/operator.token")"

echo "==> Creating backend-only passthrough TLS fixture secret"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" create secret generic sleepypods-kind-e2e-tls \
  --from-file=tls.crt="${repo_root}/scripts/kind-e2e-tls-cert.pem" \
  --from-file=tls.key="${repo_root}/scripts/kind-e2e-tls-key.pem" \
  --dry-run=client \
  -o yaml | KUBECONFIG="${kubeconfig}" kubectl apply -f -

echo "==> Deploying Postgres"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: tls
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-postgres
        sleepypods.io/kind-e2e: tls
    spec:
      containers:
        - name: postgres
          image: ${postgres_image}
          imagePullPolicy: IfNotPresent
          ports:
            - name: postgres
              containerPort: 5432
          env:
            - name: POSTGRES_USER
              value: sleepypods
            - name: POSTGRES_PASSWORD
              value: sleepypods
            - name: POSTGRES_DB
              value: sleepypods
          readinessProbe:
            exec:
              command:
                - pg_isready
                - -U
                - sleepypods
                - -d
                - sleepypods
            initialDelaySeconds: 2
            periodSeconds: 2
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: tls
spec:
  selector:
    app.kubernetes.io/name: sleepypods-postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
YAML
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-postgres --timeout=180s

echo "==> Deploying frontline"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: tls
spec:
  replicas: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-frontline
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-frontline
        sleepypods.io/kind-e2e: tls
    spec:
      automountServiceAccountToken: false
      containers:
        - name: frontline
          image: ${image_prefix}/frontline:${image_tag}
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8080
            - name: tls-term
              containerPort: 8443
            - name: tls-pass
              containerPort: 9443
            - name: metrics
              containerPort: 9090
          readinessProbe:
            tcpSocket:
              port: http
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_FRONTLINE_LISTEN_ADDR
              value: 0.0.0.0:8080
            - name: SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR
              value: 0.0.0.0:9090
            - name: SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR
              value: 0.0.0.0:8443
            - name: SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR
              value: 0.0.0.0:9443
            - name: SLEEPYPODS_CONTROL_PLANE_ENDPOINT
              value: https://sleepypods-control-plane.${namespace}.svc.cluster.local:50051
            - name: SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM
              valueFrom:
                secretKeyRef: {name: sleepypods-native-control-plane, key: cp.crt}
            - name: SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN
              valueFrom:
                secretKeyRef: {name: sleepypods-native-control-plane, key: proxy.token}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: tls
spec:
  selector:
    app.kubernetes.io/name: sleepypods-frontline
  ports:
    - name: http
      port: 8080
      targetPort: 8080
    - name: tls-term
      port: 8443
      targetPort: 8443
    - name: tls-pass
      port: 9443
      targetPort: 9443
YAML

deploy_control_plane() {
  echo "==> Deploying control-plane"
  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: tls
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: tls
rules:
  - apiGroups: [""]
    resources: ["services", "secrets"]
    verbs: ["get", "list", "watch", "patch", "create", "update", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get", "list", "watch", "patch", "create", "update", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: ["apps"]
    resources: ["replicasets"]
    verbs: ["get", "list"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: tls
subjects:
  - kind: ServiceAccount
    name: sleepypods-control-plane
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: sleepypods-control-plane
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: tls
spec:
  replicas: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-control-plane
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-control-plane
        sleepypods.io/kind-e2e: tls
    spec:
      serviceAccountName: sleepypods-control-plane
      containers:
        - name: control-plane
          image: ${image_prefix}/control-plane:${image_tag}
          imagePullPolicy: IfNotPresent
          ports:
            - name: grpc
              containerPort: 50051
            - name: operator
              containerPort: 50053
          readinessProbe:
            tcpSocket:
              port: grpc
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR
              value: 0.0.0.0:50051
            - name: SLEEPYPODS_CONTROL_PLANE_OPERATOR_LISTEN_ADDR
              value: 0.0.0.0:50053
            - name: SLEEPYPODS_CONTROL_PLANE_AUTH_MODE
              value: static-bearer-token
            - name: SLEEPYPODS_CONTROL_PLANE_TLS_CERT_FILE
              value: /etc/sleepypods/platform/cp.crt
            - name: SLEEPYPODS_CONTROL_PLANE_TLS_KEY_FILE
              value: /etc/sleepypods/platform/cp.key
            - name: SLEEPYPODS_CERTIFICATE_SEALING_KEYS_FILE
              value: /etc/sleepypods/platform/sealing.json
            - name: SLEEPYPODS_CONTROL_PLANE_PUBLIC_ENDPOINT
              value: https://sleepypods-control-plane.${namespace}.svc.cluster.local:50051
            - name: SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM
              valueFrom:
                secretKeyRef: {name: sleepypods-native-control-plane, key: cp.crt}
            - name: SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN
              valueFrom:
                secretKeyRef: {name: sleepypods-native-control-plane, key: operator.token}
            - name: SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN
              valueFrom:
                secretKeyRef: {name: sleepypods-native-control-plane, key: proxy.token}
            - name: SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN
              valueFrom:
                secretKeyRef: {name: sleepypods-native-control-plane, key: sidecar.token}
            - name: SLEEPYPODS_STORE_PROVIDER
              value: postgres
            - name: SLEEPYPODS_POSTGRES_URL
              value: postgres://sleepypods:sleepypods@sleepypods-postgres:5432/sleepypods
            - name: SLEEPYPODS_CLUSTER_ID
              value: kind-e2e-tls
            - name: SLEEPYPODS_NAMESPACE
              value: ${namespace}
          volumeMounts:
            - name: platform
              mountPath: /etc/sleepypods/platform
              readOnly: true
      volumes:
        - name: platform
          secret:
            secretName: sleepypods-native-control-plane
            items:
              - {key: cp.crt, path: cp.crt}
              - {key: cp.key, path: cp.key}
              - {key: sealing.json, path: sealing.json}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: tls
spec:
  selector:
    app.kubernetes.io/name: sleepypods-control-plane
  ports:
    - name: grpc
      port: 50051
      targetPort: 50051
    - name: operator
      port: 50053
      targetPort: 50053
YAML

  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-control-plane --timeout=180s
}

start_port_forwards() {
  stop_port_forwards

  echo "==> Starting local port-forwards"
  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" port-forward \
    svc/sleepypods-control-plane "${operator_port}:50053" >"${control_plane_pf_log}" 2>&1 &
  control_plane_pf=$!
  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" port-forward \
    svc/sleepypods-frontline "${tls_termination_port}:8443" "${tls_passthrough_port}:9443" >"${frontline_pf_log}" 2>&1 &
  frontline_pf=$!
}

run_tls_driver() {
  local test_name="$1"

  echo "==> Running TLS kind E2E driver ${test_name}"
  KUBECONFIG="${kubeconfig}" \
    SLEEPYPODS_KIND_E2E_TLS=1 \
    SLEEPYPODS_E2E_NAMESPACE="${namespace}" \
    SLEEPYPODS_E2E_OPERATOR_ENDPOINT="https://localhost:${operator_port}" \
    SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM="${platform_ca}" \
    SLEEPYPODS_E2E_OPERATOR_TOKEN="${operator_token}" \
    SLEEPYPODS_E2E_ARTIFACT_DIR="${artifact_dir}" \
    SLEEPYPODS_E2E_TLS_TERMINATION_ADDR="127.0.0.1:${tls_termination_port}" \
    SLEEPYPODS_E2E_TLS_PASSTHROUGH_ADDR="127.0.0.1:${tls_passthrough_port}" \
    SLEEPYPODS_E2E_APP_IMAGE="${app_image}" \
    SLEEPYPODS_E2E_PROTOCOL_APP_IMAGE="${protocol_app_image}" \
    SLEEPYPODS_E2E_SIDECAR_IMAGE="${image_prefix}/sidecar:${image_tag}" \
    cargo test -p control-plane --test kind_e2e_tls "${test_name}" -- --ignored --nocapture
}

deploy_control_plane
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-frontline --timeout=180s
start_port_forwards
run_tls_driver tls_termination_through_deployed_platform
run_tls_driver sni_passthrough_through_deployed_platform
run_tls_driver dynamic_certificate_lifecycle_through_exact_replicas

echo "TLS full-platform kind E2E completed"
