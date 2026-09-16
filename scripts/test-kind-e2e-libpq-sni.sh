#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/kind-image.sh"
source "${repo_root}/scripts/lib/native-tls-fixture.sh"
cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-e2e-libpq-sni-test}"
namespace="${SLEEPYPODS_KIND_E2E_NAMESPACE:-sleepypods-e2e-libpq-sni}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
keep_namespace="${SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE:-0}"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-kind-e2e-libpq-sni}"
postgres_image="${SLEEPYPODS_KIND_E2E_POSTGRES_IMAGE:-postgres:17.5-alpine3.22}"
libpq_sni_image="${SLEEPYPODS_KIND_E2E_LIBPQ_SNI_IMAGE:-${image_prefix}/libpq-sni-postgres:${image_tag}}"
operator_port="${SLEEPYPODS_KIND_E2E_OPERATOR_PORT:-19751}"
route_host="${SLEEPYPODS_KIND_E2E_LIBPQ_SNI_HOST:-exact.sni.sleepypods.test}"
miss_host="${SLEEPYPODS_KIND_E2E_LIBPQ_SNI_MISS_HOST:-passthrough.sleepypods.test}"
# SHA-256(e2e-libpq-sni-postgres), first eight hex digits.
workload_name="e2e-libpq-sni-postgres-56db3511"
kubeconfig="$(mktemp)"
security_dir="$(mktemp -d)"
artifact_dir="${SLEEPYPODS_E2E_ARTIFACT_DIR:-${repo_root}/.generated/test-kind-e2e-libpq-sni-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${artifact_dir}"
namespace_created=0
control_plane_pf_log="$(mktemp)"
created_cluster=0
control_plane_pf=""

require_command() {
  local name="$1"

  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "${name} is required for the libpq-SNI kind E2E" >&2
    exit 127
  fi
}

stop_port_forwards() {
  if [[ -n "${control_plane_pf}" ]]; then
    kill "${control_plane_pf}" >/dev/null 2>&1 || true
    wait "${control_plane_pf}" 2>/dev/null || true
    control_plane_pf=""
  fi
}

port_forward_loop() {
  local name="$1"
  local log="$2"
  shift 2
  local child=""
  local status=0

  trap 'if [[ -n "${child}" ]]; then kill "${child}" >/dev/null 2>&1 || true; wait "${child}" 2>/dev/null || true; fi; exit 0' TERM INT

  while true; do
    printf '==> Starting %s port-forward: kubectl %s\n' "${name}" "$*" >>"${log}"
    KUBECONFIG="${kubeconfig}" kubectl "$@" >>"${log}" 2>&1 &
    child=$!
    wait "${child}" || status=$?
    child=""
    printf '==> %s port-forward exited with status %s; restarting\n' "${name}" "${status}" >>"${log}"
    status=0

    sleep 1 &
    child=$!
    wait "${child}" || exit 0
    child=""
  done
}

wait_for_local_port() {
  local name="$1"
  local port="$2"
  local supervisor_pid="$3"
  local log="$4"
  local deadline=$((SECONDS + 30))

  while ((SECONDS < deadline)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; then
      return 0
    fi

    if ! kill -0 "${supervisor_pid}" >/dev/null 2>&1; then
      echo "${name} port-forward supervisor exited before local port ${port} became ready" >&2
      tail -n 80 "${log}" >&2 || true
      return 1
    fi

    sleep 1
  done

  echo "timed out waiting for ${name} port-forward on 127.0.0.1:${port}" >&2
  tail -n 80 "${log}" >&2 || true
  return 1
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
  if [[ "${status}" != "0" && -s "${control_plane_pf_log}" ]]; then
    echo "==> Last control-plane port-forward log lines (${control_plane_pf_log})" >&2
    tail -n 80 "${control_plane_pf_log}" >&2 || true
  fi

  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}" || true
  elif [[ "${keep_namespace}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi

  rm -f "${kubeconfig}" "${control_plane_pf_log}"
  rm -rf -- "${security_dir}"
  exit "${status}"
}
trap cleanup EXIT

wait_for_job() {
  local job="$1"
  local timeout="$2"

  if ! KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" wait \
    --for=condition=complete \
    --timeout="${timeout}" \
    "job/${job}"; then
    echo "==> Logs for failed job/${job}" >&2
    KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" logs "job/${job}" --all-containers=true >&2 || true
    return 1
  fi

  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" logs "job/${job}" --all-containers=true
}

assert_jsonpath() {
  local description="$1"
  local expected="$2"
  shift 2
  local actual

  actual="$(KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" get "$@")"
  if [[ "${actual}" != "${expected}" ]]; then
    printf 'expected %s to be %q, got %q\n' "${description}" "${expected}" "${actual}" >&2
    return 1
  fi
}

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

echo "==> Pulling pinned Postgres/libpq image ${postgres_image}"
docker pull "${postgres_image}"

echo "==> Building ${libpq_sni_image} from ${postgres_image}"
docker build \
  --build-arg "POSTGRES_BASE_IMAGE=${postgres_image}" \
  --tag "${libpq_sni_image}" \
  --file "${repo_root}/scripts/Dockerfile.kind-libpq-sni-postgres" \
  "${repo_root}"

fi

for image in \
  "${image_prefix}/control-plane:${image_tag}" \
  "${image_prefix}/frontline:${image_tag}" \
  "${image_prefix}/sidecar:${image_tag}" \
  "${postgres_image}" \
  "${libpq_sni_image}"; do
  echo "==> Loading ${image} into kind/${cluster_name}"
  kind_load_image "${cluster_name}" "${image}"
done

echo "==> Recreating namespace ${namespace}"
KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true
KUBECONFIG="${kubeconfig}" kubectl create namespace "${namespace}"
namespace_created=1
KUBECONFIG="${kubeconfig}" kubectl label namespace "${namespace}" \
  sleepypods.io/kind-e2e=libpq-sni --overwrite

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

echo "==> Deploying Postgres store"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: libpq-sni
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-postgres
        sleepypods.io/kind-e2e: libpq-sni
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
    sleepypods.io/kind-e2e: libpq-sni
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
    sleepypods.io/kind-e2e: libpq-sni
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-frontline
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-frontline
        sleepypods.io/kind-e2e: libpq-sni
    spec:
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
          readinessProbe:
            tcpSocket:
              port: http
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_FRONTLINE_LISTEN_ADDR
              value: 0.0.0.0:8080
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
    sleepypods.io/kind-e2e: libpq-sni
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

echo "==> Deploying control-plane"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: libpq-sni
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: libpq-sni
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
    sleepypods.io/kind-e2e: libpq-sni
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
    sleepypods.io/kind-e2e: libpq-sni
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-control-plane
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-control-plane
        sleepypods.io/kind-e2e: libpq-sni
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
              value: kind-e2e-libpq-sni
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
    sleepypods.io/kind-e2e: libpq-sni
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
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-frontline --timeout=180s

echo "==> Starting local control-plane port-forward"
port_forward_loop control-plane "${control_plane_pf_log}" -n "${namespace}" port-forward \
  svc/sleepypods-control-plane "${operator_port}:50053" &
control_plane_pf=$!
wait_for_local_port control-plane "${operator_port}" "${control_plane_pf}" "${control_plane_pf_log}"

echo "==> Creating libpq-SNI operator resources"
KUBECONFIG="${kubeconfig}" \
  SLEEPYPODS_KIND_E2E_LIBPQ_SNI=1 \
  SLEEPYPODS_E2E_NAMESPACE="${namespace}" \
  SLEEPYPODS_E2E_OPERATOR_ENDPOINT="https://localhost:${operator_port}" \
    SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM="${platform_ca}" \
    SLEEPYPODS_E2E_OPERATOR_TOKEN="${operator_token}" \
    SLEEPYPODS_E2E_ARTIFACT_DIR="${artifact_dir}" \
  SLEEPYPODS_E2E_LIBPQ_SNI_IMAGE="${libpq_sni_image}" \
  SLEEPYPODS_E2E_SIDECAR_IMAGE="${image_prefix}/sidecar:${image_tag}" \
  SLEEPYPODS_E2E_LIBPQ_SNI_HOST="${route_host}" \
  cargo test -p control-plane --test kind_e2e_libpq_sni -- --ignored --nocapture

frontline_ip="$(KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" get svc sleepypods-frontline -o jsonpath='{.spec.clusterIP}')"
if [[ -z "${frontline_ip}" || "${frontline_ip}" == "None" ]]; then
  echo "sleepypods-frontline service did not have a ClusterIP: ${frontline_ip}" >&2
  exit 1
fi

echo "==> Running positive libpq direct-SNI client job"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: libpq-sni-positive
  labels:
    sleepypods.io/kind-e2e: libpq-sni
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 240
  template:
    metadata:
      labels:
        sleepypods.io/kind-e2e: libpq-sni
    spec:
      restartPolicy: Never
      containers:
        - name: psql
          image: ${libpq_sni_image}
          imagePullPolicy: IfNotPresent
          env:
            - name: PGPASSWORD
              value: libpq_sni_password
            - name: ROUTE_HOST
              value: ${route_host}
            - name: FRONTLINE_IP
              value: ${frontline_ip}
          command:
            - /bin/sh
            - -ceu
            - |
              # The operator fixture asserted Cold. One libpq connection must survive wake.
              conninfo="host=\${ROUTE_HOST} hostaddr=\${FRONTLINE_IP} port=9443 dbname=libpq_sni user=libpq_sni sslmode=verify-full sslrootcert=/etc/postgresql/tls/tls.crt sslnegotiation=direct connect_timeout=140"
              marker="\$(psql "\${conninfo}" -Atv ON_ERROR_STOP=1 -c "select value from kind_e2e_marker")"
              if [ "\${marker}" != "sleepypods-libpq-sni" ]; then
                echo "expected marker sleepypods-libpq-sni, got: \${marker}" >&2
                exit 1
              fi
              echo "libpq direct-SNI query returned \${marker}"
YAML
wait_for_job libpq-sni-positive 240s

echo "==> Checking materialized SleepyPods TCP workload"
assert_jsonpath "workload app image" "${libpq_sni_image}" deployment "${workload_name}" -o jsonpath='{.spec.template.spec.containers[?(@.name=="postgres")].image}'
assert_jsonpath "workload sidecar image" "${image_prefix}/sidecar:${image_tag}" deployment "${workload_name}" -o jsonpath='{.spec.template.spec.containers[?(@.name=="sleepypods-sidecar")].image}'
assert_jsonpath "workload service targetPort" "15000" service "${workload_name}" -o jsonpath='{.spec.ports[0].targetPort}'
assert_jsonpath "workload backend scheme" "tcp" service "${workload_name}" -o jsonpath='{.metadata.annotations.sleepypods\.io/backend-scheme}'

echo "==> Running negative unbound-SNI client job"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: libpq-sni-negative
  labels:
    sleepypods.io/kind-e2e: libpq-sni
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 60
  template:
    metadata:
      labels:
        sleepypods.io/kind-e2e: libpq-sni
    spec:
      restartPolicy: Never
      containers:
        - name: psql
          image: ${libpq_sni_image}
          imagePullPolicy: IfNotPresent
          env:
            - name: PGPASSWORD
              value: libpq_sni_password
            - name: MISS_HOST
              value: ${miss_host}
            - name: FRONTLINE_IP
              value: ${frontline_ip}
          command:
            - /bin/sh
            - -ceu
            - |
              conninfo="host=\${MISS_HOST} hostaddr=\${FRONTLINE_IP} port=9443 dbname=libpq_sni user=libpq_sni sslmode=verify-full sslrootcert=/etc/postgresql/tls/tls.crt sslnegotiation=direct connect_timeout=5"
              if psql "\${conninfo}" -Atv ON_ERROR_STOP=1 -c "select 1"; then
                echo "unbound SNI host unexpectedly reached Postgres" >&2
                exit 1
              fi
              echo "unbound SNI host was rejected as expected"
YAML
wait_for_job libpq-sni-negative 60s

echo "libpq-SNI full-platform kind E2E completed"
