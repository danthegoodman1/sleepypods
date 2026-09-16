#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/kind-image.sh"
cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-e2e-metrics-test}"
namespace="${SLEEPYPODS_KIND_E2E_NAMESPACE:-sleepypods-e2e-metrics}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
keep_namespace="${SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE:-0}"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-kind-e2e-metrics}"
postgres_image="${SLEEPYPODS_KIND_E2E_POSTGRES_IMAGE:-postgres:17-alpine}"
operator_port="${SLEEPYPODS_KIND_E2E_OPERATOR_PORT:-19351}"
frontline_port="${SLEEPYPODS_KIND_E2E_FRONTLINE_PORT:-19380}"
sidecar_port="${SLEEPYPODS_KIND_E2E_SIDECAR_PORT:-19382}"
control_plane_metrics_port="${SLEEPYPODS_KIND_E2E_CONTROL_PLANE_METRICS_PORT:-19390}"
frontline_metrics_port="${SLEEPYPODS_KIND_E2E_FRONTLINE_METRICS_PORT:-19391}"
sidecar_metrics_port="${SLEEPYPODS_KIND_E2E_SIDECAR_METRICS_PORT:-19392}"
kubeconfig="$(mktemp)"
created_cluster=0
port_forward_pids=()
port_forward_logs=()

require_command() {
  local name="$1"

  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "${name} is required for the metrics kind E2E" >&2
    exit 127
  fi
}

cleanup() {
  local status=$?
  local pid
  local log_file

  for pid in "${port_forward_pids[@]}"; do
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" 2>/dev/null || true
  done

  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}" || true
  elif [[ "${keep_namespace}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi

  for log_file in "${port_forward_logs[@]}"; do
    rm -f "${log_file}"
  done
  rm -f "${kubeconfig}"
  exit "${status}"
}
trap cleanup EXIT

start_port_forward() {
  local service="$1"
  local log_file
  shift

  log_file="$(mktemp)"
  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" port-forward \
    "svc/${service}" "$@" >"${log_file}" 2>&1 &
  port_forward_pids+=("$!")
  port_forward_logs+=("${log_file}")
}

curl_status() {
  local url="$1"
  local body_file="$2"
  local error_file="$3"
  local status

  status="$(
    curl --http1.1 --silent --show-error --max-time 5 \
      --output "${body_file}" \
      --write-out "%{http_code}" \
      "${url}" 2>"${error_file}" || true
  )"
  printf '%s' "${status}"
}

scrape_metrics() {
  local name="$1"
  local url="$2"
  local expected="$3"
  local body_file
  local error_file
  local status
  local attempt

  body_file="$(mktemp)"
  error_file="$(mktemp)"
  for ((attempt = 1; attempt <= 60; attempt++)); do
    status="$(curl_status "${url}" "${body_file}" "${error_file}")"
    if [[ "${status}" == "200" ]] && grep -Fq "${expected}" "${body_file}"; then
      rm -f "${body_file}" "${error_file}"
      echo "==> ${name} metrics listener served /metrics"
      return
    fi
    sleep 1
  done

  echo "${name} metrics listener did not expose expected /metrics output from ${url}" >&2
  echo "last HTTP status: ${status}" >&2
  if [[ -s "${error_file}" ]]; then
    sed -n '1,40p' "${error_file}" >&2
  fi
  if [[ -s "${body_file}" ]]; then
    sed -n '1,80p' "${body_file}" >&2
  fi
  rm -f "${body_file}" "${error_file}"
  exit 1
}

assert_metrics_listener_path_is_isolated() {
  local name="$1"
  local url="$2"
  local body_file
  local error_file
  local status

  body_file="$(mktemp)"
  error_file="$(mktemp)"
  status="$(curl_status "${url}" "${body_file}" "${error_file}")"
  if [[ "${status}" != "404" ]]; then
    echo "${name} metrics listener served an unexpected status for a non-metrics path: ${status}" >&2
    sed -n '1,80p' "${body_file}" >&2 || true
    rm -f "${body_file}" "${error_file}"
    exit 1
  fi
  rm -f "${body_file}" "${error_file}"
}

assert_main_listener_does_not_serve_metrics() {
  local name="$1"
  local url="$2"
  local body_file
  local error_file
  local status

  body_file="$(mktemp)"
  error_file="$(mktemp)"
  status="$(curl_status "${url}" "${body_file}" "${error_file}")"
  if grep -Fq "# HELP sleepypods_" "${body_file}" || grep -Eq '^sleepypods_' "${body_file}"; then
    echo "${name} main listener served Prometheus metrics from ${url}; /metrics must stay on the dedicated listener" >&2
    echo "HTTP status: ${status}" >&2
    sed -n '1,80p' "${body_file}" >&2
    rm -f "${body_file}" "${error_file}"
    exit 1
  fi
  rm -f "${body_file}" "${error_file}"
  echo "==> ${name} main listener did not serve /metrics"
}

for command in kind kubectl docker curl; do
  require_command "${command}"
done

if ! kind get clusters | grep -Fxq "${cluster_name}"; then
  created_cluster=1
  KUBECONFIG="${kubeconfig}" kind create cluster --name "${cluster_name}" --wait 120s
else
  kind get kubeconfig --name "${cluster_name}" >"${kubeconfig}"
fi

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

echo "==> Pulling ${postgres_image}"
docker pull "${postgres_image}"

for image in \
  "${image_prefix}/control-plane:${image_tag}" \
  "${image_prefix}/frontline:${image_tag}" \
  "${image_prefix}/sidecar:${image_tag}" \
  "${postgres_image}"; do
  echo "==> Loading ${image} into kind/${cluster_name}"
  kind_load_image "${cluster_name}" "${image}"
done

echo "==> Recreating namespace ${namespace}"
KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true
KUBECONFIG="${kubeconfig}" kubectl create namespace "${namespace}"
KUBECONFIG="${kubeconfig}" kubectl label namespace "${namespace}" \
  sleepypods.io/kind-e2e=metrics --overwrite

echo "==> Deploying Postgres"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: metrics
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-postgres
        sleepypods.io/kind-e2e: metrics
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
    sleepypods.io/kind-e2e: metrics
spec:
  selector:
    app.kubernetes.io/name: sleepypods-postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
YAML
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-postgres --timeout=180s

echo "==> Deploying control-plane with metrics listener"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: metrics
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: metrics
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
    sleepypods.io/kind-e2e: metrics
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
    sleepypods.io/kind-e2e: metrics
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-control-plane
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-control-plane
        sleepypods.io/kind-e2e: metrics
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
            - name: metrics
              containerPort: 19090
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
            - name: SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR
              value: 0.0.0.0:19090
            - name: SLEEPYPODS_CONTROL_PLANE_AUTH_MODE
              value: no-auth
            - name: SLEEPYPODS_STORE_PROVIDER
              value: postgres
            - name: SLEEPYPODS_POSTGRES_URL
              value: postgres://sleepypods:sleepypods@sleepypods-postgres:5432/sleepypods
            - name: SLEEPYPODS_CLUSTER_ID
              value: kind-e2e-metrics
            - name: SLEEPYPODS_NAMESPACE
              value: ${namespace}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: metrics
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
    - name: metrics
      port: 19090
      targetPort: 19090
YAML
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-control-plane --timeout=180s

echo "==> Deploying frontline and sidecar with metrics listeners"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: metrics
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-frontline
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-frontline
        sleepypods.io/kind-e2e: metrics
    spec:
      containers:
        - name: frontline
          image: ${image_prefix}/frontline:${image_tag}
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8080
            - name: metrics
              containerPort: 19091
          readinessProbe:
            tcpSocket:
              port: http
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_FRONTLINE_LISTEN_ADDR
              value: 0.0.0.0:8080
            - name: SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR
              value: 0.0.0.0:19091
            - name: SLEEPYPODS_CONTROL_PLANE_ENDPOINT
              value: http://sleepypods-control-plane.${namespace}.svc.cluster.local:50051
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: metrics
spec:
  selector:
    app.kubernetes.io/name: sleepypods-frontline
  ports:
    - name: http
      port: 8080
      targetPort: 8080
    - name: metrics
      port: 19091
      targetPort: 19091
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-sidecar
  labels:
    app.kubernetes.io/name: sleepypods-sidecar
    sleepypods.io/kind-e2e: metrics
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-sidecar
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-sidecar
        sleepypods.io/kind-e2e: metrics
    spec:
      containers:
        - name: sidecar
          image: ${image_prefix}/sidecar:${image_tag}
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 15000
            - name: metrics
              containerPort: 19092
          readinessProbe:
            tcpSocket:
              port: http
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_SIDECAR_LISTEN_ADDR
              value: 0.0.0.0:15000
            - name: SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR
              value: 0.0.0.0:19092
            - name: SLEEPYPODS_APP_PORT
              value: "8081"
            - name: SLEEPYPODS_INSTANCE_ID
              value: metrics-sidecar
            - name: SLEEPYPODS_POD_UID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.uid
            - name: SLEEPYPODS_INSTANCE_GENERATION
              value: "1"
            - name: SLEEPYPODS_CONTROL_PLANE_ENDPOINT
              value: http://sleepypods-control-plane.${namespace}.svc.cluster.local:50051
            - name: SLEEPYPODS_IDLE_TIMEOUT_MS
              value: "3600000"
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-sidecar
  labels:
    app.kubernetes.io/name: sleepypods-sidecar
    sleepypods.io/kind-e2e: metrics
spec:
  selector:
    app.kubernetes.io/name: sleepypods-sidecar
  ports:
    - name: http
      port: 15000
      targetPort: 15000
    - name: metrics
      port: 19092
      targetPort: 19092
YAML

KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-frontline --timeout=180s
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-sidecar --timeout=180s

echo "==> Starting local port-forwards"
start_port_forward sleepypods-control-plane \
  "${operator_port}:50053" \
  "${control_plane_metrics_port}:19090"
start_port_forward sleepypods-frontline \
  "${frontline_port}:8080" \
  "${frontline_metrics_port}:19091"
start_port_forward sleepypods-sidecar \
  "${sidecar_port}:15000" \
  "${sidecar_metrics_port}:19092"

echo "==> Scraping dedicated metrics listeners"
scrape_metrics \
  control-plane \
  "http://127.0.0.1:${control_plane_metrics_port}/metrics" \
  'sleepypods_materializations_nonterminal{state="pending"}'
scrape_metrics \
  frontline \
  "http://127.0.0.1:${frontline_metrics_port}/metrics" \
  '# TYPE sleepypods_runtime_control_plane_calls_total counter'
scrape_metrics \
  sidecar \
  "http://127.0.0.1:${sidecar_metrics_port}/metrics" \
  '# TYPE sleepypods_runtime_control_plane_calls_total counter'

assert_metrics_listener_path_is_isolated \
  control-plane \
  "http://127.0.0.1:${control_plane_metrics_port}/not-metrics"
assert_metrics_listener_path_is_isolated \
  frontline \
  "http://127.0.0.1:${frontline_metrics_port}/not-metrics"
assert_metrics_listener_path_is_isolated \
  sidecar \
  "http://127.0.0.1:${sidecar_metrics_port}/not-metrics"

echo "==> Probing main listeners for misrouted /metrics"
assert_main_listener_does_not_serve_metrics \
  control-plane \
  "http://127.0.0.1:${operator_port}/metrics"
assert_main_listener_does_not_serve_metrics \
  frontline \
  "http://127.0.0.1:${frontline_port}/metrics"
assert_main_listener_does_not_serve_metrics \
  sidecar \
  "http://127.0.0.1:${sidecar_port}/metrics"

echo "metrics full-platform kind E2E completed"
