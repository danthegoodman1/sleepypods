#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/kind-image.sh"
cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-e2e-grpc-web-test}"
namespace="${SLEEPYPODS_KIND_E2E_NAMESPACE:-sleepypods-e2e-grpc-web}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
keep_namespace="${SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE:-0}"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-kind-e2e-grpc-web}"
postgres_image="${SLEEPYPODS_KIND_E2E_POSTGRES_IMAGE:-postgres:17-alpine}"
grpc_web_port="${SLEEPYPODS_KIND_E2E_GRPC_WEB_PORT:-19652}"
operator_token="${SLEEPYPODS_KIND_E2E_OPERATOR_TOKEN:-operator-token}"
proxy_token="${SLEEPYPODS_KIND_E2E_PROXY_TOKEN:-proxy-token}"
sidecar_token="${SLEEPYPODS_KIND_E2E_SIDECAR_TOKEN:-sidecar-token}"
invalid_token="${SLEEPYPODS_KIND_E2E_INVALID_TOKEN:-invalid-token}"
kubeconfig="$(mktemp)"
grpc_web_pf_log="$(mktemp)"
created_cluster=0
grpc_web_pf=""

require_command() {
  local name="$1"

  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "${name} is required for the grpc-web kind E2E" >&2
    exit 127
  fi
}

stop_port_forwards() {
  if [[ -n "${grpc_web_pf}" ]]; then
    kill "${grpc_web_pf}" >/dev/null 2>&1 || true
    wait "${grpc_web_pf}" 2>/dev/null || true
    grpc_web_pf=""
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
  if [[ "${status}" != "0" && -s "${grpc_web_pf_log}" ]]; then
    echo "==> Last grpc-web port-forward log lines (${grpc_web_pf_log})" >&2
    tail -n 80 "${grpc_web_pf_log}" >&2 || true
  fi

  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}" || true
  elif [[ "${keep_namespace}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi

  rm -f "${kubeconfig}" "${grpc_web_pf_log}"
  exit "${status}"
}
trap cleanup EXIT

for command in kind kubectl docker cargo; do
  require_command "${command}"
done

if ! kind get clusters | grep -Fxq "${cluster_name}"; then
  created_cluster=1
  KUBECONFIG="${kubeconfig}" kind create cluster --name "${cluster_name}" --wait 120s
else
  kind get kubeconfig --name "${cluster_name}" >"${kubeconfig}"
fi

control_plane_image="${image_prefix}/control-plane:${image_tag}"
echo "==> Building ${control_plane_image}"
docker build \
  --build-arg "BIN=control-plane" \
  --tag "${control_plane_image}" \
  --file "${repo_root}/Dockerfile" \
  "${repo_root}"

echo "==> Pulling ${postgres_image}"
docker pull "${postgres_image}"

for image in "${control_plane_image}" "${postgres_image}"; do
  echo "==> Loading ${image} into kind/${cluster_name}"
  kind_load_image "${cluster_name}" "${image}"
done

echo "==> Recreating namespace ${namespace}"
KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true
KUBECONFIG="${kubeconfig}" kubectl create namespace "${namespace}"
KUBECONFIG="${kubeconfig}" kubectl label namespace "${namespace}" \
  sleepypods.io/kind-e2e=grpc-web --overwrite

echo "==> Deploying Postgres"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: grpc-web
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-postgres
        sleepypods.io/kind-e2e: grpc-web
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
    sleepypods.io/kind-e2e: grpc-web
spec:
  selector:
    app.kubernetes.io/name: sleepypods-postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
YAML
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-postgres --timeout=180s

echo "==> Deploying control-plane with grpc-web listener"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: grpc-web
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: grpc-web
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
    sleepypods.io/kind-e2e: grpc-web
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
    sleepypods.io/kind-e2e: grpc-web
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-control-plane
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-control-plane
        sleepypods.io/kind-e2e: grpc-web
    spec:
      serviceAccountName: sleepypods-control-plane
      containers:
        - name: control-plane
          image: ${control_plane_image}
          imagePullPolicy: IfNotPresent
          ports:
            - name: grpc
              containerPort: 50051
            - name: operator
              containerPort: 50053
            - name: grpc-web
              containerPort: 50052
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
            - name: SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR
              value: 0.0.0.0:50052
            - name: SLEEPYPODS_CONTROL_PLANE_AUTH_MODE
              value: static-bearer-token
            - name: SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN
              value: ${operator_token}
            - name: SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN
              value: ${proxy_token}
            - name: SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN
              value: ${sidecar_token}
            - name: SLEEPYPODS_STORE_PROVIDER
              value: postgres
            - name: SLEEPYPODS_POSTGRES_URL
              value: postgres://sleepypods:sleepypods@sleepypods-postgres:5432/sleepypods
            - name: SLEEPYPODS_CLUSTER_ID
              value: kind-e2e-grpc-web
            - name: SLEEPYPODS_NAMESPACE
              value: ${namespace}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: grpc-web
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
    - name: grpc-web
      port: 50052
      targetPort: 50052
YAML

KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-control-plane --timeout=180s

echo "==> Starting local grpc-web port-forward"
port_forward_loop grpc-web "${grpc_web_pf_log}" -n "${namespace}" port-forward \
  svc/sleepypods-control-plane "${grpc_web_port}:50052" &
grpc_web_pf=$!
wait_for_local_port grpc-web "${grpc_web_port}" "${grpc_web_pf}" "${grpc_web_pf_log}"

echo "==> Running grpc-web kind E2E driver"
KUBECONFIG="${kubeconfig}" \
  SLEEPYPODS_KIND_E2E_GRPC_WEB=1 \
  SLEEPYPODS_E2E_GRPC_WEB_ENDPOINT="127.0.0.1:${grpc_web_port}" \
  SLEEPYPODS_E2E_OPERATOR_TOKEN="${operator_token}" \
  SLEEPYPODS_E2E_INVALID_TOKEN="${invalid_token}" \
  cargo test -p control-plane --test kind_e2e_grpc_web -- --ignored --nocapture

echo "grpc-web deployed control-plane kind E2E completed"
