#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/kind-image.sh"
cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-e2e-failures-test}"
namespace="${SLEEPYPODS_KIND_E2E_NAMESPACE:-sleepypods-e2e-failures}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
keep_namespace="${SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE:-0}"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-kind-e2e-failures}"
app_image="${SLEEPYPODS_KIND_E2E_APP_IMAGE:-${image_prefix}/stateless-app:${image_tag}}"
postgres_image="${SLEEPYPODS_KIND_E2E_POSTGRES_IMAGE:-postgres:17-alpine}"
operator_port="${SLEEPYPODS_KIND_E2E_OPERATOR_PORT:-19651}"
workload_port="${SLEEPYPODS_KIND_E2E_WORKLOAD_PORT:-19652}"
frontline_port="${SLEEPYPODS_KIND_E2E_FRONTLINE_PORT:-19680}"
kubeconfig="$(mktemp)"
control_plane_pf_log="$(mktemp)"
frontline_pf_log="$(mktemp)"
created_cluster=0
control_plane_pf=""
frontline_pf=""

require_command() {
  local name="$1"

  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "${name} is required for the failure-path kind E2E" >&2
    exit 127
  fi
}

cleanup() {
  local status=$?

  if [[ -n "${control_plane_pf}" ]]; then
    kill "${control_plane_pf}" >/dev/null 2>&1 || true
    wait "${control_plane_pf}" 2>/dev/null || true
  fi
  if [[ -n "${frontline_pf}" ]]; then
    kill "${frontline_pf}" >/dev/null 2>&1 || true
    wait "${frontline_pf}" 2>/dev/null || true
  fi

  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}" || true
  elif [[ "${keep_namespace}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
    KUBECONFIG="${kubeconfig}" kubectl delete persistentvolume \
      -l "sleepypods.io/workload-class-id in (failure-readiness,failure-unbound,failure-duplicate-volume)" --ignore-not-found --wait=true >/dev/null 2>&1 || true
    KUBECONFIG="${kubeconfig}" kubectl delete clusterrole,clusterrolebinding \
      "sleepypods-control-plane-${namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi

  rm -f "${kubeconfig}" "${control_plane_pf_log}" "${frontline_pf_log}"
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
  --file "${repo_root}/scripts/Dockerfile.kind-stateless-app" \
  "${repo_root}"

echo "==> Pulling ${postgres_image}"
docker pull "${postgres_image}"

for image in \
  "${image_prefix}/control-plane:${image_tag}" \
  "${image_prefix}/frontline:${image_tag}" \
  "${image_prefix}/sidecar:${image_tag}" \
  "${app_image}" \
  "${postgres_image}"; do
  echo "==> Loading ${image} into kind/${cluster_name}"
  kind_load_image "${cluster_name}" "${image}"
done

echo "==> Recreating namespace ${namespace}"
KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true
KUBECONFIG="${kubeconfig}" kubectl delete persistentvolume \
  -l "sleepypods.io/workload-class-id in (failure-readiness,failure-unbound,failure-duplicate-volume)" --ignore-not-found --wait=true
KUBECONFIG="${kubeconfig}" kubectl delete clusterrole,clusterrolebinding \
  "sleepypods-control-plane-${namespace}" --ignore-not-found --wait=true
KUBECONFIG="${kubeconfig}" kubectl create namespace "${namespace}"
KUBECONFIG="${kubeconfig}" kubectl label namespace "${namespace}" \
  sleepypods.io/kind-e2e=failures --overwrite

echo "==> Deploying Postgres"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: failures
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-postgres
        sleepypods.io/kind-e2e: failures
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
    sleepypods.io/kind-e2e: failures
spec:
  selector:
    app.kubernetes.io/name: sleepypods-postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
YAML
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-postgres --timeout=180s

echo "==> Deploying control-plane and frontline"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: failures
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: failures
rules:
  - apiGroups: [""]
    resources: ["services", "persistentvolumeclaims", "secrets"]
    verbs: ["get", "list", "watch", "patch", "create", "update", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments", "statefulsets"]
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
    sleepypods.io/kind-e2e: failures
subjects:
  - kind: ServiceAccount
    name: sleepypods-control-plane
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: sleepypods-control-plane
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sleepypods-control-plane-${namespace}
  labels:
    sleepypods.io/kind-e2e: failures
rules:
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "patch", "create", "update", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sleepypods-control-plane-${namespace}
  labels:
    sleepypods.io/kind-e2e: failures
subjects:
  - kind: ServiceAccount
    name: sleepypods-control-plane
    namespace: ${namespace}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sleepypods-control-plane-${namespace}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: failures
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-control-plane
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-control-plane
        sleepypods.io/kind-e2e: failures
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
              value: no-auth
            - name: SLEEPYPODS_STORE_PROVIDER
              value: postgres
            - name: SLEEPYPODS_POSTGRES_URL
              value: postgres://sleepypods:sleepypods@sleepypods-postgres:5432/sleepypods
            # Exercise the configured terminal deadline within this bounded failure gate.
            - name: SLEEPYPODS_OPERATION_TIMEOUT_MS
              value: "30000"
            - name: SLEEPYPODS_CLUSTER_ID
              value: kind-e2e-failures
            - name: SLEEPYPODS_NAMESPACE
              value: ${namespace}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: failures
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
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: failures
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-frontline
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-frontline
        sleepypods.io/kind-e2e: failures
    spec:
      containers:
        - name: frontline
          image: ${image_prefix}/frontline:${image_tag}
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            tcpSocket:
              port: http
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_FRONTLINE_LISTEN_ADDR
              value: 0.0.0.0:8080
            - name: SLEEPYPODS_CONTROL_PLANE_ENDPOINT
              value: http://sleepypods-control-plane.${namespace}.svc.cluster.local:50051
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: failures
spec:
  selector:
    app.kubernetes.io/name: sleepypods-frontline
  ports:
    - name: http
      port: 8080
      targetPort: 8080
YAML

KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-control-plane --timeout=180s
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-frontline --timeout=180s

echo "==> Starting local port-forwards"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" port-forward \
  svc/sleepypods-control-plane "${operator_port}:50053" "${workload_port}:50051" >"${control_plane_pf_log}" 2>&1 &
control_plane_pf=$!
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" port-forward \
  svc/sleepypods-frontline "${frontline_port}:8080" >"${frontline_pf_log}" 2>&1 &
frontline_pf=$!

echo "==> Running failure-path kind E2E driver"
KUBECONFIG="${kubeconfig}" \
  SLEEPYPODS_KIND_E2E_FAILURES=1 \
  SLEEPYPODS_E2E_NAMESPACE="${namespace}" \
  SLEEPYPODS_E2E_OPERATOR_ENDPOINT="http://127.0.0.1:${operator_port}" \
  SLEEPYPODS_E2E_WORKLOAD_ENDPOINT="http://127.0.0.1:${workload_port}" \
  SLEEPYPODS_E2E_FRONTLINE_ADDR="127.0.0.1:${frontline_port}" \
  SLEEPYPODS_E2E_APP_IMAGE="${app_image}" \
  SLEEPYPODS_E2E_SIDECAR_IMAGE="${image_prefix}/sidecar:${image_tag}" \
  cargo test -p control-plane --test kind_e2e_failures -- --ignored --nocapture

echo "failure-path full-platform kind E2E completed"
