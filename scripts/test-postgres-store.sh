#!/usr/bin/env bash
set -euo pipefail

postgres_image="${SLEEPYPODS_POSTGRES_IMAGE:-postgres:17-alpine}"
ready_timeout="${SLEEPYPODS_POSTGRES_READY_TIMEOUT:-60}"
container_id=""
postgres_log=""
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
postgres_url="${SLEEPYPODS_POSTGRES_URL:-}"

cleanup() {
  local status=$?
  if [[ -n "${container_id}" ]]; then
    docker rm -f "${container_id}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${postgres_log}" ]]; then
    rm -f "${postgres_log}"
  fi
  exit "${status}"
}
trap cleanup EXIT

if [[ ! "${ready_timeout}" =~ ^[1-9][0-9]*$ ]]; then
  echo "SLEEPYPODS_POSTGRES_READY_TIMEOUT must be a positive integer; got ${ready_timeout}" >&2
  exit 2
fi

if [[ -z "${postgres_url}" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "SLEEPYPODS_POSTGRES_URL is not set and docker is not available for a disposable Postgres database" >&2
    exit 127
  fi

  echo "Starting disposable Postgres ${postgres_image}"
  container_id="$(
    docker run \
      --detach \
      --env POSTGRES_DB=sleepypods \
      --env POSTGRES_USER=sleepypods \
      --env POSTGRES_PASSWORD=sleepypods \
      --publish 127.0.0.1::5432 \
      "${postgres_image}" \
      -c max_connections=1000
  )"

  deadline=$((SECONDS + ready_timeout))
  until docker exec "${container_id}" pg_isready -U sleepypods -d sleepypods >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "Postgres container did not become ready within ${ready_timeout}s" >&2
      docker logs "${container_id}" >&2 || true
      exit 1
    fi
    sleep 1
  done

  host_port="$(docker port "${container_id}" 5432/tcp | sed -E 's/^.*:([0-9]+)$/\1/' | tail -n 1)"
  if [[ ! "${host_port}" =~ ^[0-9]+$ ]]; then
    echo "Could not determine localhost port for disposable Postgres container" >&2
    docker port "${container_id}" 5432/tcp >&2 || true
    exit 1
  fi

  postgres_url="postgres://sleepypods:sleepypods@127.0.0.1:${host_port}/sleepypods"
else
  echo "Using SLEEPYPODS_POSTGRES_URL from environment"
fi

echo "Running Postgres store conformance against a real database"
postgres_log="$(mktemp "${TMPDIR:-/tmp}/sleepypods-postgres-store.XXXXXX")"
SLEEPYPODS_POSTGRES_URL="${postgres_url}" cargo test --locked -p control-plane --test postgres_store -- --nocapture --color never 2>&1 | tee "${postgres_log}"
python3 "${script_dir}/check-postgres-execution.py" "${postgres_log}"
