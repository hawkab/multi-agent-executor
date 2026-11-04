#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$project_root"

timeout_seconds="${REDEPLOY_TIMEOUT_SECONDS:-300}"
poll_seconds="${REDEPLOY_POLL_SECONDS:-2}"
log_tail="${REDEPLOY_LOG_TAIL:-100}"
docker_bin="${DOCKER_BIN:-docker}"
compose=("$docker_bin" compose)

prepare_default_specialist_workspaces() {
  local role variable target policy
  for role in backend-developer frontend-developer dba-developer; do
    case "$role" in
      backend-developer) variable=AGENT_BACKEND_WORKSPACE_PATH ;;
      frontend-developer) variable=AGENT_FRONTEND_WORKSPACE_PATH ;;
      dba-developer) variable=AGENT_DBA_WORKSPACE_PATH ;;
    esac
    if test -n "${!variable:-}"; then
      continue
    fi
    target="$project_root/agent-workspaces/$role"
    mkdir -p "$target" "$target/cron" "$target/memory" "$target/runtime" "$target/sessions"
    for policy in AGENTS.md HEARTBEAT.md SOUL.md; do
      install -m 0644 "$project_root/agent-workspace/$policy" "$target/$policy"
    done
  done
}

if ! [[ "$timeout_seconds" =~ ^[0-9]+$ ]] || test "$timeout_seconds" -lt 1; then
  echo "REDEPLOY_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi

prepare_default_specialist_workspaces

printf '%s\n' '[redeploy] validating Compose configuration'
"${compose[@]}" config --quiet
mapfile -t services < <("${compose[@]}" config --services)
if test "${#services[@]}" -eq 0; then
  echo "[redeploy] Compose has no services" >&2
  exit 2
fi
rabbitmq_enabled=false
for service in "${services[@]}"; do
  if test "$service" = rabbitmq; then
    rabbitmq_enabled=true
  fi
done

printf '[redeploy] building %d services from current sources\n' "${#services[@]}"
"${compose[@]}" build --pull

printf '%s\n' '[redeploy] removing previous project containers (persistent volumes are preserved)'
"${compose[@]}" down --remove-orphans

printf '%s\n' '[redeploy] recreating containers (persistent volumes are preserved)'
"${compose[@]}" up -d --force-recreate --remove-orphans

status_file="$(mktemp)"
consumer_file="${status_file}.consumers"
trap 'rm -f "$status_file" "$consumer_file" "$status_file.unready"' EXIT
start_epoch="$SECONDS"

check_ready() {
  "${compose[@]}" ps --format json >"$status_file"
  python3 - "$status_file" "${services[@]}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
required = set(sys.argv[2:])
text = path.read_text(encoding="utf-8").strip()
records = []
if text:
    try:
        parsed = json.loads(text)
        records = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        records = [json.loads(line) for line in text.splitlines() if line.strip()]
by_service = {str(item.get("Service", "")): item for item in records}
unready = []
for service in sorted(required):
    item = by_service.get(service)
    if item is None:
        unready.append(service)
        continue
    state = str(item.get("State", "")).lower()
    health = str(item.get("Health", "")).lower()
    if state != "running" or health not in {"", "healthy"}:
        unready.append(service)
if unready:
    print("\n".join(unready))
    raise SystemExit(1)
PY
}

check_consumers() {
  if test "$rabbitmq_enabled" != true; then
    return 0
  fi
  if ! "${compose[@]}" exec -T rabbitmq rabbitmqctl list_queues -p / -q name consumers >"$consumer_file"; then
    return 1
  fi
  python3 - "$consumer_file" <<'PY'
import sys
from pathlib import Path

required = {
    "guardian.requests",
    "workflow.requests",
    "workflow.results",
    "workflow.external-events",
    "gitlab.commands",
    "agent.devops.requests",
    "agent.analyst.requests",
    "agent.analyst-reviewer.requests",
    "agent.developer.requests",
    "agent.backend-developer.requests",
    "agent.frontend-developer.requests",
    "agent.dba-developer.requests",
    "agent.developer-reviewer.requests",
    "agent.tester.requests",
}
observed = {}
for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    fields = line.split()
    if len(fields) >= 2:
        try:
            observed[fields[0]] = int(fields[1])
        except ValueError:
            pass
missing = sorted(queue for queue in required if observed.get(queue, 0) < 1)
if missing:
    print("missing RabbitMQ consumers: " + ", ".join(missing), file=sys.stderr)
    raise SystemExit(1)
PY
}

check_all_ready() {
  check_ready || return 1
  if ! check_consumers; then
    printf '%s\n' rabbitmq
    return 1
  fi
}

while ! check_all_ready >"$status_file.unready"; do
  if test $((SECONDS - start_epoch)) -ge "$timeout_seconds"; then
    printf '%s\n' '[redeploy] readiness timeout; current status:' >&2
    "${compose[@]}" ps --all >&2 || true
    mapfile -t unready <"$status_file.unready"
    for service in "${unready[@]}"; do
      test -n "$service" || continue
      printf '[redeploy] logs for %s:\n' "$service" >&2
      "${compose[@]}" logs --tail "$log_tail" "$service" >&2 || true
    done
    exit 1
  fi
  sleep "$poll_seconds"
done
rm -f "$status_file.unready"

printf '[redeploy] all %d services are running and ready\n' "${#services[@]}"
"${compose[@]}" ps
