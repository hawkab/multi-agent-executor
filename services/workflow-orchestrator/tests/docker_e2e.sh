#!/usr/bin/env bash
set -euo pipefail

suffix="$$"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
vhost="mae-e2e-${suffix}"
workflow="mae-workflow-${suffix}"
gitlab="mae-gitlab-${suffix}"
state_dir="$(mktemp -d /tmp/mae-state-XXXXXX)"
network="multi-agent-executor_aiops"

cleanup() {
  status=$?
  if test "$status" -ne 0; then
    echo "--- workflow logs ---" >&2
    docker logs "$workflow" >&2 2>/dev/null || true
  fi
  docker rm -f "$workflow" >/dev/null 2>&1 || true
  docker rm -f "$gitlab" >/dev/null 2>&1 || true
  docker exec rabbitmq rabbitmqctl delete_vhost "$vhost" >/dev/null 2>&1 || true
  docker run --rm -v "$state_dir:/state" --entrypoint chmod \
    multi-agent-executor-workflow-orchestrator -R a+rwX /state >/dev/null 2>&1 || true
  rm -rf "$state_dir"
  return "$status"
}
trap cleanup EXIT

docker exec rabbitmq rabbitmq-diagnostics -q ping >/dev/null
docker exec rabbitmq rabbitmqctl add_vhost "$vhost" >/dev/null
docker exec rabbitmq rabbitmqctl set_permissions -p "$vhost" aiops '.*' '.*' '.*' >/dev/null

docker run -d --name "$workflow" --network "$network" \
  -e AMQP_URL="amqp://rabbitmq:5672/${vhost}" \
  -e AMQP_EXCHANGE=aiops.exchange \
  -e WORKFLOW_STATE_PATH=/state/workflow.sqlite3 \
  -e WORKFLOW_LEASE_SECONDS=60 \
  -e ALLOW_STUB_TERMINAL_VERIFICATION=true \
  -v "$state_dir:/state" \
  multi-agent-executor-workflow-orchestrator >/dev/null

for _ in $(seq 1 20); do
  if test -f "$state_dir/workflow.ready"; then
    break
  fi
  sleep 1
done
test -f "$state_dir/workflow.ready"

docker run --rm --network "$network" --entrypoint python \
  multi-agent-executor-workflow-orchestrator -c \
  "import json,pika; c=pika.BlockingConnection(pika.URLParameters('amqp://rabbitmq:5672/${vhost}')); ch=c.channel(); ch.basic_publish(exchange='aiops.exchange',routing_key='workflow.requests',body=json.dumps({'issue_key':'E2E-1','summary':'Synthetic','description':'Build exactly X','correlationId':'e2e-request-1'}).encode(),properties=pika.BasicProperties(delivery_mode=2)); c.close()"

for _ in $(seq 1 20); do
  count="$(docker exec rabbitmq rabbitmqctl list_queues -p "$vhost" -q name messages | awk '$1=="agent.analyst.requests" {print $2}')"
  if test "${count:-0}" = "1"; then
    break
  fi
  sleep 1
done

test "$(docker exec rabbitmq rabbitmqctl list_queues -p "$vhost" -q name messages | awk '$1=="agent.analyst.requests" {print $2}')" = "1"
test -s "$state_dir/workflow.sqlite3"
test -s "$state_dir/worklogs/system/E2E-1.jsonl"
test -s "$state_dir/worklogs/analyst/E2E-1.jsonl"

python3 -c "import sqlite3; db=sqlite3.connect('$state_dir/workflow.sqlite3'); row=db.execute('select state_json from workflows where issue_key=?',('E2E-1',)).fetchone(); assert row and '\"stage\": \"analysis\"' in row[0]; print('isolated-e2e-ok: analyst_queue=1 durable_state=ok per_role_worklog=ok')"

amqp_url="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$workflow" | sed -n 's/^AMQP_URL=//p')"
test -n "$amqp_url"

docker run -d --name "$gitlab" --network "$network" \
  -e AMQP_URL="$amqp_url" \
  -e AMQP_EXCHANGE=aiops.exchange \
  -e GITLAB_ADAPTER_MODE=stub \
  multi-agent-executor-gitlab-adapter >/dev/null

docker run --rm --network "$network" \
  -e AMQP_URL="$amqp_url" \
  -e AMQP_EXCHANGE=aiops.exchange \
  -v "$script_dir:/e2e:ro" \
  --entrypoint python \
  multi-agent-executor-workflow-orchestrator /e2e/e2e_driver.py

docker run --rm --network "$network" \
  -e AMQP_URL="$amqp_url" \
  -e AMQP_EXCHANGE=aiops.exchange \
  -e E2E_ISSUE=E2E-PAR-1 \
  -e E2E_MODE=parallel \
  -v "$script_dir:/e2e:ro" \
  --entrypoint python \
  multi-agent-executor-workflow-orchestrator /e2e/e2e_driver.py

python3 -c "import json,sqlite3; db=sqlite3.connect('$state_dir/workflow.sqlite3'); rows={key:json.loads(value) for key,value in db.execute('select issue_key,state_json from workflows where issue_key in (?,?)',('E2E-FULL-1','E2E-PAR-1'))}; assert rows['E2E-FULL-1']['stage']=='done'; assert rows['E2E-PAR-1']['stage']=='done'; assert rows['E2E-PAR-1']['development_mode']=='parallel'; assert rows['E2E-PAR-1']['reviewed_development_roles']==['backend-developer','frontend-developer']; assert all(state['artifacts']['gitlab_verification']['adapter_mode']=='stub' for state in rows.values()); print('isolated-full-state-ok: fullstack=done parallel=done adapter_mode=stub')"
