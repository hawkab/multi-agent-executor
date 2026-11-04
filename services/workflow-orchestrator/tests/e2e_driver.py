import json
import os
import time
from typing import Any, Callable

import pika

AMQP_URL = os.environ["AMQP_URL"]
EXCHANGE = os.getenv("AMQP_EXCHANGE", "aiops.exchange")
ISSUE = os.getenv("E2E_ISSUE", "E2E-FULL-1")
MODE = os.getenv("E2E_MODE", "fullstack")

connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
channel = connection.channel()
channel.confirm_delivery()


def publish(routing_key: str, payload: dict[str, Any]) -> None:
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload).encode(),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        mandatory=True,
    )


def take(
    queue: str,
    predicate: Callable[[dict[str, Any]], bool] | None = None,
    timeout: float = 30,
) -> dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        method, _, body = channel.basic_get(queue=queue, auto_ack=False)
        if method is None:
            connection.process_data_events(time_limit=0)
            time.sleep(0.1)
            continue
        message = json.loads(body.decode())
        channel.basic_ack(method.delivery_tag)
        if predicate is None or predicate(message):
            return message
    raise TimeoutError(f"timeout waiting for queue={queue}")


def role_result(
    role: str,
    *,
    status: str,
    artifact: dict[str, Any] | None = None,
    summary: str = "e2e",
) -> None:
    dispatch = take(
        f"agent.{role}.requests",
        lambda message: (message.get("metadata") or {}).get("issueKey") == ISSUE,
    )
    publish(
        "workflow.results",
        {
            "senderId": f"agent-{role}",
            "correlationId": dispatch["correlationId"],
            "content": json.dumps(
                {
                    "status": status,
                    "summary": summary,
                    "artifact": artifact or {},
                    "worklog": [{"step": role, "status": "done", "evidence": "e2e"}],
                }
            ),
            "metadata": {"issueKey": ISSUE, "role": role},
        },
    )


def jira_ack(action: str, entity_key: str) -> dict[str, Any]:
    command = take(
        "jira.commands",
        lambda message: message.get("issue_key") == ISSUE
        and message.get("action") == action,
    )
    operation_id = command["operation_id"]
    publish(
        "workflow.external-events",
        {
            "senderId": "jira-bot",
            "source": "jira-bot",
            "issue_key": ISSUE,
            "operation_id": operation_id,
            "status": "completed",
            "result": {"issue_key": entity_key, "parent_issue_key": ISSUE},
            "event_id": f"jira-e2e:{operation_id}:completed",
        },
    )
    return command


analyst_artifact = {
    "spec": {"requirements": ["X"]},
    "affected_components": ["backend"],
    "subtasks": [
        {"summary": "Implement X", "spec": {"part": 1}},
        {"summary": "Verify X", "spec": {"part": 2}},
    ],
}
if MODE == "parallel":
    analyst_artifact.update(
        {
            "affected_components": ["backend", "frontend"],
            "global_api_contract": {
                "version": "1.0.0",
                "status": "frozen",
                "user_outcome": "Build exactly X",
                "affected_components": ["backend", "frontend"],
                "ownership": {
                    "backend": ["backend/**"],
                    "frontend": ["frontend/**"],
                },
                "http": {"openapi": "3.1.0", "operations": []},
                "events": {"schemas": []},
                "errors": {"format": "problem+json"},
                "idempotency": {"keys": [], "retry_semantics": "none"},
                "data_contract": {"entities": [], "constraints": []},
                "migration_compatibility": {
                    "expand_migrate_contract": True,
                    "data_preservation_checks": [],
                },
                "fixtures": ["contract-v1.json"],
                "acceptance_checks": {"shared": [], "integration": []},
                "rollout": {"strategy": "compatible"},
                "rollback": {"strategy": "revert"},
            },
        }
    )

publish(
    "workflow.requests",
    {
        "issue_key": ISSUE,
        "summary": "Full isolated E2E",
        "description": "Build exactly X",
        "correlationId": "e2e-full-request-1",
    },
)
role_result(
    "analyst",
    status="completed",
    artifact=analyst_artifact,
)
role_result("analyst-reviewer", status="approved", artifact={"approved": True})
jira_ack("create_subtask", "E2E-SUB-1")
jira_ack("create_subtask", "E2E-SUB-2")
if MODE == "parallel":
    for role, mr_iid in (("backend-developer", 41), ("frontend-developer", 42)):
        role_result(
            role,
            status="completed",
            artifact={
                "contract_version": "1.0.0",
                "project": "example/project",
                "mr_iid": mr_iid,
                "commit_sha": f"{role}-abc123",
                "self_review": {
                    "status": "passed",
                    "reviewed_sha": f"{role}-abc123",
                    "findings_fixed": [],
                },
                "tests": {"status": "passed", "evidence": ["e2e"]},
            },
        )
    for role in ("backend-developer", "frontend-developer"):
        role_result(
            "developer-reviewer",
            status="approved",
            artifact={
                "reviewed_role": role,
                "reviewed_sha": f"{role}-abc123",
                "approval": {"status": "approved", "id": f"approval-{role}"},
            },
        )
else:
    role_result(
        "developer",
        status="completed",
        artifact={
            "project": "example/project",
            "mr_iid": 42,
            "commit_sha": "abc123",
            "self_review": {
                "status": "passed",
                "reviewed_sha": "abc123",
                "findings_fixed": [],
            },
            "tests": {"status": "passed", "evidence": ["e2e"]},
        },
    )
    role_result(
        "developer-reviewer",
        status="approved",
        artifact={
            "reviewed_role": "developer",
            "reviewed_sha": "abc123",
            "approval": {"status": "approved", "id": "approval-developer"},
        },
    )
deployed_sha = "integrated-abc123" if MODE == "parallel" else "abc123"
deployment_artifact: dict[str, Any] = {
    "environment": "staging",
    "deployed_sha": deployed_sha,
    "pipeline": {"id": "e2e-pipeline", "status": "success"},
    "readiness": {"status": "passed", "evidence": ["health: ok"]},
    "checks": {"status": "passed", "evidence": ["integration: passed"]},
}
if MODE == "parallel":
    deployment_artifact.update(
        {
            "project": "example/integration",
            "mr_iid": 99,
            "component_shas": {
                "backend-developer": "backend-developer-abc123",
                "frontend-developer": "frontend-developer-abc123",
            },
        }
    )
role_result("devops", status="completed", artifact=deployment_artifact)
jira_ack("create_test_task", "E2E-TEST-1")
role_result(
    "tester",
    status="approved",
    artifact={
        "tested_sha": deployed_sha,
        "tests": {"status": "passed", "evidence": ["acceptance: passed"]},
        "approval": {"status": "approved", "id": "tester-approval"},
    },
)
close_command = take(
    "jira.commands",
    lambda message: message.get("issue_key") == ISSUE and message.get("action") == "close",
    timeout=45,
)
assert close_command["evidence"]["adapter_mode"] == "stub"
assert close_command["evidence"]["verified"] is True
print(
    "isolated-full-e2e-ok: subtasks_ack=2 test_task_ack=1 "
    "gitlab_stub=verified jira_close=observed"
)
connection.close()
