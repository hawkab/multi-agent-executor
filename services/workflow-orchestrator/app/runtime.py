"""AMQP-facing application layer for the workflow domain."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from typing import Any, Protocol

from app.workflow import Action, AgentResult, Role, WorkflowEngine


class Publisher(Protocol):
    def publish(self, routing_key: str, payload: dict[str, Any]) -> None: ...


ROLE_RULES: dict[Role, str] = {
    Role.ANALYST: """Analyse the original user request without changing its intent. Ask for input when ambiguity affects behavior. Produce an English machine-readable specification, architecture, acceptance criteria, exclusions, risks, independently implementable subtasks, and affected_components chosen only from backend/frontend/database. Each subtask must contain its own decomposed specification. For two or three affected components use API first and produce one frozen versioned global_api_contract with user outcome, HTTP/event/error/idempotency boundaries, backend-facing data contract, migration compatibility, fixtures, non-overlapping ownership, shared acceptance checks, rollout and rollback. Do not add unrequested functionality.""",
    Role.ANALYST_REVIEWER: """Review the analyst artifact against the ORIGINAL USER REQUEST. Check fidelity, omissions, contradictions, testability, scope creep, affected component classification, and the global API contract for multi-component work. Request precise changes or approve. Never invent requirements.""",
    Role.DEVELOPER: """Act as the fullstack developer for small/medium work affecting at most one product component. Inspect the real codebase and approved specification. Maintain a checked implementation plan and per-step worklog. Use strict TDD (RED, GREEN, REFACTOR), SOLID, YAGNI, DRY, KISS and hexagonal boundaries without speculative abstractions. Run focused and regression tests, perform and fix a complete self-review, and create or update the internal GitLab merge request. Return artifact.self_review.status='passed' and artifact.tests.status='passed' with real evidence. Never claim commands or tests that were not executed.""",
    Role.BACKEND_DEVELOPER: """Implement only the backend ownership from the frozen global API contract. Work independently against its exact contract_version and fixtures, use strict TDD and hexagonal boundaries, run relevant tests, perform and fix a complete self-review, and create/update your MR. Return contract_version, self_review.status='passed', tests.status='passed', project and mr_iid with factual evidence. Never silently change the contract.""",
    Role.FRONTEND_DEVELOPER: """Implement only the frontend ownership from the frozen global API contract. Work independently against its exact contract_version and fixtures/doubles, use strict TDD, run relevant UI/contract tests, perform and fix a complete self-review, and create/update your MR. Return contract_version, self_review.status='passed', tests.status='passed', project and mr_iid with factual evidence. Never silently change the contract.""",
    Role.DBA_DEVELOPER: """Implement only the database ownership from the frozen global API contract. Include migrations, compatibility, data preservation and rollback checks, use strict TDD, perform and fix a complete self-review, and create/update your MR. Return contract_version, self_review.status='passed', tests.status='passed', project and mr_iid with factual evidence. Never silently change the contract.""",
    Role.DEVELOPER_REVIEWER: """Independently review only the target_role actual MR diff and test evidence against the approved specification and frozen contract. Check correctness, security, SOLID, YAGNI, DRY, KISS, architecture/ownership boundaries and unwanted functionality. An approval applies only to this target role. If clean, place a real approval through the available project CLI/API and return artifact.reviewed_role; otherwise return actionable findings.""",
    Role.DEVOPS: """Act as the deployment and cross-component integration specialist after every developer diff has independent approval. Reject mixed contract versions or integration drift. Verify contract/integration/migration tests, the real GitLab CI/CD pipeline, deploy to the test environment, and check application readiness. For parallel development, create or identify the single integration MR containing the approved component revisions and return its project and mr_iid; this is the only MR eligible for final tester/GitLab merge verification. On failure collect relevant application/deployment logs, attach or comment them on the MR, and return deploy_failed without hiding errors. Do not weaken guardian or security controls.""",
    Role.TESTER: """Test the deployed behavior against the ORIGINAL USER REQUEST, deliberately ignoring implementation assumptions in the machine-readable specification. Report obvious bugs in the open MR and include affected_components when known. Mark ambiguous behavior as ambiguous_bug with one exact question for the analyst. After all user-visible paths pass, place the tester approval and return tested_sha plus factual evidence. The trusted GitLab adapter, not this role, verifies merge/pipeline state. Never report approved without real test and approval evidence.""",
}

OUTPUT_CONTRACT = """
Return exactly one JSON object and no prose/code fence:
{
  "status": "completed|needs_input|changes_requested|approved|deploy_failed|bugs_found|ambiguous_bug|clarified|blocked",
  "summary": "concise factual result or exact question/findings",
  "artifact": {},
  "worklog": [{"step":"...","status":"done|blocked","evidence":"command/path/result"}]
}
Do not report success without real evidence. A question must use status needs_input.
""".strip()

ROLE_JIRA_USERS = {
    role: os.getenv(f"JIRA_USER_{role.value.upper().replace('-', '_')}", role.value)
    for role in Role
}


def parse_agent_result(role: Role, content: str, event_id: str) -> AgentResult:
    text = content.strip()
    fence = re.fullmatch(
        r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL | re.IGNORECASE
    )
    if fence:
        text = fence.group(1)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"agent response is not valid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("status"), str):
        raise ValueError("agent response must be an object with string status")
    artifact = payload.get("artifact") or {}
    worklog = payload.get("worklog") or []
    if not isinstance(artifact, dict) or not isinstance(worklog, list):
        raise ValueError("artifact must be object and worklog must be array")
    return AgentResult(
        role=role,
        status=payload["status"],
        summary=str(payload.get("summary", "")),
        artifact=artifact,
        worklog=[entry for entry in worklog if isinstance(entry, dict)],
        event_id=event_id,
    )


class Runtime:
    def __init__(
        self, engine: WorkflowEngine, publisher: Publisher, lease_seconds: int = 900
    ):
        self.engine = engine
        self.publisher = publisher
        self.lease_seconds = lease_seconds
        self.worker_id = f"runtime-{uuid.uuid4()}"

    def handle_request(self, message: dict[str, Any]) -> None:
        issue_key = str(
            message.get("issue_key") or message.get("issueKey") or ""
        ).strip()
        if not issue_key:
            raise ValueError("workflow request has no issue key")
        summary = str(message.get("summary", "")).strip()
        description = str(message.get("description", "")).strip()
        request = f"{summary}\n\n{description}".strip()
        source_event_id = str(
            message.get("event_id")
            or message.get("correlationId")
            or (message.get("metadata") or {}).get("eventId")
            or uuid.uuid4()
        )
        request_hash = hashlib.sha256(request.encode("utf-8")).hexdigest()
        event_id = f"{issue_key}:{source_event_id}:{request_hash}"
        self.engine.accept_request(issue_key, request, event_id)
        self.drain_outbox()

    def handle_result(self, message: dict[str, Any]) -> None:
        metadata = message.get("metadata") or {}
        issue_key = str(
            message.get("issue_key")
            or metadata.get("issueKey")
            or message.get("chatId")
            or ""
        ).strip()
        event_id = str(
            message.get("event_id") or message.get("correlationId") or uuid.uuid4()
        )
        if metadata.get("_progress"):
            role = Role(str(message.get("role") or metadata.get("role") or ""))
            dispatch_id = str(metadata.get("dispatchId") or event_id)
            self.engine.heartbeat(
                issue_key,
                role,
                dispatch_id=dispatch_id,
                lease_seconds=self.lease_seconds,
            )
            return
        if self.engine.store.has_event(event_id):
            self.drain_outbox()
            return
        try:
            role = Role(str(message.get("role") or metadata.get("role") or ""))
            result = parse_agent_result(role, str(message.get("content", "")), event_id)
        except (ValueError, TypeError) as exc:
            self.execute_action(
                Action(
                    "escalate",
                    issue_key,
                    payload={"reason": f"invalid agent result: {exc}"},
                )
            )
            return

        self.engine.accept_result(issue_key, result)
        self.drain_outbox()

    def handle_external_event(self, message: dict[str, Any]) -> None:
        source = str(message.get("source") or message.get("senderId") or "").strip()
        sender = str(message.get("senderId") or "").strip()
        if not source or sender != source:
            raise ValueError("external event source is not trusted")
        operation_id = str(message.get("operation_id", "")).strip()
        if not operation_id:
            raise ValueError("external event has no operation_id")
        result = message.get("result") or {}
        if not isinstance(result, dict):
            raise ValueError("external event result must be an object")
        event_id = str(
            message.get("event_id")
            or message.get("correlationId")
            or f"{source}:{operation_id}:{message.get('status', '')}"
        )
        self.engine.accept_external_event(
            source=source,
            operation_id=operation_id,
            status=str(message.get("status", "")).strip().lower(),
            result=result,
            event_id=event_id,
        )
        self.drain_outbox()

    def run_hypervisor(self, max_attempts: int = 3) -> None:
        self.engine.recover_expired(max_attempts=max_attempts)
        self.drain_outbox()

    def drain_outbox(self) -> None:
        while True:
            claimed = self.engine.store.claim_pending_actions(
                self.worker_id, lease_seconds=60, limit=1
            )
            if not claimed:
                return
            command_id, action = claimed[0]
            try:
                self.execute_action(action, command_id=command_id)
                self.engine.store.mark_action_delivered(command_id)
            except Exception:
                self.engine.store.release_action_claim(command_id, self.worker_id)
                raise

    @staticmethod
    def _dispatch_is_active(workflow: Any, role: Role, dispatch_id: str) -> bool:
        return workflow.active_dispatches.get(role.value) == dispatch_id or (
            workflow.current_role == role and workflow.active_dispatch_id == dispatch_id
        )

    def execute_action(self, action: Action, command_id: str | None = None) -> None:
        if action.kind == "dispatch":
            if action.role is None:
                raise ValueError("dispatch action requires role")
            workflow = self.engine.get(action.issue_key)
            event_id = str(action.payload.get("dispatch_id") or uuid.uuid4())
            if not self._dispatch_is_active(workflow, action.role, event_id):
                return
            payload = {
                "senderId": "workflow-orchestrator",
                "chatId": action.issue_key,
                "sessionKey": f"workflow:{action.issue_key}:{action.role.value}:{event_id}",
                "correlationId": event_id,
                "replyTo": "workflow.results",
                "metadata": {
                    "issueKey": action.issue_key,
                    "role": action.role.value,
                    "stage": workflow.stage.value,
                    "eventId": event_id,
                    "dispatchId": event_id,
                    "_suppress_progress": False,
                    "_wants_stream": False,
                },
                "content": self._build_prompt(workflow, action.role, action.payload),
            }
            has_lease = (
                workflow.role_leases.get(action.role.value) is not None
                if action.role.value in workflow.active_dispatches
                else workflow.lease_until is not None
            )
            if not has_lease:
                self.engine.claim(
                    action.issue_key,
                    action.role,
                    dispatch_id=event_id,
                    lease_seconds=self.lease_seconds,
                )
            self.publisher.publish(f"agent.{action.role.value}.requests", payload)
            return

        if action.kind == "assign_role":
            if action.role is None:
                raise ValueError("assign_role action requires role")
            dispatch_id = str(action.payload.get("dispatch_id") or "")
            if dispatch_id and not self._dispatch_is_active(
                self.engine.get(action.issue_key), action.role, dispatch_id
            ):
                return
            self.publisher.publish(
                "jira.commands",
                {
                    "action": "assign",
                    "issue_key": action.issue_key,
                    "login": ROLE_JIRA_USERS[action.role],
                    "command_id": command_id,
                },
            )
            return

        if action.kind == "transition_role":
            if action.role is None:
                raise ValueError("transition_role action requires role")
            dispatch_id = str(action.payload.get("dispatch_id") or "")
            if dispatch_id and not self._dispatch_is_active(
                self.engine.get(action.issue_key), action.role, dispatch_id
            ):
                return
            self.publisher.publish(
                "jira.commands",
                {
                    "action": "transition",
                    "issue_key": action.issue_key,
                    "status": "В работе",
                    "command_id": command_id,
                },
            )
            return

        if action.kind == "verify_gitlab_merge":
            self.publisher.publish(
                "gitlab.commands",
                {
                    "action": "verify_merge",
                    "issue_key": action.issue_key,
                    **action.payload,
                    "command_id": command_id,
                },
            )
            return

        command = {
            "action": action.kind,
            "issue_key": action.issue_key,
            **action.payload,
        }
        if command_id:
            command["command_id"] = command_id
        self.publisher.publish("jira.commands", command)

    @staticmethod
    def _build_prompt(workflow: Any, role: Role, context: dict[str, Any]) -> str:
        artifact_json = json.dumps(workflow.artifacts, ensure_ascii=False, indent=2)
        context_json = json.dumps(context, ensure_ascii=False, indent=2)
        return (
            f"ROLE: {role.value}\n"
            f"STAGE: {workflow.stage.value}\n"
            f"ISSUE: {workflow.issue_key}\n\n"
            f"ROLE CONTRACT:\n{ROLE_RULES[role]}\n\n"
            f"ORIGINAL USER REQUEST (immutable source of truth):\n{workflow.original_request}\n\n"
            f"CURRENT REQUEST WITH USER ANSWERS:\n{workflow.current_request}\n\n"
            f"VERIFIED ARTIFACTS FROM PREVIOUS STAGES:\n{artifact_json}\n\n"
            f"REVIEW/RECOVERY CONTEXT:\n{context_json}\n\n"
            f"{OUTPUT_CONTRACT}"
        )
