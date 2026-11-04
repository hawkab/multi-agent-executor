"""Durable domain state machine for the delivery workflow.

The module has no RabbitMQ/Jira/GitLab dependencies. Adapters execute returned
Actions, which keeps orchestration rules testable and leaves Jira ownership in
jira-bot.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterator


class Role(StrEnum):
    ANALYST = "analyst"
    ANALYST_REVIEWER = "analyst-reviewer"
    DEVELOPER = "developer"
    BACKEND_DEVELOPER = "backend-developer"
    FRONTEND_DEVELOPER = "frontend-developer"
    DBA_DEVELOPER = "dba-developer"
    DEVELOPER_REVIEWER = "developer-reviewer"
    DEVOPS = "devops"
    TESTER = "tester"


SPECIALIST_ROLES = {
    Role.BACKEND_DEVELOPER,
    Role.FRONTEND_DEVELOPER,
    Role.DBA_DEVELOPER,
}
DEVELOPER_ROLES = {Role.DEVELOPER, *SPECIALIST_ROLES}
COMPONENT_ROLES = {
    "backend": Role.BACKEND_DEVELOPER,
    "frontend": Role.FRONTEND_DEVELOPER,
    "database": Role.DBA_DEVELOPER,
}
REQUIRED_CONTRACT_FIELDS = {
    "version",
    "status",
    "user_outcome",
    "affected_components",
    "ownership",
    "http",
    "events",
    "errors",
    "idempotency",
    "data_contract",
    "migration_compatibility",
    "fixtures",
    "acceptance_checks",
    "rollout",
    "rollback",
}


def _ownership_scope(pattern: str) -> str:
    normalized = pattern.strip().replace("\\", "/").removeprefix("./")
    if not normalized or normalized.startswith("/") or ".." in normalized.split("/"):
        return ""
    wildcard_positions = [
        position
        for marker in ("*", "?", "[")
        if (position := normalized.find(marker)) >= 0
    ]
    if wildcard_positions:
        normalized = normalized[: min(wildcard_positions)]
    return normalized.rstrip("/")


def _ownership_overlaps(first: str, second: str) -> bool:
    first_scope = _ownership_scope(first)
    second_scope = _ownership_scope(second)
    if not first_scope or not second_scope:
        return True
    return (
        first_scope == second_scope
        or first_scope.startswith(second_scope + "/")
        or second_scope.startswith(first_scope + "/")
    )


class Stage(StrEnum):
    ANALYSIS = "analysis"
    WAITING_USER_ANALYSIS = "waiting-user-analysis"
    ANALYSIS_REVIEW = "analysis-review"
    CREATING_SUBTASKS = "creating-subtasks"
    DEVELOPMENT = "development"
    DEVELOPMENT_REVIEW = "development-review"
    DEPLOYMENT = "deployment"
    CREATING_TEST_TASK = "creating-test-task"
    TESTING = "testing"
    TEST_CLARIFICATION = "test-clarification"
    WAITING_USER_TEST = "waiting-user-test"
    VERIFYING_MERGE = "verifying-merge"
    DONE = "done"
    BLOCKED = "blocked"


@dataclass(slots=True)
class AgentResult:
    role: Role
    status: str
    summary: str = ""
    artifact: dict[str, Any] = field(default_factory=dict)
    worklog: list[dict[str, Any]] = field(default_factory=list)
    event_id: str = ""


@dataclass(slots=True)
class Action:
    kind: str
    issue_key: str
    role: Role | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Workflow:
    issue_key: str
    original_request: str
    current_request: str
    stage: Stage
    current_role: Role | None
    artifacts: dict[str, Any] = field(default_factory=dict)
    feedback: str = ""
    analysis_review_rounds: int = 0
    development_review_rounds: int = 0
    development_review_rounds_by_role: dict[str, int] = field(default_factory=dict)
    deployment_rounds: int = 0
    test_rounds: int = 0
    attempts: int = 0
    lease_until: float | None = None
    active_dispatch_id: str | None = None
    pending_operations: dict[str, dict[str, Any]] = field(default_factory=dict)
    development_mode: str = ""
    affected_components: list[str] = field(default_factory=list)
    contract_version: str = ""
    development_roles: list[str] = field(default_factory=list)
    development_artifacts: dict[str, dict[str, Any]] = field(default_factory=dict)
    reviewed_development_roles: list[str] = field(default_factory=list)
    review_queue: list[str] = field(default_factory=list)
    current_review_target: str = ""
    active_dispatches: dict[str, str] = field(default_factory=dict)
    role_leases: dict[str, float] = field(default_factory=dict)
    role_attempts: dict[str, int] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)


class WorkflowStore:
    """SQLite checkpoints plus append-only worklog/events."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._active_db: sqlite3.Connection | None = None
        self._pending_jsonl: list[
            tuple[str, Role | None, str, dict[str, Any], float]
        ] = []
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        with self._connection() as db:
            db.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS workflows (
                    issue_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    issue_key TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS worklog (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_key TEXT NOT NULL,
                    role TEXT,
                    event_type TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS outbox (
                    command_id TEXT PRIMARY KEY,
                    action_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    delivered_at REAL,
                    priority INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(outbox)").fetchall()
            }
            if "claimed_by" not in columns:
                db.execute("ALTER TABLE outbox ADD COLUMN claimed_by TEXT")
            if "claim_until" not in columns:
                db.execute("ALTER TABLE outbox ADD COLUMN claim_until REAL")
            if "priority" not in columns:
                db.execute(
                    "ALTER TABLE outbox ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"
                )

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if self._active_db is not None:
            yield self._active_db
            return
        with self._connect() as db:
            yield db

    @contextmanager
    def transaction(self) -> Iterator[None]:
        if self._active_db is not None:
            yield
            return
        db = self._connect()
        db.execute("BEGIN IMMEDIATE")
        self._active_db = db
        self._pending_jsonl = []
        pending: list[tuple[str, Role | None, str, dict[str, Any], float]] = []
        try:
            yield
            db.commit()
            pending = list(self._pending_jsonl)
        except Exception:
            db.rollback()
            raise
        finally:
            self._active_db = None
            self._pending_jsonl = []
            db.close()
        for issue_key, role, event_type, details, created_at in pending:
            self._append_jsonl(issue_key, role, event_type, details, created_at)

    def save(self, workflow: Workflow) -> None:
        workflow.updated_at = time.time()
        payload = asdict(workflow)
        payload["stage"] = workflow.stage.value
        payload["current_role"] = (
            workflow.current_role.value if workflow.current_role else None
        )
        transition: tuple[dict[str, Any], float] | None = None
        with self._connection() as db:
            previous = db.execute(
                "SELECT state_json FROM workflows WHERE issue_key = ?",
                (workflow.issue_key,),
            ).fetchone()
            previous_stage = (
                str(json.loads(previous["state_json"]).get("stage"))
                if previous is not None
                else None
            )
            db.execute(
                """INSERT INTO workflows(issue_key, state_json, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(issue_key) DO UPDATE SET
                    state_json=excluded.state_json, updated_at=excluded.updated_at""",
                (
                    workflow.issue_key,
                    json.dumps(payload, ensure_ascii=False),
                    workflow.updated_at,
                ),
            )
            if previous_stage != workflow.stage.value:
                details = {
                    "from_stage": previous_stage,
                    "to_stage": workflow.stage.value,
                }
                created_at = time.time()
                cursor = db.execute(
                    """INSERT INTO worklog(
                        issue_key, role, event_type, details_json, created_at
                    ) VALUES (?, NULL, 'transition', ?, ?)""",
                    (
                        workflow.issue_key,
                        json.dumps(details, ensure_ascii=False),
                        created_at,
                    ),
                )
                action = Action(
                    "worklog",
                    workflow.issue_key,
                    payload={
                        "role": "system",
                        "status": "transition",
                        "summary": (
                            f"workflow stage: {previous_stage or 'new'} "
                            f"-> {workflow.stage.value}"
                        ),
                        "from_stage": previous_stage,
                        "to_stage": workflow.stage.value,
                        "entries": [],
                        "event_id": f"transition-{cursor.lastrowid}",
                    },
                )
                action_payload = asdict(action)
                db.execute(
                    """INSERT OR IGNORE INTO outbox(
                        command_id, action_json, created_at, priority
                    ) VALUES (?, ?, ?, 100)""",
                    (
                        f"transition:{workflow.issue_key}:{cursor.lastrowid}:worklog",
                        json.dumps(action_payload, ensure_ascii=False),
                        created_at,
                    ),
                )
                transition = (details, created_at)
        if transition is not None:
            details, created_at = transition
            if self._active_db is not None:
                self._pending_jsonl.append(
                    (workflow.issue_key, None, "transition", details, created_at)
                )
            else:
                self._append_jsonl(
                    workflow.issue_key, None, "transition", details, created_at
                )

    def get(self, issue_key: str) -> Workflow | None:
        with self._connection() as db:
            row = db.execute(
                "SELECT state_json FROM workflows WHERE issue_key = ?", (issue_key,)
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row["state_json"])
        payload["stage"] = Stage(payload["stage"])
        if payload.get("current_role"):
            payload["current_role"] = Role(payload["current_role"])
        return Workflow(**payload)

    def all_active(self) -> list[Workflow]:
        with self._connection() as db:
            rows = db.execute("SELECT state_json FROM workflows").fetchall()
        workflows: list[Workflow] = []
        for row in rows:
            payload = json.loads(row["state_json"])
            payload["stage"] = Stage(payload["stage"])
            if payload.get("current_role"):
                payload["current_role"] = Role(payload["current_role"])
            workflow = Workflow(**payload)
            if workflow.stage not in {Stage.DONE, Stage.BLOCKED}:
                workflows.append(workflow)
        return workflows

    def mark_event(self, event_id: str, issue_key: str) -> bool:
        try:
            with self._connection() as db:
                db.execute(
                    "INSERT INTO processed_events(event_id, issue_key, created_at) VALUES (?, ?, ?)",
                    (event_id, issue_key, time.time()),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def has_event(self, event_id: str) -> bool:
        with self._connection() as db:
            row = db.execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
            ).fetchone()
        return row is not None

    def append_log(
        self,
        issue_key: str,
        role: Role | None,
        event_type: str,
        details: dict[str, Any],
    ) -> None:
        created_at = time.time()
        with self._connection() as db:
            db.execute(
                "INSERT INTO worklog(issue_key, role, event_type, details_json, created_at) VALUES (?, ?, ?, ?, ?)",
                (
                    issue_key,
                    role.value if role else None,
                    event_type,
                    json.dumps(details, ensure_ascii=False),
                    created_at,
                ),
            )
        if self._active_db is not None:
            self._pending_jsonl.append(
                (issue_key, role, event_type, details, created_at)
            )
        else:
            self._append_jsonl(issue_key, role, event_type, details, created_at)

    def _append_jsonl(
        self,
        issue_key: str,
        role: Role | None,
        event_type: str,
        details: dict[str, Any],
        created_at: float,
    ) -> None:
        role_name = role.value if role else "system"
        safe_issue_key = "".join(
            character if character.isalnum() or character in "-_." else "_"
            for character in issue_key
        )
        log_path = self.path.parent / "worklogs" / role_name / f"{safe_issue_key}.jsonl"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "issue_key": issue_key,
            "role": role.value if role else None,
            "event_type": event_type,
            "details": details,
            "created_at": created_at,
        }
        with log_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def reconcile_jsonl_worklogs(self) -> None:
        """Atomically rebuild JSONL mirrors from the authoritative SQLite log."""
        with self._connection() as db:
            rows = db.execute(
                """SELECT id, issue_key, role, event_type, details_json, created_at
                FROM worklog ORDER BY id"""
            ).fetchall()
        grouped: dict[Path, list[dict[str, Any]]] = {}
        for row in rows:
            role_name = str(row["role"] or "system")
            issue_key = str(row["issue_key"])
            safe_issue_key = "".join(
                character if character.isalnum() or character in "-_." else "_"
                for character in issue_key
            )
            path = self.path.parent / "worklogs" / role_name / f"{safe_issue_key}.jsonl"
            grouped.setdefault(path, []).append(
                {
                    "id": int(row["id"]),
                    "issue_key": issue_key,
                    "role": row["role"],
                    "event_type": row["event_type"],
                    "details": json.loads(row["details_json"]),
                    "created_at": row["created_at"],
                }
            )
        for path, records in grouped.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            with temporary.open("w", encoding="utf-8") as stream:
                for record in records:
                    stream.write(json.dumps(record, ensure_ascii=False) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)

    def enqueue_actions(self, event_id: str, actions: list[Action]) -> None:
        created_at = time.time()
        expanded: list[Action] = []
        for action in actions:
            expanded.append(action)
            if action.kind == "dispatch":
                dispatch_context = {"dispatch_id": action.payload.get("dispatch_id")}
                expanded.append(
                    Action(
                        "assign_role",
                        action.issue_key,
                        role=action.role,
                        payload=dispatch_context,
                    )
                )
                expanded.append(
                    Action(
                        "transition_role",
                        action.issue_key,
                        role=action.role,
                        payload=dispatch_context,
                    )
                )
        with self._connection() as db:
            for index, action in enumerate(expanded):
                command_id = f"{event_id}:{index}:{action.kind}"
                payload = asdict(action)
                payload["role"] = action.role.value if action.role else None
                priority = 50 if action.kind == "worklog" else 0
                db.execute(
                    """INSERT OR IGNORE INTO outbox(
                        command_id, action_json, created_at, priority
                    ) VALUES (?, ?, ?, ?)""",
                    (
                        command_id,
                        json.dumps(payload, ensure_ascii=False),
                        created_at,
                        priority,
                    ),
                )

    def pending_actions(self) -> list[tuple[str, Action]]:
        with self._connection() as db:
            rows = db.execute(
                "SELECT command_id, action_json FROM outbox WHERE delivered_at IS NULL ORDER BY priority, created_at, rowid"
            ).fetchall()
        pending: list[tuple[str, Action]] = []
        for row in rows:
            payload = json.loads(row["action_json"])
            if payload.get("role"):
                payload["role"] = Role(payload["role"])
            pending.append((row["command_id"], Action(**payload)))
        return pending

    def claim_pending_actions(
        self,
        worker_id: str,
        *,
        now: float | None = None,
        lease_seconds: int = 60,
        limit: int = 100,
    ) -> list[tuple[str, Action]]:
        current = time.time() if now is None else now
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                """SELECT command_id, action_json
                FROM outbox
                WHERE delivered_at IS NULL
                  AND (claimed_by IS NULL OR claim_until <= ?)
                ORDER BY priority, created_at, rowid
                LIMIT ?""",
                (current, limit),
            ).fetchall()
            claim_until = current + lease_seconds
            for row in rows:
                db.execute(
                    """UPDATE outbox SET claimed_by = ?, claim_until = ?
                    WHERE command_id = ? AND delivered_at IS NULL
                      AND (claimed_by IS NULL OR claim_until <= ?)""",
                    (worker_id, claim_until, row["command_id"], current),
                )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
        claimed: list[tuple[str, Action]] = []
        for row in rows:
            payload = json.loads(row["action_json"])
            if payload.get("role"):
                payload["role"] = Role(payload["role"])
            claimed.append((row["command_id"], Action(**payload)))
        return claimed

    def release_action_claim(self, command_id: str, worker_id: str) -> None:
        with self._connection() as db:
            db.execute(
                """UPDATE outbox SET claimed_by = NULL, claim_until = NULL
                WHERE command_id = ? AND claimed_by = ? AND delivered_at IS NULL""",
                (command_id, worker_id),
            )

    def mark_action_delivered(self, command_id: str) -> None:
        with self._connection() as db:
            db.execute(
                "UPDATE outbox SET delivered_at = ? WHERE command_id = ? AND delivered_at IS NULL",
                (time.time(), command_id),
            )


class WorkflowEngine:
    def __init__(
        self,
        store: WorkflowStore,
        max_review_rounds: int = 5,
        external_operation_lease_seconds: int = 300,
        allow_stub_terminal_verification: bool = False,
    ):
        self.store = store
        self.max_review_rounds = max_review_rounds
        self.external_operation_lease_seconds = external_operation_lease_seconds
        self.allow_stub_terminal_verification = allow_stub_terminal_verification

    def get(self, issue_key: str) -> Workflow:
        workflow = self.store.get(issue_key)
        if workflow is None:
            raise KeyError(f"unknown workflow: {issue_key}")
        return workflow

    def _external_operation(
        self,
        *,
        source: str,
        kind: str,
        payload: dict[str, Any],
        **metadata: Any,
    ) -> dict[str, Any]:
        return {
            "source": source,
            "kind": kind,
            "status": "pending",
            "attempts": 1,
            "lease_until": time.time() + self.external_operation_lease_seconds,
            "payload": dict(payload),
            **metadata,
        }

    def _external_operation_payload(
        self,
        workflow: Workflow,
        operation_id: str,
        operation: dict[str, Any],
    ) -> dict[str, Any] | None:
        payload = operation.get("payload")
        if isinstance(payload, dict):
            return dict(payload)
        kind = operation.get("kind")
        if kind == "create_subtask":
            subtasks = workflow.artifacts.get("analysis", {}).get("subtasks", [])
            index = int(operation.get("index", -1))
            if 0 <= index < len(subtasks) and isinstance(subtasks[index], dict):
                return {**subtasks[index], "operation_id": operation_id}
        if kind == "create_test_task":
            return {
                "original_request": workflow.original_request,
                "operation_id": operation_id,
            }
        if kind == "verify_gitlab_merge":
            expected = operation.get("expected")
            if isinstance(expected, dict):
                return {"operation_id": operation_id, **expected}
        return None

    def _dispatch(self, workflow: Workflow, role: Role, **payload: Any) -> Action:
        dispatch_id = str(uuid.uuid4())
        if workflow.development_mode == "parallel" and role in SPECIALIST_ROLES:
            workflow.current_role = None
            workflow.active_dispatch_id = None
            workflow.active_dispatches[role.value] = dispatch_id
            workflow.role_leases.pop(role.value, None)
        else:
            workflow.current_role = role
            workflow.active_dispatch_id = dispatch_id
        workflow.lease_until = None
        payload = {**payload, "dispatch_id": dispatch_id}
        self.store.save(workflow)
        action = Action("dispatch", workflow.issue_key, role, payload)
        self.store.append_log(workflow.issue_key, role, "dispatch", payload)
        return action

    def accept_request(
        self, issue_key: str, request: str, event_id: str
    ) -> list[Action]:
        with self.store.transaction():
            actions = self._accept_request(issue_key, request, event_id)
            self.store.enqueue_actions(event_id, actions)
            return actions

    def _accept_request(
        self, issue_key: str, request: str, event_id: str
    ) -> list[Action]:
        if not self.store.mark_event(event_id, issue_key):
            return []
        workflow = self.store.get(issue_key)
        if workflow is None:
            workflow = Workflow(
                issue_key=issue_key,
                original_request=request,
                current_request=request,
                stage=Stage.ANALYSIS,
                current_role=Role.ANALYST,
            )
            self.store.save(workflow)
            self.store.append_log(
                issue_key, None, "workflow_started", {"request": request}
            )
            return [self._dispatch(workflow, Role.ANALYST, reason="new_request")]

        if workflow.stage == Stage.WAITING_USER_ANALYSIS:
            workflow.current_request = request
            workflow.stage = Stage.ANALYSIS
            self.store.append_log(issue_key, None, "user_answer", {"request": request})
            return [self._dispatch(workflow, Role.ANALYST, reason="user_answer")]
        if workflow.stage == Stage.WAITING_USER_TEST:
            workflow.current_request = request
            workflow.stage = Stage.TEST_CLARIFICATION
            self.store.append_log(issue_key, None, "user_answer", {"request": request})
            return [
                self._dispatch(workflow, Role.ANALYST, reason="tester_clarification")
            ]
        return []

    def accept_result(self, issue_key: str, result: AgentResult) -> list[Action]:
        if not result.event_id:
            result.event_id = self.get(issue_key).active_dispatch_id or str(
                uuid.uuid4()
            )
        with self.store.transaction():
            actions, accepted = self._accept_result(issue_key, result)
            if accepted:
                worklog = Action(
                    "worklog",
                    issue_key,
                    payload={
                        "role": result.role.value,
                        "status": result.status,
                        "summary": result.summary,
                        "entries": result.worklog,
                        "event_id": result.event_id,
                    },
                )
                self.store.enqueue_actions(result.event_id, [worklog, *actions])
            return actions

    def _accept_result(
        self, issue_key: str, result: AgentResult
    ) -> tuple[list[Action], bool]:
        workflow = self.get(issue_key)
        parallel_dispatch_id = workflow.active_dispatches.get(result.role.value)
        owns_parallel_dispatch = (
            result.role in SPECIALIST_ROLES and parallel_dispatch_id == result.event_id
        )
        owns_sequential_dispatch = (
            workflow.current_role == result.role
            and workflow.active_dispatch_id == result.event_id
        )
        if not (owns_parallel_dispatch or owns_sequential_dispatch):
            self.store.append_log(
                issue_key,
                result.role,
                "stale_agent_result",
                {
                    "status": result.status,
                    "summary": result.summary,
                    "expected_role": workflow.current_role.value
                    if workflow.current_role
                    else None,
                },
            )
            return [], False
        if not self.store.mark_event(result.event_id, issue_key):
            return [], False
        self.store.append_log(
            issue_key,
            result.role,
            "agent_result",
            {
                "status": result.status,
                "summary": result.summary,
                "artifact": result.artifact,
                "worklog": result.worklog,
            },
        )
        if owns_parallel_dispatch:
            workflow.active_dispatches.pop(result.role.value, None)
            workflow.role_leases.pop(result.role.value, None)
            workflow.role_attempts.pop(result.role.value, None)
        workflow.lease_until = None
        workflow.attempts = 0
        status = result.status.strip().lower().replace("-", "_")

        if result.role == Role.ANALYST:
            actions = self._handle_analyst(workflow, status, result)
        elif result.role == Role.ANALYST_REVIEWER:
            actions = self._handle_analysis_reviewer(workflow, status, result)
        elif result.role in DEVELOPER_ROLES:
            actions = self._handle_developer(workflow, status, result)
        elif result.role == Role.DEVELOPER_REVIEWER:
            actions = self._handle_developer_reviewer(workflow, status, result)
        elif result.role == Role.DEVOPS:
            actions = self._handle_devops(workflow, status, result)
        elif result.role == Role.TESTER:
            actions = self._handle_tester(workflow, status, result)
        else:
            actions = self._block(workflow, f"unsupported role {result.role}")
        return actions, True

    def accept_external_event(
        self,
        *,
        source: str,
        operation_id: str,
        status: str,
        result: dict[str, Any],
        event_id: str,
    ) -> list[Action]:
        with self.store.transaction():
            workflows = [
                workflow
                for workflow in self.store.all_active()
                if operation_id in workflow.pending_operations
            ]
            if not workflows:
                return []
            workflow = workflows[0]
            operation = workflow.pending_operations[operation_id]
            if operation.get("source") != source:
                return []
            if not self.store.mark_event(event_id, workflow.issue_key):
                return []
            operation["status"] = status
            operation["result"] = dict(result)
            self.store.append_log(
                workflow.issue_key,
                None,
                "external_operation",
                {
                    "source": source,
                    "operation_id": operation_id,
                    "status": status,
                    "result": result,
                },
            )
            if status != "completed":
                actions = self._block(
                    workflow,
                    f"external operation {operation_id} returned {status}",
                )
            elif workflow.stage == Stage.CREATING_SUBTASKS and all(
                item.get("status") == "completed"
                for item in workflow.pending_operations.values()
                if item.get("kind") == "create_subtask"
            ):
                try:
                    completed = sorted(
                        (
                            item
                            for item in workflow.pending_operations.values()
                            if item.get("kind") == "create_subtask"
                        ),
                        key=lambda item: int(item["index"]),
                    )
                except (KeyError, TypeError, ValueError):
                    actions = self._block(
                        workflow, "subtask completion has invalid operation index"
                    )
                else:
                    issue_keys = [
                        str((item.get("result") or {}).get("issue_key", "")).strip()
                        for item in completed
                    ]
                    if not issue_keys or any(not issue_key for issue_key in issue_keys):
                        actions = self._block(
                            workflow, "subtask completion lacks issue_key"
                        )
                    else:
                        workflow.artifacts["jira_subtasks"] = issue_keys
                        actions = self._start_development(workflow)
            elif (
                workflow.stage == Stage.CREATING_TEST_TASK
                and operation.get("kind") == "create_test_task"
            ):
                test_issue_key = str(result.get("issue_key", "")).strip()
                if not test_issue_key:
                    actions = self._block(
                        workflow, "test task completion lacks issue_key"
                    )
                else:
                    workflow.artifacts["jira_test_task"] = test_issue_key
                    workflow.stage = Stage.TESTING
                    actions = [
                        self._dispatch(
                            workflow,
                            Role.TESTER,
                            test_issue_key=test_issue_key,
                        )
                    ]
            elif (
                workflow.stage == Stage.VERIFYING_MERGE
                and operation.get("kind") == "verify_gitlab_merge"
            ):
                approvers = result.get("approvers") or []
                expected = operation.get("expected") or {}
                evidence_matches = (
                    str(result.get("project", "")).strip()
                    == str(expected.get("project", "")).strip()
                    and str(result.get("mr_iid", "")).strip()
                    == str(expected.get("mr_iid", "")).strip()
                    and str(result.get("tested_sha", "")).strip()
                    == str(expected.get("tested_sha", "")).strip()
                )
                stub_terminal_allowed = (
                    str(result.get("adapter_mode", "")).lower() != "stub"
                    or self.allow_stub_terminal_verification
                )
                merge_verified = (
                    stub_terminal_allowed
                    and evidence_matches
                    and result.get("verified") is True
                    and str(result.get("mr_state", "")).lower() == "merged"
                    and bool(str(result.get("merge_commit_sha", "")).strip())
                    and str(result.get("pipeline_status", "")).lower() == "success"
                    and isinstance(approvers, list)
                    and {"developer-reviewer", "tester"}.issubset(
                        {str(item) for item in approvers if str(item)}
                    )
                )
                if not merge_verified:
                    actions = self._block(
                        workflow, "GitLab adapter did not verify merge evidence"
                    )
                else:
                    workflow.artifacts["gitlab_verification"] = dict(result)
                    workflow.stage = Stage.DONE
                    workflow.current_role = None
                    workflow.active_dispatch_id = None
                    workflow.lease_until = None
                    self.store.save(workflow)
                    actions = [
                        Action(
                            "close",
                            workflow.issue_key,
                            payload={"evidence": dict(result)},
                        )
                    ]
            else:
                self.store.save(workflow)
                actions = []
            self.store.enqueue_actions(event_id, actions)
            return actions

    def _configure_development(self, workflow: Workflow) -> str | None:
        analysis = workflow.artifacts.get("analysis", {})
        raw_components = analysis.get("affected_components") or []
        if not isinstance(raw_components, list):
            return "affected_components must be an array"
        components = list(
            dict.fromkeys(str(item).strip().lower() for item in raw_components)
        )
        unknown = sorted(set(components) - set(COMPONENT_ROLES))
        if unknown:
            return f"unsupported affected components: {', '.join(unknown)}"
        workflow.affected_components = components
        if len(components) < 2:
            workflow.development_mode = "fullstack"
            workflow.development_roles = [Role.DEVELOPER.value]
            workflow.contract_version = ""
            return None

        contract = analysis.get("global_api_contract")
        if not isinstance(contract, dict):
            return "multi-component work requires global_api_contract"
        missing = sorted(REQUIRED_CONTRACT_FIELDS - set(contract))
        if missing:
            return f"global_api_contract missing fields: {', '.join(missing)}"
        version = str(contract.get("version", "")).strip()
        if not version or str(contract.get("status", "")).strip().lower() != "frozen":
            return "global_api_contract must have a version and frozen status"
        contract_components = contract.get("affected_components")
        if not isinstance(contract_components, list) or {
            str(item).strip().lower() for item in contract_components
        } != set(components):
            return "global_api_contract affected_components must match analysis"
        structured_sections = (
            "http",
            "events",
            "errors",
            "idempotency",
            "data_contract",
            "migration_compatibility",
            "acceptance_checks",
            "rollout",
            "rollback",
        )
        if any(
            not isinstance(contract.get(section), dict)
            for section in structured_sections
        ):
            return "global_api_contract structured sections must be objects"
        if not isinstance(contract.get("fixtures"), list):
            return "global_api_contract fixtures must be an array"
        ownership = contract.get("ownership")
        if not isinstance(ownership, dict) or any(
            not isinstance(ownership.get(component), list) or not ownership[component]
            for component in components
        ):
            return "global_api_contract ownership must cover every affected component"
        owned_paths: list[tuple[str, str]] = []
        for component in components:
            normalized = [
                str(path).strip() for path in ownership[component] if str(path).strip()
            ]
            if not normalized or any(not _ownership_scope(path) for path in normalized):
                return "global_api_contract contains unsafe ownership scope"
            for path in normalized:
                if any(
                    other_component != component
                    and _ownership_overlaps(path, other_path)
                    for other_component, other_path in owned_paths
                ):
                    return "global_api_contract contains overlapping ownership"
                owned_paths.append((component, path))
        workflow.development_mode = "parallel"
        workflow.development_roles = [
            COMPONENT_ROLES[item].value for item in components
        ]
        workflow.contract_version = version
        workflow.artifacts["global_api_contract"] = dict(contract)
        return None

    def _start_development(
        self, workflow: Workflow, roles: list[str] | None = None, **context: Any
    ) -> list[Action]:
        workflow.stage = Stage.DEVELOPMENT
        selected = roles or workflow.development_roles or [Role.DEVELOPER.value]
        for role_name in selected:
            if role_name in workflow.reviewed_development_roles:
                workflow.reviewed_development_roles.remove(role_name)
            workflow.development_artifacts.pop(role_name, None)
        workflow.review_queue = []
        workflow.current_review_target = ""
        actions = [
            self._dispatch(
                workflow,
                Role(role_name),
                contract_version=workflow.contract_version or None,
                **context,
            )
            for role_name in selected
        ]
        return actions

    def _start_next_development_review(self, workflow: Workflow) -> list[Action]:
        if not workflow.review_queue:
            workflow.review_queue = [
                role_name
                for role_name in workflow.development_roles
                if role_name not in workflow.reviewed_development_roles
            ]
        if not workflow.review_queue:
            workflow.stage = Stage.DEPLOYMENT
            workflow.current_review_target = ""
            return [self._dispatch(workflow, Role.DEVOPS)]
        target = workflow.review_queue[0]
        workflow.stage = Stage.DEVELOPMENT_REVIEW
        workflow.current_review_target = target
        return [
            self._dispatch(
                workflow,
                Role.DEVELOPER_REVIEWER,
                target_role=target,
                implementation=workflow.development_artifacts[target],
                contract_version=workflow.contract_version or None,
            )
        ]

    def _handle_analyst(
        self, workflow: Workflow, status: str, result: AgentResult
    ) -> list[Action]:
        if status == "needs_input":
            workflow.stage = (
                Stage.WAITING_USER_TEST
                if workflow.stage == Stage.TEST_CLARIFICATION
                else Stage.WAITING_USER_ANALYSIS
            )
            workflow.current_role = None
            self.store.save(workflow)
            return [
                Action(
                    "ask_user", workflow.issue_key, payload={"question": result.summary}
                )
            ]
        if workflow.stage == Stage.TEST_CLARIFICATION and status in {
            "clarified",
            "completed",
        }:
            workflow.artifacts["test_clarification"] = result.artifact or {
                "answer": result.summary
            }
            workflow.stage = Stage.TESTING
            return [self._dispatch(workflow, Role.TESTER, clarification=result.summary)]
        if status != "completed":
            return self._block(workflow, f"analyst returned {status}: {result.summary}")
        workflow.artifacts["analysis"] = result.artifact
        workflow.feedback = ""
        workflow.stage = Stage.ANALYSIS_REVIEW
        return [self._dispatch(workflow, Role.ANALYST_REVIEWER)]

    def _handle_analysis_reviewer(
        self, workflow: Workflow, status: str, result: AgentResult
    ) -> list[Action]:
        if status == "approved":
            workflow.artifacts["analysis_approval"] = result.artifact or {
                "summary": result.summary
            }
            workflow.feedback = ""
            configuration_error = self._configure_development(workflow)
            if configuration_error:
                return self._block(workflow, configuration_error)
            subtasks = workflow.artifacts.get("analysis", {}).get("subtasks", [])
            if not subtasks:
                return self._start_development(workflow)
            workflow.stage = Stage.CREATING_SUBTASKS
            workflow.current_role = None
            workflow.active_dispatch_id = None
            actions: list[Action] = []
            for index, subtask in enumerate(subtasks):
                operation_id = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"{workflow.issue_key}:create_subtask:{index}:"
                        f"{json.dumps(subtask, sort_keys=True)}",
                    )
                )
                operation_payload = {**dict(subtask), "operation_id": operation_id}
                workflow.pending_operations[operation_id] = self._external_operation(
                    source="jira-bot",
                    kind="create_subtask",
                    payload=operation_payload,
                    index=index,
                )
                actions.append(
                    Action(
                        "create_subtask",
                        workflow.issue_key,
                        payload=operation_payload,
                    )
                )
            self.store.save(workflow)
            return actions
        if status == "changes_requested":
            workflow.analysis_review_rounds += 1
            if workflow.analysis_review_rounds >= self.max_review_rounds:
                return self._block(workflow, "analysis review round limit reached")
            workflow.stage = Stage.ANALYSIS
            workflow.feedback = result.summary
            return [self._dispatch(workflow, Role.ANALYST, feedback=result.summary)]
        return self._block(workflow, f"analysis reviewer returned {status}")

    def _handle_developer(
        self, workflow: Workflow, status: str, result: AgentResult
    ) -> list[Action]:
        role_name = result.role.value
        if status != "completed":
            return self._block(
                workflow, f"{role_name} returned {status}: {result.summary}"
            )
        self_review = result.artifact.get("self_review")
        if (
            not isinstance(self_review, dict)
            or str(self_review.get("status", "")).lower() != "passed"
        ):
            return self._block(
                workflow, f"{role_name} completion lacks passed self-review evidence"
            )
        tests = result.artifact.get("tests")
        if (
            not isinstance(tests, dict)
            or str(tests.get("status", "")).lower() != "passed"
        ):
            return self._block(
                workflow, f"{role_name} completion lacks passed test evidence"
            )
        if not isinstance(tests.get("evidence"), list) or not tests["evidence"]:
            return self._block(
                workflow, f"{role_name} completion lacks executed test details"
            )
        commit_sha = str(result.artifact.get("commit_sha", "")).strip()
        if (
            not commit_sha
            or str(self_review.get("reviewed_sha", "")).strip() != commit_sha
        ):
            return self._block(
                workflow, f"{role_name} self-review is not fenced to commit_sha"
            )
        if not str(
            result.artifact.get("project", "")
        ).strip() or not result.artifact.get("mr_iid"):
            return self._block(
                workflow, f"{role_name} completion lacks GitLab MR evidence"
            )
        if workflow.development_mode == "parallel" and (
            str(result.artifact.get("contract_version", ""))
            != workflow.contract_version
        ):
            return self._block(
                workflow,
                f"{role_name} contract version does not match frozen {workflow.contract_version}",
            )
        workflow.development_artifacts[role_name] = dict(result.artifact)
        if workflow.development_mode == "fullstack":
            workflow.artifacts["development"] = dict(result.artifact)
        else:
            first = next(iter(workflow.development_artifacts.values()))
            workflow.artifacts["development"] = {
                "mode": "parallel",
                "contract_version": workflow.contract_version,
                "roles": dict(workflow.development_artifacts),
                "project": first.get("project"),
                "mr_iid": first.get("mr_iid"),
            }
        if workflow.active_dispatches:
            self.store.save(workflow)
            return []
        workflow.feedback = ""
        return self._start_next_development_review(workflow)

    def _handle_developer_reviewer(
        self, workflow: Workflow, status: str, result: AgentResult
    ) -> list[Action]:
        target = workflow.current_review_target or Role.DEVELOPER.value
        reported_target = str(result.artifact.get("reviewed_role", "")).strip()
        if reported_target != target:
            return self._block(
                workflow,
                f"review verdict targets {reported_target or 'nothing'}, expected {target}",
            )
        development = workflow.development_artifacts.get(
            target
        ) or workflow.artifacts.get("development", {})
        expected_sha = str(development.get("commit_sha", "")).strip()
        if str(result.artifact.get("reviewed_sha", "")).strip() != expected_sha:
            return self._block(
                workflow, f"review verdict for {target} has wrong commit SHA"
            )
        if status == "approved":
            approval = result.artifact.get("approval")
            if (
                not isinstance(approval, dict)
                or str(approval.get("status", "")).lower() != "approved"
                or not str(approval.get("id", "")).strip()
            ):
                return self._block(
                    workflow, f"review approval for {target} lacks evidence"
                )
            workflow.artifacts.setdefault("developer_approvals", {})[target] = (
                result.artifact or {"summary": result.summary}
            )
            if target not in workflow.reviewed_development_roles:
                workflow.reviewed_development_roles.append(target)
            if workflow.review_queue and workflow.review_queue[0] == target:
                workflow.review_queue.pop(0)
            workflow.feedback = ""
            return self._start_next_development_review(workflow)
        if status == "changes_requested":
            review_rounds = workflow.development_review_rounds_by_role
            review_rounds[target] = review_rounds.get(target, 0) + 1
            workflow.development_review_rounds += 1
            if review_rounds[target] >= self.max_review_rounds:
                return self._block(
                    workflow,
                    f"development review round limit reached for {target}",
                )
            workflow.feedback = result.summary
            workflow.review_queue = [target]
            workflow.current_review_target = ""
            return self._start_development(
                workflow, [target], feedback=result.summary, review_target=target
            )
        return self._block(workflow, f"developer reviewer returned {status}")

    def _handle_devops(
        self, workflow: Workflow, status: str, result: AgentResult
    ) -> list[Action]:
        if status == "deploy_failed":
            workflow.deployment_rounds += 1
            if workflow.deployment_rounds >= self.max_review_rounds:
                return self._block(workflow, "deployment round limit reached")
            workflow.artifacts["deployment_failure"] = result.artifact
            workflow.feedback = result.summary
            return self._start_development(
                workflow,
                workflow.development_roles,
                feedback=result.summary,
                logs=result.artifact,
            )
        if status != "completed":
            return self._block(workflow, f"devops returned {status}: {result.summary}")
        deployment = result.artifact
        pipeline = deployment.get("pipeline")
        readiness = deployment.get("readiness")
        checks = deployment.get("checks")
        if (
            not str(deployment.get("environment", "")).strip()
            or not str(deployment.get("deployed_sha", "")).strip()
            or not isinstance(pipeline, dict)
            or str(pipeline.get("status", "")).lower() != "success"
            or not str(pipeline.get("id", "")).strip()
            or not isinstance(readiness, dict)
            or str(readiness.get("status", "")).lower() != "passed"
            or not readiness.get("evidence")
            or not isinstance(checks, dict)
            or str(checks.get("status", "")).lower() != "passed"
            or not checks.get("evidence")
        ):
            return self._block(workflow, "devops completion lacks deployment evidence")
        if workflow.development_mode == "fullstack":
            expected_sha = str(
                workflow.artifacts.get("development", {}).get("commit_sha", "")
            )
            if str(deployment.get("deployed_sha")) != expected_sha:
                return self._block(
                    workflow, "deployed SHA does not match approved developer SHA"
                )
        else:
            expected_component_shas = {
                role: artifact.get("commit_sha")
                for role, artifact in workflow.development_artifacts.items()
            }
            if (
                deployment.get("component_shas") != expected_component_shas
                or not str(deployment.get("project", "")).strip()
                or not deployment.get("mr_iid")
            ):
                return self._block(
                    workflow,
                    "parallel deployment lacks fenced integration MR/component SHAs",
                )
        workflow.artifacts["deployment"] = dict(deployment)
        workflow.stage = Stage.CREATING_TEST_TASK
        workflow.current_role = None
        workflow.active_dispatch_id = None
        operation_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{workflow.issue_key}:create_test_task:{workflow.deployment_rounds}",
            )
        )
        operation_payload = {
            "original_request": workflow.original_request,
            "operation_id": operation_id,
        }
        workflow.pending_operations[operation_id] = self._external_operation(
            source="jira-bot",
            kind="create_test_task",
            payload=operation_payload,
        )
        self.store.save(workflow)
        return [
            Action(
                "create_test_task",
                workflow.issue_key,
                payload=operation_payload,
            )
        ]

    def _handle_tester(
        self, workflow: Workflow, status: str, result: AgentResult
    ) -> list[Action]:
        if status == "bugs_found":
            workflow.test_rounds += 1
            if workflow.test_rounds >= self.max_review_rounds:
                return self._block(workflow, "testing round limit reached")
            workflow.artifacts["test_bugs"] = result.artifact
            workflow.feedback = result.summary
            raw_components = (
                result.artifact.get("affected_components") if result.artifact else None
            )
            roles = workflow.development_roles
            if workflow.development_mode == "parallel" and isinstance(
                raw_components, list
            ):
                requested_components = [
                    str(component).strip().lower() for component in raw_components
                ]
                unknown = sorted(set(requested_components) - set(COMPONENT_ROLES))
                if unknown:
                    return self._block(
                        workflow,
                        f"tester selected unsupported components: {', '.join(unknown)}",
                    )
                selected = [
                    COMPONENT_ROLES[component].value
                    for component in requested_components
                ]
                outside_contract = sorted(set(selected) - set(workflow.development_roles))
                if outside_contract:
                    return self._block(
                        workflow,
                        "tester fix exceeds frozen component ownership: "
                        + ", ".join(outside_contract),
                    )
                if selected:
                    roles = selected
            return self._start_development(workflow, roles, feedback=result.summary)
        if status == "ambiguous_bug":
            workflow.stage = Stage.TEST_CLARIFICATION
            workflow.feedback = result.summary
            return [self._dispatch(workflow, Role.ANALYST, question=result.summary)]
        if status == "needs_input":
            workflow.stage = Stage.WAITING_USER_TEST
            workflow.current_role = None
            self.store.save(workflow)
            return [
                Action(
                    "ask_user", workflow.issue_key, payload={"question": result.summary}
                )
            ]
        if status != "approved":
            return self._block(workflow, f"tester returned {status}: {result.summary}")
        tester_evidence = result.artifact or {}
        tester_tests = tester_evidence.get("tests")
        tester_approval = tester_evidence.get("approval")
        if (
            not isinstance(tester_tests, dict)
            or str(tester_tests.get("status", "")).lower() != "passed"
            or not tester_tests.get("evidence")
            or not isinstance(tester_approval, dict)
            or str(tester_approval.get("status", "")).lower() != "approved"
            or not str(tester_approval.get("id", "")).strip()
        ):
            return self._block(workflow, "tester approval lacks test/approval evidence")
        deployed_sha = str(
            workflow.artifacts.get("deployment", {}).get("deployed_sha", "")
        ).strip()
        if workflow.development_mode == "parallel":
            merge_target = workflow.artifacts.get("deployment", {})
        else:
            merge_target = workflow.artifacts.get("development", {})
        project = str(merge_target.get("project", "")).strip()
        mr_iid = merge_target.get("mr_iid")
        tested_sha = str(tester_evidence.get("tested_sha", "")).strip()
        if not project or not mr_iid or not tested_sha or tested_sha != deployed_sha:
            return self._block(
                workflow,
                "tester approval lacks matching project, mr_iid or deployed tested_sha",
            )
        workflow.artifacts["tester_approval"] = tester_evidence
        workflow.stage = Stage.VERIFYING_MERGE
        workflow.current_role = None
        workflow.active_dispatch_id = None
        operation_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{workflow.issue_key}:verify_gitlab_merge:{project}:{mr_iid}:{tested_sha}",
            )
        )
        operation_payload = {
            "operation_id": operation_id,
            "project": project,
            "mr_iid": mr_iid,
            "tested_sha": tested_sha,
        }
        workflow.pending_operations[operation_id] = self._external_operation(
            source="gitlab-adapter",
            kind="verify_gitlab_merge",
            payload=operation_payload,
            expected={
                "project": project,
                "mr_iid": mr_iid,
                "tested_sha": tested_sha,
            },
        )
        self.store.save(workflow)
        return [
            Action(
                "verify_gitlab_merge",
                workflow.issue_key,
                payload=operation_payload,
            )
        ]

    def _block(self, workflow: Workflow, reason: str) -> list[Action]:
        workflow.stage = Stage.BLOCKED
        workflow.current_role = None
        workflow.feedback = reason
        workflow.lease_until = None
        workflow.active_dispatch_id = None
        workflow.active_dispatches.clear()
        workflow.role_leases.clear()
        self.store.save(workflow)
        self.store.append_log(workflow.issue_key, None, "blocked", {"reason": reason})
        return [Action("escalate", workflow.issue_key, payload={"reason": reason})]

    def claim(
        self,
        issue_key: str,
        role: Role,
        dispatch_id: str,
        now: float | None = None,
        lease_seconds: int = 900,
    ) -> None:
        workflow = self.get(issue_key)
        parallel = workflow.active_dispatches.get(role.value) == dispatch_id
        sequential = (
            workflow.current_role == role and workflow.active_dispatch_id == dispatch_id
        )
        if not (parallel or sequential):
            raise ValueError(
                f"{role} cannot claim stage owned by {workflow.current_role}"
            )
        current_time = time.time() if now is None else now
        lease_until = current_time + lease_seconds
        if parallel:
            workflow.role_leases[role.value] = lease_until
            workflow.role_attempts[role.value] = (
                workflow.role_attempts.get(role.value, 0) + 1
            )
        else:
            workflow.lease_until = lease_until
            workflow.attempts += 1
        self.store.save(workflow)
        self.store.append_log(issue_key, role, "claimed", {"lease_until": lease_until})

    def heartbeat(
        self,
        issue_key: str,
        role: Role,
        dispatch_id: str,
        now: float | None = None,
        lease_seconds: int = 900,
    ) -> None:
        workflow = self.get(issue_key)
        parallel = workflow.active_dispatches.get(role.value) == dispatch_id
        sequential = (
            workflow.current_role == role and workflow.active_dispatch_id == dispatch_id
        )
        if not (parallel or sequential):
            raise ValueError("heartbeat role does not own workflow")
        lease_until = (time.time() if now is None else now) + lease_seconds
        if parallel:
            workflow.role_leases[role.value] = lease_until
        else:
            workflow.lease_until = lease_until
        self.store.save(workflow)
        self.store.append_log(
            issue_key, role, "heartbeat", {"lease_until": lease_until}
        )

    def recover_expired(
        self, now: float | None = None, max_attempts: int = 3
    ) -> list[Action]:
        current_time = time.time() if now is None else now
        with self.store.transaction():
            actions: list[Action] = []
            for workflow in self.store.all_active():
                workflow_blocked = False
                for operation_id, operation in workflow.pending_operations.items():
                    if operation.get("status") != "pending":
                        continue
                    lease_until = float(
                        operation.get("lease_until")
                        or workflow.updated_at + self.external_operation_lease_seconds
                    )
                    if lease_until > current_time:
                        continue
                    attempts = int(operation.get("attempts", 1))
                    if attempts >= max_attempts:
                        actions.extend(
                            self._block(
                                workflow,
                                f"external operation {operation_id} expired {attempts} times",
                            )
                        )
                        workflow_blocked = True
                        break
                    payload = self._external_operation_payload(
                        workflow, operation_id, operation
                    )
                    kind = str(operation.get("kind", ""))
                    if payload is None or kind not in {
                        "create_subtask",
                        "create_test_task",
                        "verify_gitlab_merge",
                    }:
                        actions.extend(
                            self._block(
                                workflow,
                                f"external operation {operation_id} cannot be recovered",
                            )
                        )
                        workflow_blocked = True
                        break
                    operation["attempts"] = attempts + 1
                    operation["lease_until"] = (
                        current_time + self.external_operation_lease_seconds
                    )
                    operation["payload"] = payload
                    self.store.save(workflow)
                    actions.append(Action(kind, workflow.issue_key, payload=payload))
                if workflow_blocked:
                    continue
                for role_name in list(workflow.active_dispatches):
                    lease_until = workflow.role_leases.get(role_name)
                    if lease_until is None or lease_until > current_time:
                        continue
                    attempts = workflow.role_attempts.get(role_name, 0)
                    role = Role(role_name)
                    if attempts >= max_attempts:
                        actions.extend(
                            self._block(
                                workflow, f"{role} lease expired {attempts} times"
                            )
                        )
                        workflow_blocked = True
                        break
                    actions.append(
                        self._dispatch(workflow, role, reason="hypervisor_recovery")
                    )
                if workflow_blocked:
                    continue
                if not workflow.current_role or workflow.lease_until is None:
                    continue
                if workflow.lease_until > current_time:
                    continue
                role = workflow.current_role
                if workflow.attempts >= max_attempts:
                    actions.extend(
                        self._block(
                            workflow, f"{role} lease expired {workflow.attempts} times"
                        )
                    )
                    continue
                actions.append(
                    self._dispatch(workflow, role, reason="hypervisor_recovery")
                )
            if actions:
                recovery_id = f"hypervisor:{current_time}:{uuid.uuid4()}"
                self.store.enqueue_actions(recovery_id, actions)
            return actions
