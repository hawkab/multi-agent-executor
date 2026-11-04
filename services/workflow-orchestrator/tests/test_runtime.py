import json
from pathlib import Path

import pytest

from app.runtime import Runtime, parse_agent_result
from app.hypervisor import open_confirmed_channel
from app.workflow import Action, AgentResult, Role, WorkflowEngine, WorkflowStore


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict]] = []

    def publish(self, routing_key: str, payload: dict) -> None:
        self.messages.append((routing_key, payload))


class FailOncePublisher(FakePublisher):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    def publish(self, routing_key: str, payload: dict) -> None:
        if not self.failed:
            self.failed = True
            raise RuntimeError("broker unavailable")
        super().publish(routing_key, payload)


def make_runtime(tmp_path: Path) -> tuple[Runtime, FakePublisher]:
    publisher = FakePublisher()
    engine = WorkflowEngine(WorkflowStore(tmp_path / "state.sqlite3"))
    return Runtime(engine, publisher, lease_seconds=60), publisher


def test_dispatch_uses_role_queue_and_machine_readable_contract(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    runtime.handle_request(
        {
            "issue_key": "PRJ-1",
            "summary": "Title",
            "description": "Original user wish",
            "correlationId": "guardian-1",
        }
    )

    routing_key, payload = publisher.messages[0]
    assert routing_key == "agent.analyst.requests"
    assert "Original user wish" in payload["content"]
    assert '"status"' in payload["content"]
    assert payload["replyTo"] == "workflow.results"
    assert payload["metadata"]["role"] == "analyst"
    assert payload["metadata"]["dispatchId"] == payload["correlationId"]


def test_external_jira_completion_event_resumes_developer(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    engine = runtime.engine
    engine.accept_request("PRJ-EXT", "Original", "guardian-1")
    runtime.drain_outbox()
    engine.accept_result(
        "PRJ-EXT",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact={"spec": {}, "subtasks": [{"summary": "Part", "spec": {}}]},
        ),
    )
    runtime.drain_outbox()
    actions = engine.accept_result(
        "PRJ-EXT", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    runtime.drain_outbox()
    publisher.messages.clear()

    runtime.handle_external_event(
        {
            "senderId": "jira-bot",
            "source": "jira-bot",
            "operation_id": actions[0].payload["operation_id"],
            "status": "completed",
            "result": {"issue_key": "PRJ-EXT-1"},
            "event_id": "jira:command-1:completed",
        }
    )

    assert any(key == "agent.developer.requests" for key, _ in publisher.messages)


def test_gitlab_verification_action_uses_adapter_queue(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)

    runtime.execute_action(
        Action(
            "verify_gitlab_merge",
            "PRJ-GL",
            payload={
                "operation_id": "gitlab-op-1",
                "project": "group/app",
                "mr_iid": 7,
                "tested_sha": "abc123",
            },
        ),
        command_id="outbox-1",
    )

    routing_key, payload = publisher.messages[0]
    assert routing_key == "gitlab.commands"
    assert payload["action"] == "verify_merge"
    assert payload["operation_id"] == "gitlab-op-1"


def test_hypervisor_channel_enables_publisher_confirms() -> None:
    class Channel:
        def __init__(self) -> None:
            self.confirmed = False
            self.exchanges = []

        def exchange_declare(self, **kwargs):
            self.exchanges.append(kwargs)

        def confirm_delivery(self):
            self.confirmed = True

    channel = Channel()

    class Connection:
        def channel(self):
            return channel

    assert open_confirmed_channel(Connection()) is channel
    assert channel.confirmed is True
    assert channel.exchanges


def test_outbox_claim_prevents_concurrent_publishers(tmp_path: Path) -> None:
    store = WorkflowStore(tmp_path / "state.sqlite3")
    store.enqueue_actions("event-1", [Action("comment", "PRJ-1")])

    first = store.claim_pending_actions("worker-a", now=100, lease_seconds=30)
    second = store.claim_pending_actions("worker-b", now=101, lease_seconds=30)
    expired = store.claim_pending_actions("worker-b", now=131, lease_seconds=30)

    assert [command_id for command_id, _ in first] == ["event-1:0:comment"]
    assert second == []
    assert [command_id for command_id, _ in expired] == ["event-1:0:comment"]


def test_publisher_failure_leaves_outbox_action_for_restart(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    store = WorkflowStore(db)
    failing = FailOncePublisher()
    runtime = Runtime(WorkflowEngine(store), failing, lease_seconds=60)

    try:
        runtime.handle_request(
            {"issue_key": "PRJ-OUT", "description": "wish", "event_id": "request-1"}
        )
    except RuntimeError as exc:
        assert str(exc) == "broker unavailable"
    else:
        raise AssertionError("publisher failure was not propagated")

    assert len(store.pending_actions()) == 4

    recovered_publisher = FakePublisher()
    recovered = Runtime(
        WorkflowEngine(WorkflowStore(db)), recovered_publisher, lease_seconds=60
    )
    recovered.drain_outbox()

    assert recovered_publisher.messages[0][0] == "agent.analyst.requests"
    assert WorkflowStore(db).pending_actions() == []


def test_stale_outbox_dispatch_is_superseded_without_starving_recovery(
    tmp_path: Path,
) -> None:
    store = WorkflowStore(tmp_path / "state.sqlite3")
    engine = WorkflowEngine(store)
    failing = FailOncePublisher()
    runtime = Runtime(engine, failing, lease_seconds=60)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        runtime.handle_request(
            {"issue_key": "PRJ-STALE-OUTBOX", "description": "wish", "event_id": "request"}
        )
    old_dispatch = engine.get("PRJ-STALE-OUTBOX").active_dispatch_id
    lease_until = engine.get("PRJ-STALE-OUTBOX").lease_until or 0
    engine.recover_expired(now=lease_until + 1, max_attempts=3)
    new_dispatch = engine.get("PRJ-STALE-OUTBOX").active_dispatch_id
    assert new_dispatch != old_dispatch

    publisher = FakePublisher()
    Runtime(engine, publisher, lease_seconds=60).drain_outbox()

    dispatches = [
        payload["correlationId"]
        for route, payload in publisher.messages
        if route == "agent.analyst.requests"
    ]
    assert dispatches == [new_dispatch]
    assert store.pending_actions() == []


def test_jira_publish_failure_after_dispatch_does_not_requeue_agent_command(
    tmp_path: Path,
) -> None:
    class FailJiraPublisher(FakePublisher):
        def publish(self, routing_key: str, payload: dict) -> None:
            if routing_key == "jira.commands":
                raise RuntimeError("jira route unavailable")
            super().publish(routing_key, payload)

    db = tmp_path / "workflow.sqlite3"
    store = WorkflowStore(db)
    publisher = FailJiraPublisher()
    runtime = Runtime(WorkflowEngine(store), publisher)

    with pytest.raises(RuntimeError, match="jira route unavailable"):
        runtime.handle_request(
            {"issue_key": "PRJ-OUTBOX", "description": "wish", "correlationId": "g"}
        )

    assert [key for key, _ in publisher.messages] == ["agent.analyst.requests"]
    pending_kinds = [action.kind for _, action in store.pending_actions()]
    assert pending_kinds == ["assign_role", "transition_role", "worklog"]


def test_parse_agent_result_accepts_json_code_fence_and_preserves_worklog() -> None:
    result = parse_agent_result(
        Role.ANALYST,
        """```json
        {"status":"completed","summary":"done","artifact":{"spec":{"goal":"x"}},
         "worklog":[{"step":"read","status":"done"}]}
        ```""",
        "event-1",
    )

    assert result.status == "completed"
    assert result.artifact["spec"]["goal"] == "x"
    assert result.worklog == [{"step": "read", "status": "done"}]


def test_invalid_agent_result_is_escalated_not_treated_as_success(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    runtime.handle_request({"issue_key": "PRJ-2", "description": "wish", "correlationId": "g2"})
    publisher.messages.clear()

    runtime.handle_result(
        {
            "content": "looks done",
            "correlationId": "result-2",
            "metadata": {"issueKey": "PRJ-2", "role": "analyst"},
        }
    )

    assert publisher.messages[0][0] == "jira.commands"
    assert publisher.messages[0][1]["action"] == "escalate"


def test_jira_side_effects_are_commands_for_jira_bot(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    runtime.execute_action(
        Action("ask_user", "PRJ-3", payload={"question": "Which behavior?"})
    )

    routing_key, payload = publisher.messages[-1]
    assert routing_key == "jira.commands"
    assert payload == {
        "action": "ask_user",
        "issue_key": "PRJ-3",
        "question": "Which behavior?",
    }


def test_same_correlation_and_request_are_isolated_by_issue_key(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    for issue_key in ("PRJ-A", "PRJ-B"):
        runtime.handle_request(
            {
                "issue_key": issue_key,
                "description": "Build exactly X",
                "correlationId": "shared-source-id",
            }
        )

    analyst_issues = {
        payload["metadata"]["issueKey"]
        for key, payload in publisher.messages
        if key == "agent.analyst.requests"
    }
    assert analyst_issues == {"PRJ-A", "PRJ-B"}


def test_same_correlation_with_changed_user_answer_resumes_waiting_workflow(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    runtime.handle_request(
        {"issue_key": "PRJ-5", "description": "Build X", "correlationId": "jira-PRJ-5"}
    )
    dispatch_id = publisher.messages[0][1]["correlationId"]
    runtime.handle_result(
        {
            "content": json.dumps({"status": "needs_input", "summary": "Which X?"}),
            "correlationId": dispatch_id,
            "metadata": {"issueKey": "PRJ-5", "role": "analyst"},
        }
    )
    publisher.messages.clear()

    runtime.handle_request(
        {
            "issue_key": "PRJ-5",
            "description": "Build X\nUser answer: X1",
            "correlationId": "jira-PRJ-5",
        }
    )

    assert publisher.messages[0][0] == "agent.analyst.requests"
    assert "User answer: X1" in publisher.messages[0][1]["content"]


def test_duplicate_agent_event_does_not_duplicate_jira_worklog(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    runtime.handle_request({"issue_key": "PRJ-6", "description": "wish", "correlationId": "g6"})
    dispatch_id = publisher.messages[0][1]["correlationId"]
    publisher.messages.clear()
    result = {
        "content": json.dumps(
            {"status": "completed", "summary": "done", "artifact": {"spec": {}, "subtasks": []}}
        ),
        "correlationId": dispatch_id,
        "metadata": {"issueKey": "PRJ-6", "role": "analyst"},
    }

    runtime.handle_result(result)
    first_count = len(
        [
            m
            for m in publisher.messages
            if m[0] == "jira.commands"
            and m[1]["action"] == "worklog"
            and m[1].get("status") != "transition"
        ]
    )
    runtime.handle_result(result)
    second_count = len(
        [
            m
            for m in publisher.messages
            if m[0] == "jira.commands"
            and m[1]["action"] == "worklog"
            and m[1].get("status") != "transition"
        ]
    )

    assert first_count == 1
    assert second_count == 1


def test_agent_progress_renews_lease_without_becoming_result(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    runtime.handle_request({"issue_key": "PRJ-7", "description": "wish", "correlationId": "g7"})
    workflow = runtime.engine.get("PRJ-7")
    workflow.lease_until = 1
    runtime.engine.store.save(workflow)
    publisher.messages.clear()

    dispatch_id = runtime.engine.get("PRJ-7").active_dispatch_id
    runtime.handle_result(
        {
            "content": "internal progress must not reach Jira",
            "correlationId": dispatch_id,
            "metadata": {
                "issueKey": "PRJ-7",
                "role": "analyst",
                "dispatchId": dispatch_id,
                "_progress": True,
            },
        }
    )

    assert runtime.engine.get("PRJ-7").lease_until > 1
    assert publisher.messages == []


def test_every_agent_result_is_relayed_as_traceable_jira_worklog(tmp_path: Path) -> None:
    runtime, publisher = make_runtime(tmp_path)
    runtime.handle_request({"issue_key": "PRJ-4", "description": "wish", "correlationId": "g4"})
    dispatch_id = publisher.messages[0][1]["correlationId"]
    publisher.messages.clear()
    runtime.handle_result(
        {
            "content": json.dumps(
                {
                    "status": "completed",
                    "summary": "spec ready",
                    "artifact": {"spec": {}, "subtasks": []},
                    "worklog": [{"step": "analyse", "status": "done"}],
                }
            ),
            "correlationId": dispatch_id,
            "metadata": {"issueKey": "PRJ-4", "role": "analyst"},
        }
    )

    worklogs = [payload for key, payload in publisher.messages if key == "jira.commands" and payload["action"] == "worklog"]
    assert worklogs
    assert worklogs[0]["role"] == "analyst"
    assert worklogs[0]["entries"][0]["step"] == "analyse"
