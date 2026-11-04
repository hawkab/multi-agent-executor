import importlib.util
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def jira_bot():
    path = Path(__file__).parents[1] / "app" / "app.py"
    spec = importlib.util.spec_from_file_location("jira_bot_app", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


def test_commands_queue_is_bound(jira_bot) -> None:
    assert "jira.commands" in jira_bot.INBOUND_QUEUES


def test_fetch_issues_always_includes_waiting_user_label_query(jira_bot) -> None:
    calls = []

    class Response:
        def __init__(self, issues):
            self.issues = issues

        def raise_for_status(self):
            return None

        def json(self):
            return {"issues": self.issues}

    class Session:
        def get(self, url, params, timeout):
            calls.append(params["jql"])
            if len(calls) == 1:
                return Response([{"key": "PRJ-1"}])
            return Response([{"key": "PRJ-1"}, {"key": "PRJ-2"}])

    issues = jira_bot.fetch_issues(Session())

    assert any(jira_bot.WF_LABEL_WORKFLOW_WAITING_USER in jql for jql in calls)
    assert [issue["key"] for issue in issues] == ["PRJ-1", "PRJ-2"]


def test_worklog_comment_is_traceable_and_factual(jira_bot) -> None:
    comment = jira_bot.build_worklog_comment(
        {
            "role": "developer",
            "status": "completed",
            "summary": "Implemented endpoint",
            "event_id": "event-1",
            "entries": [
                {"step": "RED", "status": "done", "evidence": "1 failed"},
                {"step": "GREEN", "status": "done", "evidence": "12 passed"},
            ],
        }
    )
    assert "developer" in comment
    assert "event-1" in comment
    assert "RED" in comment and "1 failed" in comment
    assert "GREEN" in comment and "12 passed" in comment


def test_ask_user_command_comments_waits_and_assigns_reporter(jira_bot, monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(jira_bot, "post_comment_to_jira", lambda s, key, text: calls.append(("comment", key, text)))
    monkeypatch.setattr(jira_bot, "transition_issue_to_status", lambda s, key, status: calls.append(("transition", key, status)) or True)
    monkeypatch.setattr(jira_bot, "fetch_issue_full", lambda s, key: {"key": key, "fields": {"reporter": {"name": "alice"}}})
    monkeypatch.setattr(jira_bot, "assign_issue_to_reporter", lambda s, issue: calls.append(("reporter", issue["key"])) or True)
    monkeypatch.setattr(
        jira_bot,
        "update_issue_labels",
        lambda s, key, add_labels, remove_labels: calls.append(
            ("labels", key, add_labels, remove_labels)
        ),
    )

    jira_bot.execute_jira_command(None, {"action": "ask_user", "issue_key": "PRJ-1", "question": "Which mode?"})

    assert any(call[0] == "comment" and "Which mode?" in call[2] for call in calls)
    assert ("transition", "PRJ-1", jira_bot.JIRA_WAITING_STATUS_NAME) in calls
    assert ("reporter", "PRJ-1") in calls
    assert any(
        call[0] == "labels" and jira_bot.WF_LABEL_WORKFLOW_WAITING_USER in call[2]
        for call in calls
    )


def test_waiting_user_issue_resumes_workflow_even_when_assigned_to_reporter(
    jira_bot, monkeypatch
) -> None:
    issue = {
        "key": "PRJ-9",
        "fields": {
            "summary": "Need answer",
            "description": "Original",
            "labels": [jira_bot.WF_LABEL_WORKFLOW_WAITING_USER],
            "assignee": {"name": "alice"},
            "reporter": {"name": "alice"},
            "comment": {
                "comments": [
                    {
                        "id": "10",
                        "author": {"name": "jira-bot"},
                        "body": "[BOT][QUESTION] Which mode?",
                    },
                    {
                        "id": "11",
                        "author": {"name": "alice"},
                        "body": "Use mode X1",
                    },
                ]
            },
            "status": {
                "name": jira_bot.JIRA_INITIAL_STATUS_NAME,
                "statusCategory": {"name": jira_bot.JIRA_INITIAL_STATUS_CATEGORY_NAME},
            },
        },
    }
    published = []
    labels = []
    monkeypatch.setattr(jira_bot, "fetch_myself", lambda session: {"name": "jira-bot"})
    monkeypatch.setattr(jira_bot, "fetch_issues", lambda session: [issue])
    monkeypatch.setattr(jira_bot, "fetch_issue_full", lambda session, key: issue)
    monkeypatch.setattr(
        jira_bot,
        "publish",
        lambda channel, routing_key, payload: published.append((routing_key, payload)),
    )
    monkeypatch.setattr(
        jira_bot,
        "update_issue_labels",
        lambda session, key, add_labels, remove_labels: labels.append(
            (key, add_labels, remove_labels)
        ),
    )
    monkeypatch.setattr(
        jira_bot, "transition_issue_to_status", lambda session, key, status: True
    )

    jira_bot.publish_new_issues(None, None)

    assert published[0][0] == jira_bot.ROUTING_KEY_OUT
    assert "Use mode X1" in published[0][1]["description"]
    assert published[0][1]["correlationId"] == "jira-user-answer-PRJ-9-11"
    assert jira_bot.WF_LABEL_WORKFLOW_WAITING_USER in labels[0][2]


def test_create_subtask_attaches_machine_readable_spec(jira_bot, monkeypatch) -> None:
    attached = []
    monkeypatch.setattr(jira_bot, "create_jira_subtask", lambda s, key, payload: "PRJ-2")
    monkeypatch.setattr(jira_bot, "attach_json_to_issue", lambda s, key, name, payload: attached.append((key, name, payload)))
    monkeypatch.setattr(jira_bot, "post_comment_to_jira", lambda s, key, text: None)

    jira_bot.execute_jira_command(
        None,
        {
            "action": "create_subtask",
            "issue_key": "PRJ-1",
            "summary": "Implement API",
            "spec": {"goal": "Expose endpoint", "acceptanceCriteria": ["200 OK"]},
        },
    )

    assert attached == [
        (
            "PRJ-2",
            "machine-readable-spec.json",
            {"goal": "Expose endpoint", "acceptanceCriteria": ["200 OK"]},
        )
    ]


def test_create_testing_task_uses_original_user_request(jira_bot, monkeypatch) -> None:
    created = []
    attached = []

    def create(session, parent, payload):
        created.append((parent, payload))
        return "PRJ-TEST"

    monkeypatch.setattr(jira_bot, "create_jira_subtask", create)
    monkeypatch.setattr(jira_bot, "attach_json_to_issue", lambda s, key, name, payload: attached.append((key, name, payload)))
    monkeypatch.setattr(jira_bot, "post_comment_to_jira", lambda s, key, text: None)

    jira_bot.execute_jira_command(
        None,
        {"action": "create_test_task", "issue_key": "PRJ-1", "original_request": "User wants X"},
    )

    assert created[0][1]["summary"] == "Testing: PRJ-1"
    assert attached == [
        ("PRJ-TEST", "original-user-request.json", {"original_request": "User wants X"})
    ]


def test_unknown_command_fails_closed(jira_bot) -> None:
    with pytest.raises(ValueError, match="unsupported Jira command"):
        jira_bot.execute_jira_command(None, {"action": "delete_everything", "issue_key": "PRJ-1"})


def test_jira_command_id_is_deduplicated_durably(
    jira_bot, monkeypatch, tmp_path
) -> None:
    calls = []
    monkeypatch.setattr(
        jira_bot,
        "execute_jira_command",
        lambda session, message: calls.append(message["command_id"])
        or {"issue_key": "PRJ-2"},
    )
    state_path = tmp_path / "jira-commands.sqlite3"
    message = {
        "action": "create_subtask",
        "issue_key": "PRJ-1",
        "command_id": "workflow-event:0:create_subtask",
    }

    assert jira_bot.execute_jira_command_once(None, message, state_path) == {
        "issue_key": "PRJ-2"
    }
    assert jira_bot.execute_jira_command_once(None, message, state_path) == {
        "issue_key": "PRJ-2"
    }
    assert calls == ["workflow-event:0:create_subtask"]


def test_failed_transition_is_retryable_and_not_marked_processed(
    jira_bot, monkeypatch, tmp_path
) -> None:
    calls = []
    monkeypatch.setattr(
        jira_bot,
        "transition_issue_to_status",
        lambda session, key, status: calls.append((key, status)) or False,
    )
    monkeypatch.setattr(
        jira_bot,
        "fetch_issue_full",
        lambda session, key: {"key": key, "fields": {"status": {"name": "Сделать"}}},
    )
    message = {
        "action": "transition",
        "issue_key": "PRJ-1",
        "status": "В работе",
        "command_id": "transition-1",
    }
    state_path = tmp_path / "commands.sqlite3"

    with pytest.raises(RuntimeError, match="transition was not applied"):
        jira_bot.execute_jira_command_once(None, message, state_path)
    with pytest.raises(RuntimeError, match="transition was not applied"):
        jira_bot.execute_jira_command_once(None, message, state_path)

    assert calls == [("PRJ-1", "В работе"), ("PRJ-1", "В работе")]


def test_transition_reconciles_already_applied_target(jira_bot, monkeypatch) -> None:
    monkeypatch.setattr(
        jira_bot, "transition_issue_to_status", lambda *args: False
    )
    monkeypatch.setattr(
        jira_bot,
        "fetch_issue_full",
        lambda session, key: {
            "key": key,
            "fields": {"status": {"name": "В работе"}},
        },
    )

    result = jira_bot.execute_jira_command(
        None,
        {
            "action": "transition",
            "issue_key": "PRJ-1",
            "status": "В работе",
        },
    )

    assert result == {"issue_key": "PRJ-1", "status": "В работе"}


def test_assignment_reconciles_already_applied_login(jira_bot, monkeypatch) -> None:
    monkeypatch.setattr(jira_bot, "assign_issue_to_user", lambda *args: False)
    monkeypatch.setattr(
        jira_bot,
        "fetch_issue_full",
        lambda session, key: {
            "key": key,
            "fields": {"assignee": {"name": "agent-developer"}},
        },
    )

    result = jira_bot.execute_jira_command(
        None,
        {
            "action": "assign",
            "issue_key": "PRJ-1",
            "login": "agent-developer",
        },
    )

    assert result == {"issue_key": "PRJ-1", "assignee": "agent-developer"}


def test_create_subtask_reconciles_existing_operation_marker(
    jira_bot, monkeypatch
) -> None:
    monkeypatch.setattr(
        jira_bot,
        "find_generated_subtask",
        lambda session, parent, operation_id: "PRJ-2",
    )
    monkeypatch.setattr(
        jira_bot,
        "create_jira_subtask",
        lambda *args: pytest.fail("duplicate subtask must not be created"),
    )
    monkeypatch.setattr(
        jira_bot,
        "issue_has_attachment",
        lambda session, issue_key, filename: True,
    )
    monkeypatch.setattr(jira_bot, "parent_has_operation_comment", lambda *args: True)

    result = jira_bot.execute_jira_command(
        None,
        {
            "action": "create_subtask",
            "issue_key": "PRJ-1",
            "operation_id": "operation-1",
            "summary": "Part",
            "spec": {"part": "a"},
        },
    )

    assert result == {"issue_key": "PRJ-2", "parent_issue_key": "PRJ-1"}


def test_create_test_task_reconciles_existing_operation_marker(
    jira_bot, monkeypatch
) -> None:
    monkeypatch.setattr(
        jira_bot,
        "find_generated_subtask",
        lambda session, parent, operation_id: "PRJ-TEST",
    )
    monkeypatch.setattr(
        jira_bot,
        "create_jira_subtask",
        lambda *args: pytest.fail("duplicate test task must not be created"),
    )
    monkeypatch.setattr(jira_bot, "issue_has_attachment", lambda *args: True)
    monkeypatch.setattr(jira_bot, "parent_has_operation_comment", lambda *args: True)

    result = jira_bot.execute_jira_command(
        None,
        {
            "action": "create_test_task",
            "issue_key": "PRJ-1",
            "operation_id": "test-operation-1",
            "original_request": "Original",
        },
    )

    assert result == {"issue_key": "PRJ-TEST", "parent_issue_key": "PRJ-1"}


def test_comment_command_reconciles_existing_command_marker(
    jira_bot, monkeypatch
) -> None:
    monkeypatch.setattr(
        jira_bot, "parent_has_operation_comment", lambda *args: True
    )
    monkeypatch.setattr(
        jira_bot,
        "post_comment_to_jira",
        lambda *args: pytest.fail("duplicate comment must not be posted"),
    )

    result = jira_bot.execute_jira_command(
        None,
        {
            "action": "comment",
            "issue_key": "PRJ-1",
            "command_id": "comment-command-1",
            "text": "Done",
        },
    )

    assert result == {"issue_key": "PRJ-1"}


def test_jira_completion_event_contains_stable_operation_result(jira_bot) -> None:
    event = jira_bot.build_command_completion_event(
        {
            "issue_key": "PRJ-1",
            "operation_id": "operation-1",
            "command_id": "outbox-1",
        },
        {"issue_key": "PRJ-2"},
    )

    assert event["source"] == "jira-bot"
    assert event["senderId"] == "jira-bot"
    assert event["operation_id"] == "operation-1"
    assert event["event_id"] == "jira:outbox-1:completed"
    assert event["result"] == {"issue_key": "PRJ-2"}


def test_permanent_queue_error_goes_to_dlq_without_requeue(jira_bot) -> None:
    class Channel:
        def __init__(self):
            self.published = []
            self.acked = []

        def basic_publish(self, **kwargs):
            self.published.append(kwargs)

        def basic_ack(self, delivery_tag):
            self.acked.append(delivery_tag)

    class Method:
        delivery_tag = 11

    class Properties:
        headers = {}
        content_type = "application/json"

    channel = Channel()
    jira_bot.handle_inbound_failure(
        channel,
        "jira.commands",
        Method(),
        Properties(),
        b"bad",
        ValueError("invalid command"),
    )

    assert channel.published[0]["routing_key"] == "jira.commands.dlq"
    assert channel.acked == [11]


def test_transient_queue_error_is_republished_with_bounded_retry(jira_bot) -> None:
    class Channel:
        def __init__(self):
            self.published = []
            self.acked = []

        def basic_publish(self, **kwargs):
            self.published.append(kwargs)

        def basic_ack(self, delivery_tag):
            self.acked.append(delivery_tag)

    class Method:
        delivery_tag = 12

    class Properties:
        headers = {"x-aiops-retry": 1}
        content_type = "application/json"

    channel = Channel()
    jira_bot.handle_inbound_failure(
        channel,
        "jira.commands",
        Method(),
        Properties(),
        b"{}",
        RuntimeError("Jira unavailable"),
    )

    assert channel.published[0]["routing_key"] == "jira.commands"
    assert channel.published[0]["properties"].headers["x-aiops-retry"] == 2
    assert channel.acked == [12]
