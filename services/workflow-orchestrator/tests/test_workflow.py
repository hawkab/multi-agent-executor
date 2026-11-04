import json
from pathlib import Path

import pytest

from app.workflow import Action, AgentResult, Role, Stage, WorkflowEngine, WorkflowStore


@pytest.fixture()
def engine(tmp_path: Path) -> WorkflowEngine:
    return WorkflowEngine(
        WorkflowStore(tmp_path / "workflow.sqlite3"),
        max_review_rounds=3,
        allow_stub_terminal_verification=True,
    )


def fullstack_artifact(**extra: object) -> dict:
    return {
        "project": "group/app",
        "mr_iid": 7,
        "commit_sha": "abc123",
        "self_review": {
            "status": "passed",
            "reviewed_sha": "abc123",
            "findings_fixed": [],
        },
        "tests": {"status": "passed", "evidence": ["focused: passed"]},
        **extra,
    }


def review_artifact(role: Role, *, sha: str = "abc123") -> dict:
    return {
        "reviewed_role": role.value,
        "reviewed_sha": sha,
        "approval": {"status": "approved", "id": f"approval-{role.value}"},
    }


def review_findings(role: Role, *, sha: str = "abc123") -> dict:
    return {"reviewed_role": role.value, "reviewed_sha": sha}


def deployment_artifact(**extra: object) -> dict:
    return {
        "environment": "staging",
        "deployed_sha": "abc123",
        "pipeline": {"id": "pipeline-1", "status": "success"},
        "readiness": {"status": "passed", "evidence": ["health: ok"]},
        "checks": {"status": "passed", "evidence": ["integration: passed"]},
        **extra,
    }


def approved_tester_artifact(**extra: object) -> dict:
    return {
        "tested_sha": "abc123",
        "tests": {"status": "passed", "evidence": ["acceptance: passed"]},
        "approval": {"status": "approved", "id": "tester-approval"},
        **extra,
    }


def complete_test_task(
    engine: WorkflowEngine, issue_key: str, actions: list[Action]
) -> None:
    create = next(action for action in actions if action.kind == "create_test_task")
    engine.accept_external_event(
        source="jira-bot",
        operation_id=create.payload["operation_id"],
        status="completed",
        result={"issue_key": f"{issue_key}-TEST"},
        event_id=f"jira-test-task-{issue_key}",
    )


def advance_fullstack_to_devops(engine: WorkflowEngine, issue_key: str) -> None:
    engine.accept_request(issue_key, "Original", "guardian")
    engine.accept_result(
        issue_key,
        AgentResult(Role.ANALYST, "completed", artifact={"spec": {}, "subtasks": []}),
    )
    engine.accept_result(issue_key, AgentResult(Role.ANALYST_REVIEWER, "approved"))
    engine.accept_result(
        issue_key,
        AgentResult(
            Role.DEVELOPER,
            "completed",
            artifact=fullstack_artifact(project="group/app", mr_iid=7),
        ),
    )
    engine.accept_result(
        issue_key,
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )


def test_new_request_starts_analyst_after_guardian(engine: WorkflowEngine) -> None:
    actions = engine.accept_request("PRJ-1", "Original user request", "guard-1")

    assert [(a.kind, a.role) for a in actions] == [("dispatch", Role.ANALYST)]
    workflow = engine.get("PRJ-1")
    assert workflow.stage == Stage.ANALYSIS
    assert workflow.original_request == "Original user request"


def test_stage_changes_are_recorded_as_transition_events(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-TRACE", "Original", "guardian")
    engine.accept_result(
        "PRJ-TRACE",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact={"spec": {}, "subtasks": []},
        ),
    )

    log_path = engine.store.path.parent / "worklogs" / "system" / "PRJ-TRACE.jsonl"
    transitions = [
        json.loads(line)["details"]
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if json.loads(line)["event_type"] == "transition"
    ]

    assert {item["to_stage"] for item in transitions} >= {
        Stage.ANALYSIS.value,
        Stage.ANALYSIS_REVIEW.value,
    }
    transition_worklogs = [
        action
        for _, action in engine.store.pending_actions()
        if action.kind == "worklog" and action.payload.get("status") == "transition"
    ]
    assert {action.payload["to_stage"] for action in transition_worklogs} >= {
        Stage.ANALYSIS.value,
        Stage.ANALYSIS_REVIEW.value,
    }


def test_analyst_question_waits_for_user_and_resume_keeps_original_request(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-2", "Build exactly X", "guard-2")
    actions = engine.accept_result(
        "PRJ-2", AgentResult(Role.ANALYST, "needs_input", "Which X?")
    )

    assert actions[0].kind == "ask_user"
    assert engine.get("PRJ-2").stage == Stage.WAITING_USER_ANALYSIS

    resumed = engine.accept_request(
        "PRJ-2", "Build exactly X\nUser answer: X1", "guard-3"
    )
    assert resumed[0].kind == "dispatch"
    assert resumed[0].role == Role.ANALYST
    assert engine.get("PRJ-2").original_request == "Build exactly X"


def test_analysis_review_loops_until_approved(engine: WorkflowEngine) -> None:
    engine.accept_request("PRJ-3", "User request", "guard-3")
    actions = engine.accept_result(
        "PRJ-3",
        AgentResult(
            Role.ANALYST,
            "completed",
            "spec ready",
            artifact={"spec": {"goal": "x"}, "subtasks": []},
        ),
    )
    assert actions[0].role == Role.ANALYST_REVIEWER

    actions = engine.accept_result(
        "PRJ-3", AgentResult(Role.ANALYST_REVIEWER, "changes_requested", "scope drift")
    )
    assert actions[0].role == Role.ANALYST

    engine.accept_result(
        "PRJ-3",
        AgentResult(
            Role.ANALYST,
            "completed",
            "fixed",
            artifact={"spec": {"goal": "x"}, "subtasks": []},
        ),
    )
    actions = engine.accept_result(
        "PRJ-3", AgentResult(Role.ANALYST_REVIEWER, "approved", "faithful")
    )
    assert actions[0].role == Role.DEVELOPER
    assert engine.get("PRJ-3").stage == Stage.DEVELOPMENT


def test_subtasks_are_created_only_after_analysis_approval(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-SUB", "Original", "g")
    analyst_actions = engine.accept_result(
        "PRJ-SUB",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact={
                "spec": {"goal": "x"},
                "subtasks": [
                    {"summary": "Part A", "spec": {"part": "a"}},
                    {"summary": "Part B", "spec": {"part": "b"}},
                ],
            },
        ),
    )

    assert [action.kind for action in analyst_actions] == ["dispatch"]
    assert analyst_actions[0].role == Role.ANALYST_REVIEWER

    approved_actions = engine.accept_result(
        "PRJ-SUB", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )

    assert [action.kind for action in approved_actions] == [
        "create_subtask",
        "create_subtask",
    ]
    assert engine.get("PRJ-SUB").stage == Stage.CREATING_SUBTASKS
    assert engine.get("PRJ-SUB").current_role is None


def test_developer_waits_for_all_subtask_completion_events(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-ACK", "Original", "g")
    engine.accept_result(
        "PRJ-ACK",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact={
                "spec": {},
                "subtasks": [
                    {"summary": "A", "spec": {"part": "a"}},
                    {"summary": "B", "spec": {"part": "b"}},
                ],
            },
        ),
    )
    actions = engine.accept_result(
        "PRJ-ACK", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    operation_ids = [action.payload["operation_id"] for action in actions]

    first = engine.accept_external_event(
        source="jira-bot",
        operation_id=operation_ids[0],
        status="completed",
        result={"issue_key": "PRJ-ACK-1"},
        event_id="jira-event-1",
    )
    assert first == []
    assert engine.get("PRJ-ACK").stage == Stage.CREATING_SUBTASKS

    second = engine.accept_external_event(
        source="jira-bot",
        operation_id=operation_ids[1],
        status="completed",
        result={"issue_key": "PRJ-ACK-2"},
        event_id="jira-event-2",
    )
    assert second[0].kind == "dispatch"
    assert second[0].role == Role.DEVELOPER
    assert engine.get("PRJ-ACK").artifacts["jira_subtasks"] == [
        "PRJ-ACK-1",
        "PRJ-ACK-2",
    ]


def test_deploy_failure_returns_to_developer_and_success_starts_tester(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-4", "User request", "g")
    engine.accept_result(
        "PRJ-4",
        AgentResult(Role.ANALYST, "completed", artifact={"spec": {}, "subtasks": []}),
    )
    engine.accept_result("PRJ-4", AgentResult(Role.ANALYST_REVIEWER, "approved"))
    engine.accept_result(
        "PRJ-4",
        AgentResult(
            Role.DEVELOPER, "completed", artifact=fullstack_artifact(mr="internal/mr/1")
        ),
    )
    actions = engine.accept_result(
        "PRJ-4",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )
    assert actions[0].role == Role.DEVOPS

    actions = engine.accept_result(
        "PRJ-4",
        AgentResult(
            Role.DEVOPS, "deploy_failed", "startup error", artifact={"logs": "trace"}
        ),
    )
    assert actions[0].role == Role.DEVELOPER

    engine.accept_result(
        "PRJ-4",
        AgentResult(
            Role.DEVELOPER, "completed", artifact=fullstack_artifact(mr="internal/mr/1")
        ),
    )
    engine.accept_result(
        "PRJ-4",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )
    actions = engine.accept_result(
        "PRJ-4",
        AgentResult(
            Role.DEVOPS,
            "completed",
            "staging healthy",
            artifact=deployment_artifact(),
        ),
    )
    assert [action.kind for action in actions] == ["create_test_task"]
    assert engine.get("PRJ-4").stage == Stage.CREATING_TEST_TASK

    resumed = engine.accept_external_event(
        source="jira-bot",
        operation_id=actions[0].payload["operation_id"],
        status="completed",
        result={"issue_key": "PRJ-4-TEST"},
        event_id="jira-test-task-completed",
    )
    assert resumed[0].role == Role.TESTER
    assert resumed[0].payload["test_issue_key"] == "PRJ-4-TEST"
    assert engine.get("PRJ-4").stage == Stage.TESTING


def test_repeated_deploy_failures_escalate_instead_of_looping_forever(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-LOOP", "User request", "g")
    engine.accept_result(
        "PRJ-LOOP",
        AgentResult(Role.ANALYST, "completed", artifact={"spec": {}, "subtasks": []}),
    )
    engine.accept_result("PRJ-LOOP", AgentResult(Role.ANALYST_REVIEWER, "approved"))
    engine.accept_result(
        "PRJ-LOOP",
        AgentResult(Role.DEVELOPER, "completed", artifact=fullstack_artifact()),
    )
    engine.accept_result(
        "PRJ-LOOP",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )

    for _ in range(2):
        actions = engine.accept_result(
            "PRJ-LOOP", AgentResult(Role.DEVOPS, "deploy_failed", "boom")
        )
        assert actions[0].role == Role.DEVELOPER
        engine.accept_result(
            "PRJ-LOOP",
            AgentResult(Role.DEVELOPER, "completed", artifact=fullstack_artifact()),
        )
        engine.accept_result(
            "PRJ-LOOP",
            AgentResult(
                Role.DEVELOPER_REVIEWER,
                "approved",
                artifact=review_artifact(Role.DEVELOPER),
            ),
        )

    actions = engine.accept_result(
        "PRJ-LOOP", AgentResult(Role.DEVOPS, "deploy_failed", "boom")
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-LOOP").stage == Stage.BLOCKED


def test_tester_ambiguous_bug_routes_to_analyst_then_back_to_tester(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-5", "Original wish", "g")
    engine.accept_result(
        "PRJ-5",
        AgentResult(Role.ANALYST, "completed", artifact={"spec": {}, "subtasks": []}),
    )
    engine.accept_result("PRJ-5", AgentResult(Role.ANALYST_REVIEWER, "approved"))
    engine.accept_result(
        "PRJ-5",
        AgentResult(Role.DEVELOPER, "completed", artifact=fullstack_artifact(mr="mr")),
    )
    engine.accept_result(
        "PRJ-5",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )
    test_task_actions = engine.accept_result(
        "PRJ-5", AgentResult(Role.DEVOPS, "completed", artifact=deployment_artifact())
    )
    complete_test_task(engine, "PRJ-5", test_task_actions)

    actions = engine.accept_result(
        "PRJ-5", AgentResult(Role.TESTER, "ambiguous_bug", "Expected A?")
    )
    assert actions[0].role == Role.ANALYST
    actions = engine.accept_result(
        "PRJ-5", AgentResult(Role.ANALYST, "clarified", "It is a bug")
    )
    assert actions[0].role == Role.TESTER


def test_devops_completion_without_pipeline_and_readiness_evidence_blocks(
    engine: WorkflowEngine,
) -> None:
    advance_fullstack_to_devops(engine, "PRJ-DEVOPS-EVIDENCE")

    actions = engine.accept_result(
        "PRJ-DEVOPS-EVIDENCE", AgentResult(Role.DEVOPS, "completed", artifact={})
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-DEVOPS-EVIDENCE").stage == Stage.BLOCKED


def test_tester_approval_requires_tests_approval_and_deployed_sha(
    engine: WorkflowEngine,
) -> None:
    advance_fullstack_to_devops(engine, "PRJ-TEST-EVIDENCE")
    test_task = engine.accept_result(
        "PRJ-TEST-EVIDENCE",
        AgentResult(Role.DEVOPS, "completed", artifact=deployment_artifact()),
    )
    complete_test_task(engine, "PRJ-TEST-EVIDENCE", test_task)

    actions = engine.accept_result(
        "PRJ-TEST-EVIDENCE",
        AgentResult(Role.TESTER, "approved", artifact={"tested_sha": "abc123"}),
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-TEST-EVIDENCE").stage == Stage.BLOCKED


def test_two_approvals_complete_only_after_verified_gitlab_merge(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-6", "Original wish", "g")
    engine.accept_result(
        "PRJ-6",
        AgentResult(Role.ANALYST, "completed", artifact={"spec": {}, "subtasks": []}),
    )
    engine.accept_result("PRJ-6", AgentResult(Role.ANALYST_REVIEWER, "approved"))
    engine.accept_result(
        "PRJ-6",
        AgentResult(
            Role.DEVELOPER,
            "completed",
            artifact=fullstack_artifact(project="group/app", mr_iid=7),
        ),
    )
    engine.accept_result(
        "PRJ-6",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )
    test_task_actions = engine.accept_result(
        "PRJ-6", AgentResult(Role.DEVOPS, "completed", artifact=deployment_artifact())
    )
    complete_test_task(engine, "PRJ-6", test_task_actions)

    actions = engine.accept_result(
        "PRJ-6",
        AgentResult(
            Role.TESTER,
            "approved",
            artifact=approved_tester_artifact(),
        ),
    )

    assert [action.kind for action in actions] == ["verify_gitlab_merge"]
    assert engine.get("PRJ-6").stage == Stage.VERIFYING_MERGE
    assert not any(action.kind == "close" for action in actions)

    verified = engine.accept_external_event(
        source="gitlab-adapter",
        operation_id=actions[0].payload["operation_id"],
        status="completed",
        result={
            "verified": True,
            "adapter_mode": "stub",
            "project": "group/app",
            "mr_iid": 7,
            "tested_sha": "abc123",
            "mr_state": "merged",
            "merge_commit_sha": "merge123",
            "pipeline_status": "success",
            "approvers": ["developer-reviewer", "tester"],
        },
        event_id="gitlab-verify-completed",
    )

    assert any(action.kind == "close" for action in verified)
    assert engine.get("PRJ-6").stage == Stage.DONE


def test_stub_verification_cannot_close_workflow_without_explicit_opt_in(
    tmp_path: Path,
) -> None:
    restricted = WorkflowEngine(WorkflowStore(tmp_path / "restricted.sqlite3"))
    restricted.accept_request("PRJ-STUB-SAFE", "Original", "g")
    restricted.accept_result(
        "PRJ-STUB-SAFE",
        AgentResult(Role.ANALYST, "completed", artifact={"spec": {}, "subtasks": []}),
    )
    restricted.accept_result(
        "PRJ-STUB-SAFE", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    restricted.accept_result(
        "PRJ-STUB-SAFE",
        AgentResult(
            Role.DEVELOPER,
            "completed",
            artifact=fullstack_artifact(project="group/app", mr_iid=7),
        ),
    )
    restricted.accept_result(
        "PRJ-STUB-SAFE",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )
    test_task = restricted.accept_result(
        "PRJ-STUB-SAFE",
        AgentResult(Role.DEVOPS, "completed", artifact=deployment_artifact()),
    )
    complete_test_task(restricted, "PRJ-STUB-SAFE", test_task)
    verify = restricted.accept_result(
        "PRJ-STUB-SAFE",
        AgentResult(Role.TESTER, "approved", artifact=approved_tester_artifact()),
    )

    actions = restricted.accept_external_event(
        source="gitlab-adapter",
        operation_id=verify[0].payload["operation_id"],
        status="completed",
        result={
            "verified": True,
            "adapter_mode": "stub",
            "project": "group/app",
            "mr_iid": 7,
            "tested_sha": "abc123",
            "mr_state": "merged",
            "merge_commit_sha": "stub-merge-abc123",
            "pipeline_status": "success",
            "approvers": ["developer-reviewer", "tester"],
        },
        event_id="stub-terminal-attempt",
    )

    assert actions[0].kind == "escalate"
    assert restricted.get("PRJ-STUB-SAFE").stage == Stage.BLOCKED


def test_gitlab_completion_must_match_requested_mr_and_sha(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-6-MISMATCH", "Original wish", "g")
    engine.accept_result(
        "PRJ-6-MISMATCH",
        AgentResult(Role.ANALYST, "completed", artifact={"spec": {}, "subtasks": []}),
    )
    engine.accept_result(
        "PRJ-6-MISMATCH", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    engine.accept_result(
        "PRJ-6-MISMATCH",
        AgentResult(
            Role.DEVELOPER,
            "completed",
            artifact=fullstack_artifact(project="group/app", mr_iid=7),
        ),
    )
    engine.accept_result(
        "PRJ-6-MISMATCH",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )
    test_task = engine.accept_result(
        "PRJ-6-MISMATCH",
        AgentResult(Role.DEVOPS, "completed", artifact=deployment_artifact()),
    )
    complete_test_task(engine, "PRJ-6-MISMATCH", test_task)
    verify = engine.accept_result(
        "PRJ-6-MISMATCH",
        AgentResult(Role.TESTER, "approved", artifact=approved_tester_artifact()),
    )

    actions = engine.accept_external_event(
        source="gitlab-adapter",
        operation_id=verify[0].payload["operation_id"],
        status="completed",
        result={
            "verified": True,
            "project": "other/project",
            "mr_iid": 99,
            "tested_sha": "other-sha",
            "mr_state": "merged",
            "merge_commit_sha": "merge123",
            "pipeline_status": "success",
            "approvers": ["developer-reviewer", "tester"],
        },
        event_id="gitlab-mismatch-completed",
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-6-MISMATCH").stage == Stage.BLOCKED


def test_tester_approval_without_merge_evidence_blocks_completion(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-6B", "Original wish", "g")
    engine.accept_result(
        "PRJ-6B",
        AgentResult(Role.ANALYST, "completed", artifact={"spec": {}, "subtasks": []}),
    )
    engine.accept_result("PRJ-6B", AgentResult(Role.ANALYST_REVIEWER, "approved"))
    engine.accept_result(
        "PRJ-6B",
        AgentResult(Role.DEVELOPER, "completed", artifact=fullstack_artifact(mr="mr")),
    )
    engine.accept_result(
        "PRJ-6B",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(Role.DEVELOPER),
        ),
    )
    test_task_actions = engine.accept_result(
        "PRJ-6B", AgentResult(Role.DEVOPS, "completed", artifact=deployment_artifact())
    )
    complete_test_task(engine, "PRJ-6B", test_task_actions)

    actions = engine.accept_result(
        "PRJ-6B", AgentResult(Role.TESTER, "approved", artifact={})
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-6B").stage == Stage.BLOCKED


def test_stale_dispatch_token_from_same_role_is_ignored(engine: WorkflowEngine) -> None:
    engine.accept_request("PRJ-FENCE", "Original", "g")
    active = engine.get("PRJ-FENCE").active_dispatch_id

    actions = engine.accept_result(
        "PRJ-FENCE",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact={"spec": {}, "subtasks": []},
            event_id="old-dispatch",
        ),
    )

    assert actions == []
    assert engine.get("PRJ-FENCE").active_dispatch_id == active
    assert engine.get("PRJ-FENCE").stage == Stage.ANALYSIS


def test_duplicate_result_is_idempotent_and_restart_restores_state(
    tmp_path: Path,
) -> None:
    db = tmp_path / "workflow.sqlite3"
    first = WorkflowEngine(WorkflowStore(db))
    first.accept_request("PRJ-7", "Original", "g")
    result = AgentResult(
        Role.ANALYST,
        "completed",
        artifact={"spec": {}, "subtasks": []},
        event_id=first.get("PRJ-7").active_dispatch_id or "",
    )
    first.accept_result("PRJ-7", result)

    restarted = WorkflowEngine(WorkflowStore(db))
    assert restarted.get("PRJ-7").stage == Stage.ANALYSIS_REVIEW
    assert restarted.accept_result("PRJ-7", result) == []


def test_agent_result_is_appended_to_role_jsonl_worklog(tmp_path: Path) -> None:
    engine = WorkflowEngine(WorkflowStore(tmp_path / "workflow.sqlite3"))
    engine.accept_request("PRJ-9", "Original", "g")
    dispatch_id = engine.get("PRJ-9").active_dispatch_id or ""
    engine.accept_result(
        "PRJ-9",
        AgentResult(
            Role.ANALYST,
            "completed",
            "spec ready",
            artifact={"spec": {}, "subtasks": []},
            worklog=[{"step": "analyse", "status": "done", "evidence": "spec.json"}],
            event_id=dispatch_id,
        ),
    )

    path = tmp_path / "worklogs" / "analyst" / "PRJ-9.jsonl"
    assert path.exists()
    text = path.read_text(encoding="utf-8")
    assert '"event_type": "agent_result"' in text
    assert '"evidence": "spec.json"' in text


def test_reconcile_restores_jsonl_after_crash_between_db_commit_and_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    engine = WorkflowEngine(store)

    def fail_append(*args, **kwargs):
        raise OSError("simulated power loss after SQLite commit")

    monkeypatch.setattr(store, "_append_jsonl", fail_append)
    with pytest.raises(OSError, match="simulated power loss"):
        engine.accept_request("PRJ-RECOVER-LOG", "Original", "guardian")
    assert store.get("PRJ-RECOVER-LOG") is not None

    restarted = WorkflowStore(tmp_path / "workflow.sqlite3")
    restarted.reconcile_jsonl_worklogs()

    path = tmp_path / "worklogs" / "system" / "PRJ-RECOVER-LOG.jsonl"
    records = [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert any(record["event_type"] == "transition" for record in records)


def test_out_of_order_role_result_cannot_advance_workflow(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-10", "Original", "g")

    actions = engine.accept_result(
        "PRJ-10", AgentResult(Role.TESTER, "approved", event_id="stale-10")
    )

    assert actions == []
    assert engine.get("PRJ-10").stage == Stage.ANALYSIS
    assert engine.get("PRJ-10").current_role == Role.ANALYST


def test_wrong_role_result_does_not_consume_active_dispatch_event_id(
    engine: WorkflowEngine,
) -> None:
    dispatch = engine.accept_request("PRJ-WRONG-ROLE", "Original", "guardian")[0]
    dispatch_id = dispatch.payload["dispatch_id"]

    assert engine.accept_result(
        "PRJ-WRONG-ROLE",
        AgentResult(Role.TESTER, "approved", event_id=dispatch_id),
    ) == []

    actions = engine.accept_result(
        "PRJ-WRONG-ROLE",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact={"spec": {}, "subtasks": []},
            event_id=dispatch_id,
        ),
    )

    assert actions[0].role == Role.ANALYST_REVIEWER
    assert engine.get("PRJ-WRONG-ROLE").stage == Stage.ANALYSIS_REVIEW


def test_tester_cannot_expand_parallel_fix_beyond_frozen_components(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-FROZEN-FIX", "Original", "guardian")
    workflow = engine.get("PRJ-FROZEN-FIX")
    workflow.stage = Stage.TESTING
    workflow.development_mode = "parallel"
    workflow.development_roles = [
        Role.BACKEND_DEVELOPER.value,
        Role.FRONTEND_DEVELOPER.value,
    ]
    workflow.current_role = Role.TESTER
    workflow.active_dispatch_id = "tester-frozen-dispatch"
    engine.store.save(workflow)

    actions = engine.accept_result(
        "PRJ-FROZEN-FIX",
        AgentResult(
            Role.TESTER,
            "bugs_found",
            artifact={"affected_components": ["database"]},
            event_id="tester-frozen-dispatch",
        ),
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-FROZEN-FIX").stage == Stage.BLOCKED


def test_malformed_subtask_completion_blocks_without_requeue_loop(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-SUBTASK-EVIDENCE", "Original", "guardian")
    workflow = engine.get("PRJ-SUBTASK-EVIDENCE")
    workflow.stage = Stage.CREATING_SUBTASKS
    workflow.current_role = None
    workflow.active_dispatch_id = None
    workflow.pending_operations = {
        "subtask-operation": {
            "source": "jira-bot",
            "kind": "create_subtask",
            "index": 0,
            "status": "pending",
            "payload": {"operation_id": "subtask-operation"},
        }
    }
    engine.store.save(workflow)

    actions = engine.accept_external_event(
        source="jira-bot",
        operation_id="subtask-operation",
        status="completed",
        result={},
        event_id="malformed-subtask-completion",
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-SUBTASK-EVIDENCE").stage == Stage.BLOCKED


def test_hypervisor_requeues_expired_lease_then_escalates(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-8", "Original", "g")
    dispatch_id = engine.get("PRJ-8").active_dispatch_id or ""
    engine.claim("PRJ-8", Role.ANALYST, dispatch_id, now=100, lease_seconds=10)

    actions = engine.recover_expired(now=111, max_attempts=2)
    assert actions[0].kind == "dispatch"
    assert actions[0].role == Role.ANALYST

    dispatch_id = engine.get("PRJ-8").active_dispatch_id or ""
    engine.claim("PRJ-8", Role.ANALYST, dispatch_id, now=120, lease_seconds=10)
    actions = engine.recover_expired(now=131, max_attempts=2)
    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-8").stage == Stage.BLOCKED


def _complex_analysis(*components: str) -> dict:
    return {
        "spec": {"goal": "contract-first change"},
        "affected_components": list(components),
        "global_api_contract": {
            "version": "1.0.0",
            "status": "frozen",
            "user_outcome": "contract-first change",
            "affected_components": list(components),
            "ownership": {component: [f"{component}/**"] for component in components},
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
        "subtasks": [],
    }


def _developer_result(
    role: Role, dispatch_id: str, *, version: str = "1.0.0"
) -> AgentResult:
    return AgentResult(
        role,
        "completed",
        artifact={
            "contract_version": version,
            "commit_sha": f"{role.value}-sha",
            "self_review": {
                "status": "passed",
                "reviewed_sha": f"{role.value}-sha",
                "findings_fixed": [],
            },
            "tests": {"status": "passed", "evidence": ["focused: passed"]},
            "project": "group/app",
            "mr_iid": role.value,
        },
        event_id=dispatch_id,
    )


def test_two_components_dispatch_only_affected_specialists_in_parallel(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-PAR", "Change API and UI", "guardian")
    engine.accept_result(
        "PRJ-PAR",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact=_complex_analysis("backend", "frontend"),
        ),
    )

    actions = engine.accept_result(
        "PRJ-PAR", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )

    dispatches = [action for action in actions if action.kind == "dispatch"]
    assert {action.role for action in dispatches} == {
        Role.BACKEND_DEVELOPER,
        Role.FRONTEND_DEVELOPER,
    }
    assert len({action.payload["dispatch_id"] for action in dispatches}) == 2
    workflow = engine.get("PRJ-PAR")
    assert workflow.development_mode == "parallel"
    assert workflow.contract_version == "1.0.0"
    assert set(workflow.active_dispatches) == {
        Role.BACKEND_DEVELOPER.value,
        Role.FRONTEND_DEVELOPER.value,
    }


def test_parallel_contract_missing_required_boundary_is_blocked(
    engine: WorkflowEngine,
) -> None:
    artifact = _complex_analysis("backend", "frontend")
    artifact["global_api_contract"].pop("events")
    engine.accept_request("PRJ-CONTRACT-MISSING", "Change API and UI", "guardian")
    engine.accept_result(
        "PRJ-CONTRACT-MISSING",
        AgentResult(Role.ANALYST, "completed", artifact=artifact),
    )

    actions = engine.accept_result(
        "PRJ-CONTRACT-MISSING", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-CONTRACT-MISSING").stage == Stage.BLOCKED


def test_parallel_contract_rejects_nested_ownership_overlap(
    engine: WorkflowEngine,
) -> None:
    artifact = _complex_analysis("backend", "frontend")
    artifact["global_api_contract"]["ownership"] = {
        "backend": ["src/**"],
        "frontend": ["src/api/**"],
    }
    engine.accept_request("PRJ-CONTRACT-OVERLAP", "Change API and UI", "guardian")
    engine.accept_result(
        "PRJ-CONTRACT-OVERLAP",
        AgentResult(Role.ANALYST, "completed", artifact=artifact),
    )

    actions = engine.accept_result(
        "PRJ-CONTRACT-OVERLAP", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-CONTRACT-OVERLAP").stage == Stage.BLOCKED


def test_single_component_keeps_one_fullstack_developer(engine: WorkflowEngine) -> None:
    engine.accept_request("PRJ-SINGLE", "Small backend fix", "guardian")
    engine.accept_result(
        "PRJ-SINGLE",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact={
                "spec": {"goal": "small fix"},
                "affected_components": ["backend"],
                "subtasks": [],
            },
        ),
    )

    actions = engine.accept_result(
        "PRJ-SINGLE", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )

    assert [(action.kind, action.role) for action in actions] == [
        ("dispatch", Role.DEVELOPER)
    ]
    assert engine.get("PRJ-SINGLE").development_mode == "fullstack"


def test_each_parallel_developer_requires_self_review_and_independent_review(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-REV", "Change API and database", "guardian")
    engine.accept_result(
        "PRJ-REV",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact=_complex_analysis("backend", "database"),
        ),
    )
    dispatches = engine.accept_result(
        "PRJ-REV", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    by_role = {action.role: action for action in dispatches}

    assert (
        engine.accept_result(
            "PRJ-REV",
            _developer_result(
                Role.BACKEND_DEVELOPER,
                by_role[Role.BACKEND_DEVELOPER].payload["dispatch_id"],
            ),
        )
        == []
    )
    review = engine.accept_result(
        "PRJ-REV",
        _developer_result(
            Role.DBA_DEVELOPER,
            by_role[Role.DBA_DEVELOPER].payload["dispatch_id"],
        ),
    )
    assert review[0].role == Role.DEVELOPER_REVIEWER
    assert review[0].payload["target_role"] == Role.BACKEND_DEVELOPER.value

    second_review = engine.accept_result(
        "PRJ-REV",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(
                Role.BACKEND_DEVELOPER,
                sha="backend-developer-sha",
            ),
        ),
    )
    assert second_review[0].role == Role.DEVELOPER_REVIEWER
    assert second_review[0].payload["target_role"] == Role.DBA_DEVELOPER.value

    deploy = engine.accept_result(
        "PRJ-REV",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(
                Role.DBA_DEVELOPER,
                sha="dba-developer-sha",
            ),
        ),
    )
    assert deploy[0].role == Role.DEVOPS
    assert engine.get("PRJ-REV").reviewed_development_roles == [
        Role.BACKEND_DEVELOPER.value,
        Role.DBA_DEVELOPER.value,
    ]


def test_parallel_reviewer_must_identify_role_and_reviewed_sha(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-REVIEW-EVIDENCE", "Change API and UI", "guardian")
    engine.accept_result(
        "PRJ-REVIEW-EVIDENCE",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact=_complex_analysis("backend", "frontend"),
        ),
    )
    dispatches = engine.accept_result(
        "PRJ-REVIEW-EVIDENCE", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    for dispatch in dispatches:
        assert dispatch.role is not None
        engine.accept_result(
            "PRJ-REVIEW-EVIDENCE",
            _developer_result(dispatch.role, dispatch.payload["dispatch_id"]),
        )

    actions = engine.accept_result(
        "PRJ-REVIEW-EVIDENCE",
        AgentResult(Role.DEVELOPER_REVIEWER, "approved", artifact={}),
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-REVIEW-EVIDENCE").stage == Stage.BLOCKED


def test_parallel_developer_contract_version_mismatch_blocks(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-FENCE-CONTRACT", "Change API and UI", "guardian")
    engine.accept_result(
        "PRJ-FENCE-CONTRACT",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact=_complex_analysis("backend", "frontend"),
        ),
    )
    dispatches = engine.accept_result(
        "PRJ-FENCE-CONTRACT", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    backend = next(
        action for action in dispatches if action.role == Role.BACKEND_DEVELOPER
    )

    actions = engine.accept_result(
        "PRJ-FENCE-CONTRACT",
        _developer_result(
            Role.BACKEND_DEVELOPER,
            backend.payload["dispatch_id"],
            version="2.0.0",
        ),
    )

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-FENCE-CONTRACT").stage == Stage.BLOCKED


def test_parallel_success_resets_role_attempt_counter(engine: WorkflowEngine) -> None:
    engine.accept_request("PRJ-ATTEMPT", "Change API and UI", "guardian")
    engine.accept_result(
        "PRJ-ATTEMPT",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact=_complex_analysis("backend", "frontend"),
        ),
    )
    dispatches = engine.accept_result(
        "PRJ-ATTEMPT", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    backend = next(
        action for action in dispatches if action.role == Role.BACKEND_DEVELOPER
    )
    engine.claim(
        "PRJ-ATTEMPT",
        Role.BACKEND_DEVELOPER,
        backend.payload["dispatch_id"],
        now=100,
    )

    engine.accept_result(
        "PRJ-ATTEMPT",
        _developer_result(Role.BACKEND_DEVELOPER, backend.payload["dispatch_id"]),
    )

    assert Role.BACKEND_DEVELOPER.value not in engine.get("PRJ-ATTEMPT").role_attempts


def test_hypervisor_retries_expired_external_operation_with_stable_id(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-EXTERNAL", "Original", "guardian")
    workflow = engine.get("PRJ-EXTERNAL")
    workflow.stage = Stage.CREATING_TEST_TASK
    workflow.current_role = None
    workflow.active_dispatch_id = None
    workflow.lease_until = None
    workflow.pending_operations = {
        "stable-operation": {
            "source": "jira-bot",
            "kind": "create_test_task",
            "status": "pending",
            "attempts": 1,
            "lease_until": 10,
            "payload": {
                "operation_id": "stable-operation",
                "original_request": "Original",
            },
        }
    }
    engine.store.save(workflow)

    actions = engine.recover_expired(now=11, max_attempts=3)

    retry = next(action for action in actions if action.kind == "create_test_task")
    assert retry.payload["operation_id"] == "stable-operation"
    operation = engine.get("PRJ-EXTERNAL").pending_operations["stable-operation"]
    assert operation["attempts"] == 2
    assert operation["lease_until"] > 11


def test_hypervisor_blocks_external_operation_after_attempt_limit(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-EXTERNAL-LIMIT", "Original", "guardian")
    workflow = engine.get("PRJ-EXTERNAL-LIMIT")
    workflow.stage = Stage.VERIFYING_MERGE
    workflow.current_role = None
    workflow.active_dispatch_id = None
    workflow.lease_until = None
    workflow.pending_operations = {
        "expired-operation": {
            "source": "gitlab-adapter",
            "kind": "verify_gitlab_merge",
            "status": "pending",
            "attempts": 3,
            "lease_until": 10,
            "payload": {"operation_id": "expired-operation"},
        }
    }
    engine.store.save(workflow)

    actions = engine.recover_expired(now=11, max_attempts=3)

    assert actions[0].kind == "escalate"
    assert engine.get("PRJ-EXTERNAL-LIMIT").stage == Stage.BLOCKED


def test_review_round_limits_are_independent_per_implementation_role(
    engine: WorkflowEngine,
) -> None:
    engine.max_review_rounds = 2
    engine.accept_request("PRJ-ROLE-LOOPS", "Change API and database", "guardian")
    engine.accept_result(
        "PRJ-ROLE-LOOPS",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact=_complex_analysis("backend", "database"),
        ),
    )
    dispatches = engine.accept_result(
        "PRJ-ROLE-LOOPS", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    by_role = {action.role: action for action in dispatches}
    for role in (Role.BACKEND_DEVELOPER, Role.DBA_DEVELOPER):
        engine.accept_result(
            "PRJ-ROLE-LOOPS",
            _developer_result(role, by_role[role].payload["dispatch_id"]),
        )

    backend_fix = engine.accept_result(
        "PRJ-ROLE-LOOPS",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "changes_requested",
            "backend finding",
            artifact=review_findings(
                Role.BACKEND_DEVELOPER,
                sha="backend-developer-sha",
            ),
        ),
    )
    engine.accept_result(
        "PRJ-ROLE-LOOPS",
        _developer_result(
            Role.BACKEND_DEVELOPER, backend_fix[0].payload["dispatch_id"]
        ),
    )
    engine.accept_result(
        "PRJ-ROLE-LOOPS",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "approved",
            artifact=review_artifact(
                Role.BACKEND_DEVELOPER,
                sha="backend-developer-sha",
            ),
        ),
    )
    dba_fix = engine.accept_result(
        "PRJ-ROLE-LOOPS",
        AgentResult(
            Role.DEVELOPER_REVIEWER,
            "changes_requested",
            "dba finding",
            artifact=review_findings(Role.DBA_DEVELOPER, sha="dba-developer-sha"),
        ),
    )

    assert dba_fix[0].role == Role.DBA_DEVELOPER
    assert engine.get("PRJ-ROLE-LOOPS").stage == Stage.DEVELOPMENT


def test_parallel_merge_verification_uses_devops_integration_mr(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-INTEGRATION", "Change API and UI", "guardian")
    engine.accept_result(
        "PRJ-INTEGRATION",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact=_complex_analysis("backend", "frontend"),
        ),
    )
    dispatches = engine.accept_result(
        "PRJ-INTEGRATION", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    for action in dispatches:
        assert action.role is not None
        engine.accept_result(
            "PRJ-INTEGRATION",
            _developer_result(action.role, action.payload["dispatch_id"]),
        )
    for role in (Role.BACKEND_DEVELOPER, Role.FRONTEND_DEVELOPER):
        engine.accept_result(
            "PRJ-INTEGRATION",
            AgentResult(
                Role.DEVELOPER_REVIEWER,
                "approved",
                artifact=review_artifact(role, sha=f"{role.value}-sha"),
            ),
        )
    test_task = engine.accept_result(
        "PRJ-INTEGRATION",
        AgentResult(
            Role.DEVOPS,
            "completed",
            artifact=deployment_artifact(
                project="group/integration",
                mr_iid=99,
                deployed_sha="integrated-sha",
                component_shas={
                    Role.BACKEND_DEVELOPER.value: "backend-developer-sha",
                    Role.FRONTEND_DEVELOPER.value: "frontend-developer-sha",
                },
            ),
        ),
    )
    complete_test_task(engine, "PRJ-INTEGRATION", test_task)

    verify = engine.accept_result(
        "PRJ-INTEGRATION",
        AgentResult(
            Role.TESTER,
            "approved",
            artifact=approved_tester_artifact(tested_sha="integrated-sha"),
        ),
    )

    assert verify[0].payload["project"] == "group/integration"
    assert verify[0].payload["mr_iid"] == 99


def test_parallel_developer_without_self_review_is_blocked(
    engine: WorkflowEngine,
) -> None:
    engine.accept_request("PRJ-NO-SELF", "Change API and UI", "guardian")
    engine.accept_result(
        "PRJ-NO-SELF",
        AgentResult(
            Role.ANALYST,
            "completed",
            artifact=_complex_analysis("backend", "frontend"),
        ),
    )
    dispatches = engine.accept_result(
        "PRJ-NO-SELF", AgentResult(Role.ANALYST_REVIEWER, "approved")
    )
    backend = next(
        action for action in dispatches if action.role == Role.BACKEND_DEVELOPER
    )

    actions = engine.accept_result(
        "PRJ-NO-SELF",
        AgentResult(
            Role.BACKEND_DEVELOPER,
            "completed",
            artifact={"contract_version": "1.0.0", "tests": {"status": "passed"}},
            event_id=backend.payload["dispatch_id"],
        ),
    )

    assert actions[0].kind == "escalate"
    assert "self-review" in engine.get("PRJ-NO-SELF").feedback
