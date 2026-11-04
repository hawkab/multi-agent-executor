import pytest

from app.app import build_completion_event, verify_merge_stub


def test_stub_returns_deterministic_explicit_verification() -> None:
    command = {
        "action": "verify_merge",
        "issue_key": "PRJ-1",
        "operation_id": "gitlab-op-1",
        "command_id": "outbox-1",
        "project": "group/app",
        "mr_iid": 7,
        "tested_sha": "abc123def456",
    }

    result = verify_merge_stub(command)

    assert result == {
        "verified": True,
        "adapter_mode": "stub",
        "project": "group/app",
        "mr_iid": 7,
        "tested_sha": "abc123def456",
        "mr_state": "merged",
        "merge_commit_sha": "stub-merge-abc123def456",
        "pipeline_status": "success",
        "approvers": ["developer-reviewer", "tester"],
    }


def test_stub_rejects_incomplete_verification_command() -> None:
    with pytest.raises(ValueError, match="project, mr_iid and tested_sha"):
        verify_merge_stub(
            {
                "action": "verify_merge",
                "issue_key": "PRJ-1",
                "operation_id": "gitlab-op-1",
            }
        )


def test_completion_event_has_trusted_sender_and_stable_identity() -> None:
    command = {
        "issue_key": "PRJ-1",
        "operation_id": "gitlab-op-1",
        "command_id": "outbox-1",
    }
    result = {"verified": True, "adapter_mode": "stub"}

    event = build_completion_event(command, result)

    assert event["senderId"] == "gitlab-adapter"
    assert event["source"] == "gitlab-adapter"
    assert event["operation_id"] == "gitlab-op-1"
    assert event["event_id"] == "gitlab:outbox-1:completed"
    assert event["result"] == result
