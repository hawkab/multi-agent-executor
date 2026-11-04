from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_project_policy_encodes_component_routing_and_review_invariants() -> None:
    text = (ROOT / "agent-workspace" / "AGENTS.md").read_text(encoding="utf-8")

    assert "backend-developer" in text
    assert "frontend-developer" in text
    assert "dba-developer" in text
    assert "two or more" in text.lower()
    assert "global API contract" in text
    assert "self-review" in text
    assert "independent review" in text


def test_parallel_specialists_use_distinct_serialized_workspaces() -> None:
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    for role, variable in (
        ("backend-developer", "AGENT_BACKEND_WORKSPACE_PATH"),
        ("frontend-developer", "AGENT_FRONTEND_WORKSPACE_PATH"),
        ("dba-developer", "AGENT_DBA_WORKSPACE_PATH"),
    ):
        service = text.split(f"  agent-{role}:", 1)[1].split("\n  agent-", 1)[0]
        assert variable in service
        assert 'PIKOBOT_MAX_CONCURRENT_REQUESTS: "1"' in service
        assert "${AGENT_WORKSPACE_PATH" not in service


def test_global_api_contract_template_has_required_fencing_sections() -> None:
    text = (ROOT / "docs" / "templates" / "global-api-contract.yaml").read_text(
        encoding="utf-8"
    )

    for section in (
        "version:",
        "status:",
        "user_outcome:",
        "affected_components:",
        "http:",
        "events:",
        "errors:",
        "idempotency:",
        "data_contract:",
        "migration_compatibility:",
        "fixtures:",
        "ownership:",
        "acceptance_checks:",
        "rollout:",
        "rollback:",
    ):
        assert section in text
