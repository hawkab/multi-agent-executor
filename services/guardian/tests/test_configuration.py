"""Keep Guardian credentials out of source defaults."""

import importlib.util
from pathlib import Path


def load_guardian():
    path = Path(__file__).parents[1] / "app/app.py"
    spec = importlib.util.spec_from_file_location("guardian_configuration_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_no_provider_credential_is_embedded_in_the_default(monkeypatch):
    monkeypatch.delenv("OLLAMA_TOKEN", raising=False)
    assert load_guardian().OLLAMA_TOKEN == ""


def test_deployment_can_supply_the_provider_credential(monkeypatch):
    monkeypatch.setenv("OLLAMA_TOKEN", "fixture-provider-token")
    assert load_guardian().OLLAMA_TOKEN == "fixture-provider-token"
