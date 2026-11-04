"""Private deployment configuration and credential injection regressions."""

import ast
import importlib.util
from pathlib import Path
import re
import runpy
import stat

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("init_env", ROOT / "scripts/init_env.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_environment_passwords_are_unique_and_private(tmp_path):
    target = tmp_path / ".env"
    assert MODULE.initialize(target, ROOT / ".env.example")
    values = dict(line.split("=", 1) for line in target.read_text().splitlines() if "=" in line)
    assert len(values["POSTGRES_PASSWORD"]) >= 32
    assert len(values["RABBITMQ_PASSWORD"]) >= 32
    assert values["POSTGRES_PASSWORD"] != values["RABBITMQ_PASSWORD"]
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert values["JIRA_PASSWORD"] == ""


def test_existing_environment_is_never_overwritten(tmp_path):
    target = tmp_path / ".env"
    target.write_text("existing configuration\n")
    assert not MODULE.initialize(target, ROOT / ".env.example")
    assert target.read_text() == "existing configuration\n"


def test_agents_receive_broker_authentication_from_environment():
    compose = (ROOT / "docker-compose.yml").read_text()
    workers = [
        service for service in re.split(r"(?m)^  [\w-]+:\s*$", compose)
        if "PIKOBOT_WORKSPACE:" in service
    ]
    assert workers
    for service in workers:
        assert "PIKOBOT_RABBITMQ_URL:" in service
        assert "${RABBITMQ_PASSWORD:?" in service
    assert "${POSTGRES_PASSWORD:?" in compose


@pytest.mark.parametrize("package", ["pikobot", "nanobot"])
def test_transport_generator_supports_external_credentials(package):
    generated = runpy.run_path(str(ROOT / "services" / package / "inst.py"))[
        "RABBITMQ_CHANNEL_CODE"
    ]
    ast.parse(generated)
    assert f'{package.upper()}_RABBITMQ_URL' in generated
