import os
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts" / "redeploy.sh"


def _fake_docker(
    tmp_path: Path,
    *,
    healthy: bool,
    include_rabbitmq: bool = False,
    consumers_ready: bool = True,
) -> tuple[Path, Path]:
    log = tmp_path / "docker.log"
    binary = tmp_path / "docker"
    status = "healthy" if healthy else "unhealthy"
    services = "rabbitmq worker" if include_rabbitmq else "db app worker"
    if include_rabbitmq:
        ps_records = (
            f'  printf \'%s\\n\' \'{{"Service":"rabbitmq","State":"running","Health":"{status}"}}\'\n'
            '  printf \'%s\\n\' \'{"Service":"worker","State":"running","Health":""}\'\n'
        )
    else:
        ps_records = (
            f'  printf \'%s\\n\' \'{{"Service":"db","State":"running","Health":"{status}"}}\'\n'
            '  printf \'%s\\n\' \'{"Service":"app","State":"running","Health":""}\'\n'
            '  printf \'%s\\n\' \'{"Service":"worker","State":"running","Health":""}\'\n'
        )
    queues = (
        "guardian.requests workflow.requests workflow.results workflow.external-events "
        "jira.commands gitlab.commands agent.devops.requests agent.analyst.requests "
        "agent.analyst-reviewer.requests agent.developer.requests "
        "agent.backend-developer.requests agent.frontend-developer.requests "
        "agent.dba-developer.requests agent.developer-reviewer.requests "
        "agent.tester.requests"
    )
    consumer_output = (
        f"  for queue in {queues}; do printf '%s %s\\n' \"$queue\" 1; done\n"
        if consumers_ready
        else "  printf '%s %s\\n' agent.analyst.requests 0\n"
    )
    binary.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        f"printf '%s\\n' \"$*\" >> {str(log)!r}\n"
        'if test "${1:-}" = compose && test "${2:-}" = config && '
        'test "${3:-}" = --services; then\n'
        f"  printf '%s\\n' {services}\n"
        'elif test "${1:-}" = compose && test "${2:-}" = ps && '
        'test "${3:-}" = --format; then\n'
        f"{ps_records}"
        'elif test "${1:-}" = compose && test "${2:-}" = exec; then\n'
        f"{consumer_output}"
        "fi\n",
        encoding="utf-8",
    )
    binary.chmod(0o755)
    return binary, log


def _run(
    tmp_path: Path,
    *,
    healthy: bool,
    timeout: int = 2,
    include_rabbitmq: bool = False,
    consumers_ready: bool = True,
) -> subprocess.CompletedProcess[str]:
    _, log = _fake_docker(
        tmp_path,
        healthy=healthy,
        include_rabbitmq=include_rabbitmq,
        consumers_ready=consumers_ready,
    )
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "REDEPLOY_TIMEOUT_SECONDS": str(timeout),
        "REDEPLOY_POLL_SECONDS": "0",
    }
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
    )
    result.docker_log = (  # type: ignore[attr-defined]
        log.read_text(encoding="utf-8") if log.exists() else ""
    )
    return result


def test_redeploy_builds_from_source_recreates_and_waits_for_readiness(
    tmp_path: Path,
) -> None:
    result = _run(tmp_path, healthy=True)

    assert result.returncode == 0, result.stderr
    log = result.docker_log  # type: ignore[attr-defined]
    assert "compose config --quiet" in log
    assert "compose build --pull" in log
    assert "compose down --remove-orphans" in log
    assert "compose up -d --force-recreate --remove-orphans" in log
    assert "compose ps --format json" in log
    assert "all 3 services are running and ready" in result.stdout


def test_redeploy_failure_prints_status_and_logs_without_deleting_volumes(
    tmp_path: Path,
) -> None:
    result = _run(tmp_path, healthy=False, timeout=1)

    assert result.returncode != 0
    log = result.docker_log  # type: ignore[attr-defined]
    assert "compose ps --all" in log
    assert "compose logs --tail 100 db" in log
    assert "compose down --remove-orphans" in log
    assert "compose down --volumes" not in log
    assert "compose down -v" not in log


def test_redeploy_requires_functional_rabbitmq_consumers(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        healthy=True,
        timeout=1,
        include_rabbitmq=True,
        consumers_ready=False,
    )

    assert result.returncode != 0
    log = result.docker_log  # type: ignore[attr-defined]
    assert "compose exec -T rabbitmq rabbitmqctl list_queues" in log
    assert "compose logs --tail 100 rabbitmq" in log
