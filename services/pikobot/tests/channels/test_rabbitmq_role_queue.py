import pytest

from pikobot.channels.rabbitmq import RabbitMQChannel


def test_role_worker_queue_can_be_overridden_by_environment(monkeypatch) -> None:
    monkeypatch.setenv("PIKOBOT_EXPECTED_ROLE", "analyst")
    monkeypatch.setenv("PIKOBOT_RABBITMQ_INBOUND_QUEUE", "agent.analyst.requests")
    monkeypatch.setenv("PIKOBOT_RABBITMQ_INBOUND_ROUTING_KEY", "agent.analyst.requests")
    channel = RabbitMQChannel(
        {
            "enabled": True,
            "url": "amqp://localhost/",
            "allowFrom": ["*"],
        },
        bus=object(),
    )

    assert channel.config.inbound_queue == "agent.analyst.requests"
    assert channel.config.inbound_routing_key == "agent.analyst.requests"
    assert channel.config.allow_from == ["workflow-orchestrator"]


def test_role_worker_rejects_queue_for_different_role(monkeypatch) -> None:
    monkeypatch.setenv("PIKOBOT_EXPECTED_ROLE", "analyst")
    monkeypatch.setenv("PIKOBOT_RABBITMQ_INBOUND_QUEUE", "jira.commands")
    monkeypatch.setenv("PIKOBOT_RABBITMQ_INBOUND_ROUTING_KEY", "jira.commands")

    with pytest.raises(ValueError, match="expected role queue"):
        RabbitMQChannel(
            {
                "enabled": True,
                "url": "amqp://localhost/",
                "allowFrom": ["*"],
            },
            bus=object(),
        )
