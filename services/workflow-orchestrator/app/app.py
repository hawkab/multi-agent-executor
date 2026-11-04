"""Long-running RabbitMQ adapter for the durable workflow engine."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Callable

import pika

from app.runtime import Runtime
from app.workflow import Role, WorkflowEngine, WorkflowStore

AMQP_URL = os.getenv("AMQP_URL", "amqp://rabbitmq:5672/")
AMQP_EXCHANGE = os.getenv("AMQP_EXCHANGE", "aiops.exchange")
STATE_PATH = os.getenv("WORKFLOW_STATE_PATH", "/state/workflow.sqlite3")
LEASE_SECONDS = int(os.getenv("WORKFLOW_LEASE_SECONDS", "1800"))
ALLOW_STUB_TERMINAL_VERIFICATION = os.getenv(
    "ALLOW_STUB_TERMINAL_VERIFICATION", "false"
).strip().lower() in {"1", "true", "yes"}
INPUT_QUEUES = ("workflow.requests", "workflow.results", "workflow.external-events")


class PikaPublisher:
    def __init__(self, channel: Any):
        self.channel = channel

    def publish(self, routing_key: str, payload: dict[str, Any]) -> None:
        self.channel.basic_publish(
            exchange=AMQP_EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json", delivery_mode=2
            ),
            mandatory=True,
        )


def declare_topology(channel: Any) -> None:
    channel.exchange_declare(
        exchange=AMQP_EXCHANGE, exchange_type="topic", durable=True
    )
    for queue in (*INPUT_QUEUES, "jira.commands", "gitlab.commands"):
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(exchange=AMQP_EXCHANGE, queue=queue, routing_key=queue)
    for role in Role:
        queue = f"agent.{role.value}.requests"
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(exchange=AMQP_EXCHANGE, queue=queue, routing_key=queue)


def callback(handler: Callable[[dict[str, Any]], None]):
    def consume(channel: Any, method: Any, properties: Any, body: bytes) -> None:
        try:
            payload = json.loads(body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("payload must be a JSON object")
            handler(payload)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            print(f"[workflow-orchestrator] message failed: {exc}", flush=True)
            poison = isinstance(exc, (ValueError, TypeError, json.JSONDecodeError))
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=not poison)

    return consume


def clear_stale_readiness(ready_path: Path) -> None:
    """Remove a marker left by a previously killed process before reconnecting."""
    ready_path.unlink(missing_ok=True)


def main() -> None:
    ready_path = Path(STATE_PATH).with_suffix(".ready")
    clear_stale_readiness(ready_path)
    while True:
        connection = None
        try:
            connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
            channel = connection.channel()
            channel.basic_qos(prefetch_count=10)
            declare_topology(channel)
            channel.confirm_delivery()
            store = WorkflowStore(STATE_PATH)
            store.reconcile_jsonl_worklogs()
            engine = WorkflowEngine(
                store,
                allow_stub_terminal_verification=ALLOW_STUB_TERMINAL_VERIFICATION,
            )
            runtime = Runtime(
                engine, PikaPublisher(channel), lease_seconds=LEASE_SECONDS
            )
            runtime.drain_outbox()
            channel.basic_consume(
                queue="workflow.requests",
                on_message_callback=callback(runtime.handle_request),
            )
            channel.basic_consume(
                queue="workflow.results",
                on_message_callback=callback(runtime.handle_result),
            )
            channel.basic_consume(
                queue="workflow.external-events",
                on_message_callback=callback(runtime.handle_external_event),
            )
            ready_path.parent.mkdir(parents=True, exist_ok=True)
            ready_path.write_text("ready\n", encoding="utf-8")
            print("[workflow-orchestrator] ready", flush=True)
            while connection.is_open:
                connection.process_data_events(time_limit=2)
        except Exception as exc:
            print(f"[workflow-orchestrator] fatal loop error: {exc}", flush=True)
            ready_path.unlink(missing_ok=True)
            time.sleep(5)
        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
