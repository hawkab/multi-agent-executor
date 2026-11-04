import json
import os
from typing import Any

import pika

APP_NAME = "gitlab-adapter"
ADAPTER_MODE = os.getenv("GITLAB_ADAPTER_MODE", "stub").strip().lower()
AMQP_URL = os.getenv("AMQP_URL", "amqp://rabbitmq:5672/")
AMQP_EXCHANGE = os.getenv("AMQP_EXCHANGE", "aiops.exchange")
COMMAND_QUEUE = os.getenv("GITLAB_COMMAND_QUEUE", "gitlab.commands")
EXTERNAL_EVENTS_QUEUE = os.getenv(
    "WORKFLOW_EXTERNAL_EVENTS_QUEUE", "workflow.external-events"
)
STUB_APPROVERS = [
    value.strip()
    for value in os.getenv(
        "GITLAB_STUB_APPROVERS", "developer-reviewer,tester"
    ).split(",")
    if value.strip()
]


def verify_merge_stub(command: dict[str, Any]) -> dict[str, Any]:
    project = str(command.get("project", "")).strip()
    mr_iid = command.get("mr_iid")
    tested_sha = str(command.get("tested_sha", "")).strip()
    if not project or not mr_iid or not tested_sha:
        raise ValueError("project, mr_iid and tested_sha are required")
    if len(set(STUB_APPROVERS)) < 2:
        raise ValueError("stub requires two distinct approvers")
    return {
        "verified": True,
        "adapter_mode": "stub",
        "project": project,
        "mr_iid": mr_iid,
        "tested_sha": tested_sha,
        "mr_state": "merged",
        "merge_commit_sha": f"stub-merge-{tested_sha[:12]}",
        "pipeline_status": "success",
        "approvers": STUB_APPROVERS,
    }


def verify_merge(command: dict[str, Any]) -> dict[str, Any]:
    if ADAPTER_MODE != "stub":
        raise RuntimeError(
            f"unsupported GitLab adapter mode {ADAPTER_MODE!r}; real integration is not installed"
        )
    return verify_merge_stub(command)


def build_completion_event(
    command: dict[str, Any], result: dict[str, Any]
) -> dict[str, Any]:
    operation_id = str(command.get("operation_id", "")).strip()
    command_id = str(command.get("command_id") or operation_id).strip()
    if not operation_id:
        raise ValueError("GitLab command has no operation_id")
    return {
        "senderId": APP_NAME,
        "source": APP_NAME,
        "issue_key": str(command.get("issue_key", "")).strip(),
        "operation_id": operation_id,
        "status": "completed",
        "result": result,
        "event_id": f"gitlab:{command_id}:completed",
    }


def publish(
    channel: Any,
    routing_key: str,
    payload: dict[str, Any],
) -> None:
    channel.basic_publish(
        exchange=AMQP_EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json", delivery_mode=2
        ),
        mandatory=True,
    )


def main() -> None:
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.exchange_declare(
        exchange=AMQP_EXCHANGE, exchange_type="topic", durable=True
    )
    for queue_name in (
        COMMAND_QUEUE,
        f"{COMMAND_QUEUE}.dlq",
        EXTERNAL_EVENTS_QUEUE,
    ):
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(
            exchange=AMQP_EXCHANGE,
            queue=queue_name,
            routing_key=queue_name,
        )
    channel.confirm_delivery()
    channel.basic_qos(prefetch_count=1)

    def callback(channel: Any, method: Any, properties: Any, body: bytes) -> None:
        try:
            command = json.loads(body.decode("utf-8"))
            if str(command.get("action", "")) != "verify_merge":
                raise ValueError("unsupported GitLab command")
            result = verify_merge(command)
            publish(
                channel,
                EXTERNAL_EVENTS_QUEUE,
                build_completion_event(command, result),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            publish(
                channel,
                f"{COMMAND_QUEUE}.dlq",
                {
                    "error": type(exc).__name__,
                    "message": str(exc),
                    "original": body.decode("utf-8", errors="replace"),
                },
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=callback)
    print(f"[{APP_NAME}] ready mode={ADAPTER_MODE}", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
