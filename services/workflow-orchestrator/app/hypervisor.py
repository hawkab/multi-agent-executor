"""Independent deterministic hypervisor for expired agent leases."""

from __future__ import annotations

import os
import time
from typing import Any

import pika

from app.app import AMQP_EXCHANGE, AMQP_URL, LEASE_SECONDS, PikaPublisher
from app.runtime import Runtime
from app.workflow import WorkflowEngine, WorkflowStore

STATE_PATH = os.getenv("WORKFLOW_STATE_PATH", "/state/workflow.sqlite3")
POLL_SECONDS = int(os.getenv("HYPERVISOR_POLL_SECONDS", "30"))
MAX_ATTEMPTS = int(os.getenv("WORKFLOW_MAX_ATTEMPTS", "3"))


def open_confirmed_channel(connection: Any) -> Any:
    channel = connection.channel()
    channel.exchange_declare(
        exchange=AMQP_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.confirm_delivery()
    return channel


def main() -> None:
    while True:
        connection = None
        try:
            connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
            channel = open_confirmed_channel(connection)
            runtime = Runtime(
                WorkflowEngine(WorkflowStore(STATE_PATH)),
                PikaPublisher(channel),
                lease_seconds=LEASE_SECONDS,
            )
            print("[agent-hypervisor] monitoring leases", flush=True)
            while connection.is_open:
                runtime.run_hypervisor(MAX_ATTEMPTS)
                connection.sleep(POLL_SECONDS)
        except Exception as exc:
            print(f"[agent-hypervisor] error: {exc}", flush=True)
            time.sleep(5)
        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
