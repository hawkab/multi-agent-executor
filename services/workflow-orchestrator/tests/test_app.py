from app.app import callback, clear_stale_readiness, declare_topology


class FakeChannel:
    def __init__(self) -> None:
        self.acks = []
        self.nacks = []

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacks.append((delivery_tag, requeue))


def test_topology_declares_all_specialist_role_queues() -> None:
    class TopologyChannel:
        def __init__(self) -> None:
            self.queues = []

        def exchange_declare(self, **kwargs):
            pass

        def queue_declare(self, queue, durable):
            self.queues.append(queue)

        def queue_bind(self, **kwargs):
            pass

    channel = TopologyChannel()
    declare_topology(channel)

    assert {
        "agent.backend-developer.requests",
        "agent.frontend-developer.requests",
        "agent.dba-developer.requests",
    } <= set(channel.queues)


def test_clear_stale_readiness_removes_marker_before_connection(tmp_path) -> None:
    ready = tmp_path / "workflow.ready"
    ready.write_text("ready\n", encoding="utf-8")

    clear_stale_readiness(ready)

    assert not ready.exists()


class Method:
    delivery_tag = 7


def test_callback_requeues_transient_handler_failure() -> None:
    channel = FakeChannel()

    def transient(payload):
        raise RuntimeError("sqlite temporarily locked")

    callback(transient)(channel, Method(), None, b'{"issue_key":"PRJ-1"}')

    assert channel.nacks == [(7, True)]


def test_callback_drops_poison_payload() -> None:
    channel = FakeChannel()

    callback(lambda payload: None)(channel, Method(), None, b'not-json')

    assert channel.nacks == [(7, False)]
