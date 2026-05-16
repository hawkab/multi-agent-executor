# Pikobot

RabbitMQ-driven AI agent worker for `multi-agent-executor`.

Pikobot consumes tasks from RabbitMQ, executes them with an LLM-backed agent, and publishes the result back to RabbitMQ. This embedded copy keeps only the channel required for the Jira → RabbitMQ → Guardian → Pikobot → Jira flow.

## What was kept

- RabbitMQ channel.
- CLI and gateway runtime.
- LLM providers and local/Ollama-compatible provider support.
- Agent tools needed to work inside the container.

## What was removed

- WhatsApp bridge.
- Telegram, Slack, Matrix, Discord, Email, DingTalk, Feishu, Mochat, QQ, WeCom and Weixin channels.
- Node.js bridge build.
- Upstream demo media and channel plugin documentation.

## Configuration

Default config path in this project:

```text
./pikobot-config/config.json
```

RabbitMQ integration used by `multi-agent-executor`:

```text
exchange: aiops.exchange
inbound queue: pikobot.requests
inbound routing key: pikobot.requests
outbound routing key: jira.comments
```

## Run

```bash
docker compose up -d --build pikobot
```

## Logs

```bash
docker compose logs -f pikobot
```

## CLI status

```bash
docker compose run --rm pikobot-cli status
```
