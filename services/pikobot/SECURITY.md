# Security

Pikobot is embedded into `multi-agent-executor` as a RabbitMQ-driven worker.

## Local secrets

- Keep `./pikobot-config/config.json` outside public repositories when it contains API keys.
- Use file permissions `0600` for local config files with secrets.
- Rotate API keys if the archive is shared outside the trusted environment.

## Runtime

- Run the container only on a trusted Docker host.
- Limit RabbitMQ access to internal services.
- Do not expose the gateway port to the public Internet unless authentication and network filtering are added.

## Removed surfaces

This embedded variant keeps only RabbitMQ and removes unrelated chat channels and the Node.js bridge.
