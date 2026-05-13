# Multi Agent Executor

[EN](README.md) | RU

Многоагентный исполнитель задач Jira с управлением по очередям. В состав стека входят Jira, PostgreSQL, RabbitMQ, Ollama, Guardian, Jira bot и Nanobot.

## Архитектура

### Инфраструктура

Jira хранит задачи, PostgreSQL хранит данные Jira, RabbitMQ передаёт сообщения между сервисами, Ollama предоставляет локальную LLM-точку, Guardian фильтрует опасные запросы, Nanobot выполняет согласованную агентскую работу.

![Инфраструктурная архитектура](docs/hardware_arch.png)

### Конвейер агентов

Jira-задача забирается `jira-bot`, отправляется в очередь сообщений, проверяется `guardian`, выполняется ИИ-агентом и возвращается в Jira в виде комментариев или изменений статуса.

![Конвейер агентов](docs/agent_conveyor.png)

### Отдельный агент

Каждый агент работает через настроенную модель, рабочую директорию, инструменты и RabbitMQ-канал.

![Отдельный агент](docs/individual_agent.png)

## Минимальные требования

- Docker Engine 24+
- Docker Compose v2
- 4 ядра CPU
- 8 GB RAM минимум, 16 GB+ рекомендуется для локальных моделей Ollama
- 30 GB свободного места на диске
- Свободные порты: `80`, `1234`, `5555`, `5672`, `8080`, `8081`, `11434`, `15672`, `18790`

## Настройка

Создай `.env` в корне проекта, если нужно переопределить значения по умолчанию:

```env
JIRA_USERNAME=agent-orchestrator
JIRA_PASSWORD=agent-orchestrator
JIRA_SECURITY_REVIEWER_LOGIN=security-user
JIRA_BROWSER_BASE_URL=http://localhost:8080
OLLAMA_MODEL=qwen2.5:14b
```

## Запуск

```bash
docker compose up -d jira-db rabbitmq ollama
docker exec -it ollama ollama pull qwen2.5:14b
docker compose up -d --build
```

Открой Jira и создай пользователя бота из `.env` перед назначением ему задач.

## Передеплой

```bash
git pull
docker compose pull
docker compose up -d --build --remove-orphans
```

Принудительная пересборка локальных сервисов:

```bash
docker compose build --no-cache jira-bot guardian nanobot
docker compose up -d --remove-orphans
```

## Остановка

```bash
docker compose down
```

Остановка с удалением всех Docker volumes:

```bash
docker compose down -v
```

## Логи

```bash
docker compose logs -f --tail=200
```

Только агентские сервисы:

```bash
docker compose logs -f --tail=200 jira-bot guardian nanobot
```

## URL

- Jira: <http://localhost:8080>
- RabbitMQ UI: <http://localhost:15672> / `aiops` / `aiops_secret`
- Ollama: <http://localhost:11434>
- Nanobot gateway: <http://localhost:18790>

## Протокол тестирования
[Тестирование](testing_ru.md)