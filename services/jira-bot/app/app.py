import hashlib
import json
import os
import re
import time
from datetime import datetime
from typing import Any

import pika
import requests
from requests.auth import HTTPBasicAuth

APP_NAME = os.getenv("APP_NAME", "jira-bot")

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
AMQP_EXCHANGE = os.getenv("AMQP_EXCHANGE", "aiops.exchange")
ROUTING_KEY_OUT = os.getenv("ROUTING_KEY_OUT", "guardian.requests")
ROUTING_KEY_DIRECT_AGENT = os.getenv("ROUTING_KEY_DIRECT_AGENT", "nanobot.requests")

JIRA_BASE_URL = os.getenv("JIRA_BASE_URL", "http://jira:8080").rstrip("/")
JIRA_BROWSER_BASE_URL = os.getenv("JIRA_BROWSER_BASE_URL", JIRA_BASE_URL).rstrip("/")
JIRA_USERNAME = os.getenv("JIRA_USERNAME", "")
JIRA_PASSWORD = os.getenv("JIRA_PASSWORD", "")

JIRA_POLL_SECONDS = int(os.getenv("JIRA_POLL_SECONDS", "20"))
JIRA_MAX_RESULTS = int(os.getenv("JIRA_MAX_RESULTS", "20"))
JIRA_JQL = os.getenv(
    "JIRA_JQL",
    'assignee = currentUser() AND statusCategory = "To Do" ORDER BY created DESC',
)

JIRA_INITIAL_STATUS_NAME = os.getenv("JIRA_INITIAL_STATUS_NAME", "Сделать").strip().lower()
JIRA_INITIAL_STATUS_CATEGORY_NAME = os.getenv("JIRA_INITIAL_STATUS_CATEGORY_NAME", "To Do").strip().lower()
JIRA_IN_PROGRESS_STATUS_NAME = os.getenv("JIRA_IN_PROGRESS_STATUS_NAME", "В работе").strip()
JIRA_WAITING_STATUS_NAME = os.getenv("JIRA_WAITING_STATUS_NAME", "Ожидание").strip()

JIRA_COMMENT_ON_TAKE = os.getenv("JIRA_COMMENT_ON_TAKE", "true").strip().lower() == "true"
JIRA_FORWARD_STATUS_COMMENTS = os.getenv("JIRA_FORWARD_STATUS_COMMENTS", "false").strip().lower() == "true"
JIRA_SECURITY_REVIEWER_LOGIN = os.getenv("JIRA_SECURITY_REVIEWER_LOGIN", "").strip()

SECURITY_APPROVAL_PHRASE = os.getenv(
    "SECURITY_APPROVAL_PHRASE",
    "Согласовано, замечаний нет.",
).strip()

BOT_SECURITY_REVIEW_MARKER_PREFIX = os.getenv(
    "BOT_SECURITY_REVIEW_MARKER_PREFIX",
    "[BOT][WF][SECURITY_REVIEW]",
).strip()

WF_LABEL_GUARDIAN_REVIEW = os.getenv("WF_LABEL_GUARDIAN_REVIEW", "aiops-guardian-review").strip()
WF_LABEL_APPROVED_FOR_AGENT = os.getenv("WF_LABEL_APPROVED_FOR_AGENT", "aiops-approved-for-agent").strip()
WF_LABEL_NEED_INFO = os.getenv("WF_LABEL_NEED_INFO", "aiops-need-info").strip()
WF_LABEL_SECURITY_REVIEW = os.getenv("WF_LABEL_SECURITY_REVIEW", "aiops-security-review").strip()
WF_LABEL_AGENT_REPLIED = os.getenv("WF_LABEL_AGENT_REPLIED", "aiops-agent-replied").strip()
WF_LABEL_AGENT_QUESTION = os.getenv("WF_LABEL_AGENT_QUESTION", "aiops-agent-question").strip()

ALL_WORKFLOW_LABELS = {
    WF_LABEL_GUARDIAN_REVIEW,
    WF_LABEL_APPROVED_FOR_AGENT,
    WF_LABEL_NEED_INFO,
    WF_LABEL_SECURITY_REVIEW,
    WF_LABEL_AGENT_REPLIED,
    WF_LABEL_AGENT_QUESTION,
}

INBOUND_QUEUES = [
    "jira.comments",
    "jira.status",
    "jira.human-review",
]

CURRENT_USER_CACHE: dict[str, Any] | None = None


def log(message: str) -> None:
    print(f"[{APP_NAME}] {message}", flush=True)


def json_dumps(data: dict[str, Any]) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def connect_amqp() -> tuple[pika.BlockingConnection, pika.adapters.blocking_connection.BlockingChannel]:
    parameters = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=AMQP_EXCHANGE,
        exchange_type="topic",
        durable=True,
    )

    for queue_name in INBOUND_QUEUES:
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(
            exchange=AMQP_EXCHANGE,
            queue=queue_name,
            routing_key=queue_name,
        )

    log("connected to RabbitMQ")
    return connection, channel


def publish(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    routing_key: str,
    payload: dict[str, Any],
) -> None:
    channel.basic_publish(
        exchange=AMQP_EXCHANGE,
        routing_key=routing_key,
        body=json_dumps(payload),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
    )
    log(f"published to {routing_key}: issue={payload.get('issue_key')}")


def create_jira_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    )
    if JIRA_USERNAME:
        session.auth = HTTPBasicAuth(JIRA_USERNAME, JIRA_PASSWORD)
    return session


def extract_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts = [extract_text(item) for item in value]
        return "\n".join(part for part in parts if part).strip()
    if isinstance(value, dict):
        parts: list[str] = []
        text = value.get("text")
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
        content = value.get("content")
        if content is not None:
            nested = extract_text(content)
            if nested:
                parts.append(nested)
        return "\n".join(part for part in parts if part).strip()
    return str(value).strip()


def normalize_description(raw_value: Any) -> str:
    text = extract_text(raw_value)
    if text:
        return text
    if raw_value is None:
        return ""
    if isinstance(raw_value, str):
        return raw_value.strip()
    return json.dumps(raw_value, ensure_ascii=False)


def normalize_label(label: str) -> str:
    return label.strip().lower()


def issue_labels(issue: dict[str, Any]) -> set[str]:
    labels = ((issue.get("fields") or {}).get("labels") or [])
    return {normalize_label(str(item)) for item in labels if str(item).strip()}


def has_any_workflow_label(issue: dict[str, Any]) -> bool:
    current = issue_labels(issue)
    target = {normalize_label(x) for x in ALL_WORKFLOW_LABELS}
    return bool(current.intersection(target))


def fetch_myself(session: requests.Session) -> dict[str, Any]:
    global CURRENT_USER_CACHE
    if CURRENT_USER_CACHE is not None:
        return CURRENT_USER_CACHE

    url = f"{JIRA_BASE_URL}/rest/api/2/myself"
    response = session.get(url, timeout=20)
    response.raise_for_status()
    CURRENT_USER_CACHE = response.json()
    log(
        "current Jira user loaded: "
        f"name={CURRENT_USER_CACHE.get('name')} "
        f"key={CURRENT_USER_CACHE.get('key')} "
        f"displayName={CURRENT_USER_CACHE.get('displayName')}"
    )
    return CURRENT_USER_CACHE


def fetch_issues(session: requests.Session) -> list[dict[str, Any]]:
    url = f"{JIRA_BASE_URL}/rest/api/2/search"
    params = {
        "jql": JIRA_JQL,
        "fields": "summary,description,status,labels,assignee",
        "maxResults": JIRA_MAX_RESULTS,
    }

    response = session.get(url, params=params, timeout=20)
    response.raise_for_status()

    payload = response.json()
    return payload.get("issues", [])


def fetch_issue_full(session: requests.Session, issue_key: str) -> dict[str, Any]:
    url = f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}"
    params = {
        "fields": "summary,description,status,labels,assignee,reporter,comment",
        "expand": "changelog",
    }
    response = session.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def build_guardian_payload(issue: dict[str, Any]) -> dict[str, Any]:
    key = issue.get("key", "")
    fields = issue.get("fields", {}) or {}

    return {
        "issue_key": key,
        "summary": fields.get("summary", ""),
        "description": normalize_description(fields.get("description")),
        "status": (fields.get("status") or {}).get("name", ""),
        "source": "jira-bot",
        "senderId": "jira-bot",
        "chatId": key,
        "sessionKey": f"jira:{key}",
        "replyTo": "jira.comments",
        "correlationId": f"jira-{key}",
        "metadata": {
            "issueKey": key,
            "source": "jira-bot",
            "_suppress_progress": True,
            "_wants_stream": False,
        },
    }


def current_user_identifiers(current_user: dict[str, Any]) -> set[str]:
    values = set()
    for field in ("name", "key", "displayName", "emailAddress"):
        value = str(current_user.get(field, "")).strip().lower()
        if value:
            values.add(value)
    return values


def issue_assignee_identifiers(issue: dict[str, Any]) -> set[str]:
    assignee = ((issue.get("fields") or {}).get("assignee") or {})
    values = set()
    for field in ("name", "key", "displayName", "emailAddress"):
        value = str(assignee.get(field, "")).strip().lower()
        if value:
            values.add(value)
    return values


def issue_matches_current_user(issue: dict[str, Any], current_user: dict[str, Any]) -> bool:
    assignee_ids = issue_assignee_identifiers(issue)
    if not assignee_ids:
        return False
    return bool(assignee_ids.intersection(current_user_identifiers(current_user)))


def issue_is_in_initial_status(issue: dict[str, Any]) -> bool:
    status = ((issue.get("fields") or {}).get("status") or {})
    status_name = str(status.get("name", "")).strip().lower()
    status_category = status.get("statusCategory") or {}
    status_category_name = str(status_category.get("name", "")).strip().lower()

    return (
        status_name == JIRA_INITIAL_STATUS_NAME
        and status_category_name == JIRA_INITIAL_STATUS_CATEGORY_NAME
    )


def get_issue_transitions(session: requests.Session, issue_key: str) -> list[dict[str, Any]]:
    url = f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}/transitions"
    response = session.get(url, timeout=20)
    response.raise_for_status()
    payload = response.json()
    return payload.get("transitions", [])


def transition_issue_to_status(session: requests.Session, issue_key: str, target_status_name: str) -> bool:
    transitions = get_issue_transitions(session, issue_key)

    target = None
    for transition in transitions:
        to_status = transition.get("to") or {}
        to_name = str(to_status.get("name", "")).strip().lower()
        if to_name == target_status_name.strip().lower():
            target = transition
            break

    if target is None:
        log(f"no transition found for issue={issue_key} to status='{target_status_name}'")
        return False

    url = f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}/transitions"
    body = {"transition": {"id": str(target.get("id"))}}
    response = session.post(url, data=json.dumps(body, ensure_ascii=False), timeout=20)
    response.raise_for_status()

    log(
        f"issue transitioned: issue={issue_key}, "
        f"transitionId={target.get('id')}, "
        f"to={((target.get('to') or {}).get('name'))}"
    )
    return True


def update_issue(
    session: requests.Session,
    issue_key: str,
    *,
    fields: dict[str, Any] | None = None,
    update: dict[str, Any] | None = None,
) -> None:
    body: dict[str, Any] = {}
    if fields:
        body["fields"] = fields
    if update:
        body["update"] = update

    if not body:
        return

    url = f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}"
    response = session.put(url, data=json.dumps(body, ensure_ascii=False), timeout=20)
    response.raise_for_status()
    log(f"issue updated: {issue_key}")


def update_issue_labels(
    session: requests.Session,
    issue_key: str,
    add_labels: list[str] | None = None,
    remove_labels: list[str] | None = None,
) -> None:
    add_labels = [x.strip() for x in (add_labels or []) if x and x.strip()]
    remove_labels = [x.strip() for x in (remove_labels or []) if x and x.strip()]

    ops: list[dict[str, str]] = []
    for label in add_labels:
        ops.append({"add": label})
    for label in remove_labels:
        ops.append({"remove": label})

    if not ops:
        return

    update_issue(session, issue_key, update={"labels": ops})


def assign_issue_to_user(session: requests.Session, issue_key: str, login: str) -> bool:
    login = login.strip()
    if not login:
        return False

    try:
        update_issue(session, issue_key, fields={"assignee": {"name": login}})
        log(f"issue assigned by name: {issue_key} -> {login}")
        return True
    except Exception as first_exc:
        log(f"assign by name failed for {issue_key} -> {login}: {first_exc}")

    try:
        update_issue(session, issue_key, fields={"assignee": {"key": login}})
        log(f"issue assigned by key: {issue_key} -> {login}")
        return True
    except Exception as second_exc:
        log(f"assign by key failed for {issue_key} -> {login}: {second_exc}")

    return False


def assign_issue_to_reporter(session: requests.Session, issue: dict[str, Any]) -> bool:
    fields = issue.get("fields", {}) or {}
    reporter = fields.get("reporter") or {}
    for candidate in ("name", "key"):
        login = str(reporter.get(candidate, "")).strip()
        if login:
            return assign_issue_to_user(session, str(issue.get("key", "")).strip(), login)
    return False


def post_comment_to_jira(
    session: requests.Session,
    issue_key: str,
    text: str,
) -> None:
    if not issue_key:
        log("skip comment: issue_key is empty")
        return

    url = f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}/comment"
    body = {"body": text}

    response = session.post(url, data=json.dumps(body, ensure_ascii=False), timeout=20)
    response.raise_for_status()
    log(f"comment added to {issue_key}")


def parse_jira_datetime(value: str) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None

    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass

    return None


def normalize_for_compare(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).lower()


def build_issue_text_blob(summary: str, description: str) -> str:
    return f"summary:\n{(summary or '').strip()}\n\ndescription:\n{(description or '').strip()}"


def build_issue_text_hash(summary: str, description: str) -> str:
    blob = build_issue_text_blob(summary, description)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def user_identifiers(user: dict[str, Any] | None) -> set[str]:
    user = user or {}
    values = set()

    for field in ("name", "key", "displayName", "emailAddress"):
        value = str(user.get(field, "")).strip().lower()
        if value:
            values.add(value)

    return values


def extract_issue_summary_and_description(issue: dict[str, Any]) -> tuple[str, str]:
    fields = issue.get("fields", {}) or {}
    summary = str(fields.get("summary", "")).strip()
    description = normalize_description(fields.get("description"))
    return summary, description


def build_security_review_marker(summary: str, description: str) -> str:
    text_hash = build_issue_text_hash(summary, description)
    return f"{BOT_SECURITY_REVIEW_MARKER_PREFIX} hash={text_hash}"


def find_latest_security_review_marker(
    issue: dict[str, Any],
    bot_user: dict[str, Any],
) -> dict[str, Any] | None:
    comments = (((issue.get("fields") or {}).get("comment") or {}).get("comments") or [])
    bot_ids = user_identifiers(bot_user)

    latest = None
    marker_regex = re.compile(re.escape(BOT_SECURITY_REVIEW_MARKER_PREFIX) + r"\s+hash=([0-9a-f]{64})")

    for comment in comments:
        body = str(comment.get("body", "")).strip()
        author_ids = user_identifiers(comment.get("author") or {})
        if not bot_ids.intersection(author_ids):
            continue

        match = marker_regex.search(body)
        if not match:
            continue

        created_raw = str(comment.get("created", "")).strip()
        created_dt = parse_jira_datetime(created_raw)
        if created_dt is None:
            continue

        current = {
            "body": body,
            "hash": match.group(1),
            "created_raw": created_raw,
            "created_dt": created_dt,
        }

        if latest is None or created_dt > latest["created_dt"]:
            latest = current

    return latest


def find_security_approval_comment(
    issue: dict[str, Any],
    marker_created_dt: datetime,
) -> dict[str, Any] | None:
    comments = (((issue.get("fields") or {}).get("comment") or {}).get("comments") or [])
    approval_phrase = normalize_for_compare(SECURITY_APPROVAL_PHRASE)
    security_login = JIRA_SECURITY_REVIEWER_LOGIN.strip().lower()

    latest = None

    for comment in comments:
        author_ids = user_identifiers(comment.get("author") or {})
        if security_login not in author_ids:
            continue

        body = str(comment.get("body", "")).strip()
        if approval_phrase not in normalize_for_compare(body):
            continue

        created_raw = str(comment.get("created", "")).strip()
        created_dt = parse_jira_datetime(created_raw)
        if created_dt is None:
            continue

        if created_dt <= marker_created_dt:
            continue

        current = {
            "id": str(comment.get("id", "")).strip(),
            "body": body,
            "created_raw": created_raw,
            "created_dt": created_dt,
            "author": comment.get("author") or {},
        }

        if latest is None or created_dt > latest["created_dt"]:
            latest = current

    return latest


def summary_or_description_changed_after(
    issue: dict[str, Any],
    point_dt: datetime,
) -> bool | None:
    changelog = issue.get("changelog") or {}
    histories = changelog.get("histories")
    if not isinstance(histories, list):
        return None

    for history in histories:
        created_raw = str(history.get("created", "")).strip()
        created_dt = parse_jira_datetime(created_raw)
        if created_dt is None or created_dt <= point_dt:
            continue

        items = history.get("items") or []
        for item in items:
            field_name = str(item.get("field", "")).strip().lower()
            if field_name in {"summary", "description"}:
                return True

    return False


def build_comment_url(issue_key: str, comment_id: str) -> str:
    return (
        f"{JIRA_BROWSER_BASE_URL}/browse/{issue_key}"
        f"?focusedCommentId={comment_id}"
        f"&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel"
        f"#comment-{comment_id}"
    )


def jira_external_link(text: str, url: str) -> str:
    if not url:
        return text
    return f"[{text}|{url}]"


def build_direct_agent_content(
    issue_key: str,
    summary: str,
    description: str,
    approved_by: str,
    approved_at: str,
) -> str:
    return (
        "Ты DevOps-агент.\n"
        "Повторную проверку Guardian выполнять не нужно. Задача вручную согласована ИБ.\n\n"
        "Обязательные правила:\n"
        "1. Отвечай только на русском языке.\n"
        "2. Не рассуждай вслух.\n"
        "3. Не пиши промежуточные сообщения.\n"
        "4. Не описывай намерения и планы до результата.\n"
        "5. Не фантазируй и не придумывай отсутствующие факты.\n"
        "6. Не используй команды, которых нет в окружении.\n"
        "7. Перед действиями учитывай файлы USER.md и TOOLS.md из workspace.\n"
        "8. Если данных не хватает, задай ровно один короткий вопрос.\n"
        "9. Если дальше нельзя двигаться из-за неустранимой ошибки, верни только блокирующий итог.\n\n"
        "Формат ответа строго один из трёх:\n"
        "РЕЗУЛЬТАТ\n"
        "<краткий итог>\n"
        "<список фактически выполненных команд и проверок>\n\n"
        "БЛОКЕР\n"
        "<краткая причина остановки>\n\n"
        "ВОПРОС\n"
        "<ровно один короткий вопрос>\n\n"
        "Другие форматы запрещены.\n\n"
        f"Issue: {issue_key}\n"
        f"Summary: {summary}\n\n"
        f"Description:\n{description}\n\n"
        f"Security approval by: {approved_by}\n"
        f"Security approval at: {approved_at}\n"
    )


def build_direct_agent_payload(
    issue: dict[str, Any],
    approved_by: str,
    approved_at: str,
) -> dict[str, Any]:
    key = issue.get("key", "")
    summary, description = extract_issue_summary_and_description(issue)
    fields = issue.get("fields", {}) or {}

    return {
        "issue_key": key,
        "summary": summary,
        "description": description,
        "status": (fields.get("status") or {}).get("name", ""),
        "source": "jira-bot-security-approved",
        "senderId": "jira-bot",
        "chatId": key,
        "sessionKey": f"jira:{key}",
        "replyTo": "jira.comments",
        "correlationId": f"jira-{key}",
        "guardian": {
            "checked": False,
            "bypass": "security_approved",
            "approvedBy": approved_by,
            "approvedAt": approved_at,
        },
        "metadata": {
            "issueKey": key,
            "source": "jira-bot",
            "bypassGuardian": True,
            "securityApproved": True,
            "_suppress_progress": True,
            "_wants_stream": False,
        },
        "content": build_direct_agent_content(
            issue_key=key,
            summary=summary,
            description=description,
            approved_by=approved_by,
            approved_at=approved_at,
        ),
    }


def inspect_security_review_state(
    session: requests.Session,
    issue_key: str,
    bot_user: dict[str, Any],
) -> dict[str, Any]:
    issue = fetch_issue_full(session, issue_key)
    summary, description = extract_issue_summary_and_description(issue)

    marker = find_latest_security_review_marker(issue, bot_user)
    if marker is None:
        return {
            "action": "restart_guardian",
            "reason": "Не найден машинный маркер security review. Требуется новый цикл проверки.",
            "issue": issue,
        }

    changed_after_marker = summary_or_description_changed_after(issue, marker["created_dt"])
    if changed_after_marker is None:
        return {
            "action": "restart_guardian",
            "reason": "Не удалось получить changelog issue. Без него bypass запрещён, запускается новый цикл.",
            "issue": issue,
        }

    if changed_after_marker:
        return {
            "action": "restart_guardian",
            "reason": "Summary или description изменялись после отправки задачи на ручную проверку ИБ.",
            "issue": issue,
        }

    approval = find_security_approval_comment(issue, marker["created_dt"])
    if approval is None:
        return {
            "action": "wait_security",
            "reason": "Нет валидного комментария согласования от security-user.",
            "issue": issue,
        }

    current_hash = build_issue_text_hash(summary, description)
    if current_hash != marker["hash"]:
        return {
            "action": "restart_guardian",
            "reason": "Текущий хэш текста заявки не совпадает с хэшем, который был зафиксирован при отправке на ИБ.",
            "issue": issue,
        }

    approved_by = (
        str((approval.get("author") or {}).get("displayName", "")).strip()
        or str((approval.get("author") or {}).get("name", "")).strip()
        or JIRA_SECURITY_REVIEWER_LOGIN
    )

    approval_comment_id = str(approval.get("id", "")).strip()
    approval_comment_url = build_comment_url(issue_key, approval_comment_id) if approval_comment_id else ""

    return {
        "action": "bypass",
        "issue": issue,
        "approved_by": approved_by,
        "approved_at": approval["created_raw"],
        "approval_comment_url": approval_comment_url,
    }


def start_guardian_round(
    session: requests.Session,
    channel: pika.adapters.blocking_connection.BlockingChannel,
    issue: dict[str, Any],
    *,
    restart_reason: str | None = None,
) -> bool:
    key = issue.get("key", "")
    if not key:
        return False

    ok = transition_issue_to_status(session, key, JIRA_IN_PROGRESS_STATUS_NAME)
    if not ok:
        return False

    update_issue_labels(
        session,
        key,
        add_labels=[WF_LABEL_GUARDIAN_REVIEW],
        remove_labels=[
            WF_LABEL_APPROVED_FOR_AGENT,
            WF_LABEL_NEED_INFO,
            WF_LABEL_SECURITY_REVIEW,
            WF_LABEL_AGENT_REPLIED,
            WF_LABEL_AGENT_QUESTION,
        ],
    )

    if restart_reason:
        post_comment_to_jira(
            session,
            key,
            "[BOT] Запускаю новый круг через Guardian.\n"
            f"Причина: {restart_reason}",
        )
    elif JIRA_COMMENT_ON_TAKE:
        post_comment_to_jira(
            session,
            key,
            "[BOT] Задача принята в работу и передана на проверку Guardian.",
        )

    publish(channel, ROUTING_KEY_OUT, build_guardian_payload(issue))
    return True


def start_direct_agent_round(
    session: requests.Session,
    channel: pika.adapters.blocking_connection.BlockingChannel,
    issue: dict[str, Any],
    approved_by: str,
    approved_at: str,
    approval_comment_url: str,
) -> bool:
    key = issue.get("key", "")
    if not key:
        return False

    ok = transition_issue_to_status(session, key, JIRA_IN_PROGRESS_STATUS_NAME)
    if not ok:
        return False

    update_issue_labels(
        session,
        key,
        add_labels=[WF_LABEL_APPROVED_FOR_AGENT],
        remove_labels=[
            WF_LABEL_GUARDIAN_REVIEW,
            WF_LABEL_NEED_INFO,
            WF_LABEL_SECURITY_REVIEW,
            WF_LABEL_AGENT_REPLIED,
            WF_LABEL_AGENT_QUESTION,
        ],
    )

    link_text = jira_external_link("согласование ИБ", approval_comment_url)
    post_comment_to_jira(
        session,
        key,
        f"Задача передана агенту на выполнение после ручного согласования ({link_text}).",
    )

    publish(
        channel,
        ROUTING_KEY_DIRECT_AGENT,
        build_direct_agent_payload(issue, approved_by, approved_at),
    )
    return True


def publish_new_issues(
    session: requests.Session,
    channel: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    current_user = fetch_myself(session)
    issues = fetch_issues(session)

    for issue in issues:
        key = issue.get("key", "")
        if not key:
            continue

        if not issue_matches_current_user(issue, current_user):
            log(f"skip not assigned to current user: {key}")
            continue

        if not issue_is_in_initial_status(issue):
            log(f"skip issue not in initial status/category: {key}")
            continue

        labels = issue_labels(issue)
        has_security_review = normalize_label(WF_LABEL_SECURITY_REVIEW) in labels
        has_agent_question = normalize_label(WF_LABEL_AGENT_QUESTION) in labels

        if has_security_review:
            decision = inspect_security_review_state(session, key, current_user)

            if decision["action"] == "bypass":
                started = start_direct_agent_round(
                    session,
                    channel,
                    decision["issue"],
                    decision["approved_by"],
                    decision["approved_at"],
                    decision["approval_comment_url"],
                )
                if not started:
                    log(f"skip direct agent round because transition failed: {key}")
                continue

            if decision["action"] == "restart_guardian":
                started = start_guardian_round(
                    session,
                    channel,
                    decision["issue"],
                    restart_reason=decision["reason"],
                )
                if not started:
                    log(f"skip guardian restart because transition failed: {key}")
                continue

            log(f"skip waiting security approval: {key}")
            continue

        if has_agent_question:
            started = start_guardian_round(
                session,
                channel,
                issue,
                restart_reason="Получены уточнения после вопроса агента.",
            )
            if not started:
                log(f"skip guardian restart after agent question: {key}")
            continue

        if has_any_workflow_label(issue):
            log(f"skip issue already in workflow: {key}")
            continue

        started = start_guardian_round(session, channel, issue)
        if not started:
            log(f"skip publish because cannot transition to '{JIRA_IN_PROGRESS_STATUS_NAME}': {key}")


def sanitize_for_jira_noformat(text: str) -> str:
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = cleaned.replace("\\", "\\\\")
    cleaned = cleaned.replace("{noformat}", r"\{noformat\}")
    cleaned = cleaned.replace("{quote}", r"\{quote\}")
    return cleaned.strip()


def jira_quote(text: str) -> str:
    safe_text = sanitize_for_jira_noformat(text) or "Пустое сообщение."
    return "{quote}\n{noformat}\n" + safe_text + "\n{noformat}\n{quote}"


def jira_mention(login: str) -> str:
    return f"[~{login}]" if login else ""


def issue_key_from_message(message: dict[str, Any]) -> str:
    metadata = message.get("metadata") or {}
    return (
        str(message.get("issue_key", "")).strip()
        or str(metadata.get("issueKey", "")).strip()
        or str(metadata.get("issue_key", "")).strip()
        or str(message.get("chatId", "")).strip()
    )


def message_text(message: dict[str, Any]) -> str:
    return str(message.get("comment", "")).strip() or str(message.get("content", "")).strip()


def is_agent_response(message: dict[str, Any], source_queue: str) -> bool:
    if source_queue != "jira.comments":
        return False

    origin_service = str(message.get("origin_service", "")).strip().lower()
    if origin_service == "guardian":
        return False

    content = str(message.get("content", "")).strip()
    return bool(content)


def infer_author_label(message: dict[str, Any], source_queue: str) -> str:
    explicit = str(message.get("author_label", "")).strip()
    if explicit:
        return explicit

    origin_service = str(message.get("origin_service", "")).strip().lower()
    if origin_service == "guardian":
        return "Guardian"

    if is_agent_response(message, source_queue):
        return "AI Agent"

    return "External"


def build_relay_comment(author_label: str, raw_text: str, mention_login: str = "") -> str:
    parts: list[str] = []

    mention = jira_mention(mention_login)
    if mention:
        parts.append(mention)

    parts.append(f"{author_label}:")
    parts.append(jira_quote(raw_text))
    return "\n".join(parts)


def build_security_review_comment(raw_text: str, marker_line: str) -> str:
    parts: list[str] = []

    mention = jira_mention(JIRA_SECURITY_REVIEWER_LOGIN)
    if mention:
        parts.append(mention)

    parts.append("Guardian:")
    parts.append(jira_quote(raw_text))
    parts.append(
        "Требуется ваше участие: Guardian не согласовал автоматическое выполнение этой задачи. "
        "Задача переназначена на вас для проверки и принятия решения."
    )
    parts.append(marker_line)
    return "\n".join(parts)


def labels_for_status_message(status: str) -> tuple[list[str], list[str]]:
    status = status.strip().upper()

    if status == "GUARDIAN_REVIEW":
        return [WF_LABEL_GUARDIAN_REVIEW], [
            WF_LABEL_APPROVED_FOR_AGENT,
            WF_LABEL_NEED_INFO,
            WF_LABEL_SECURITY_REVIEW,
            WF_LABEL_AGENT_REPLIED,
            WF_LABEL_AGENT_QUESTION,
        ]

    if status == "APPROVED_FOR_AGENT":
        return [WF_LABEL_APPROVED_FOR_AGENT], [
            WF_LABEL_GUARDIAN_REVIEW,
            WF_LABEL_NEED_INFO,
            WF_LABEL_SECURITY_REVIEW,
            WF_LABEL_AGENT_QUESTION,
        ]

    if status == "NEED_INFO":
        return [WF_LABEL_NEED_INFO], [
            WF_LABEL_GUARDIAN_REVIEW,
            WF_LABEL_APPROVED_FOR_AGENT,
            WF_LABEL_SECURITY_REVIEW,
            WF_LABEL_AGENT_QUESTION,
        ]

    if status in {"BLOCKED_BY_GUARDIAN", "NEEDS_SECURITY_REVIEW"}:
        return [WF_LABEL_SECURITY_REVIEW], [
            WF_LABEL_GUARDIAN_REVIEW,
            WF_LABEL_APPROVED_FOR_AGENT,
            WF_LABEL_NEED_INFO,
            WF_LABEL_AGENT_QUESTION,
        ]

    return [], []


def classify_agent_message(raw_text: str) -> tuple[str, str]:
    text = (raw_text or "").strip()
    if not text:
        return "RESULT", "Пустой ответ агента."

    lines = text.splitlines()
    first = normalize_for_compare(lines[0]) if lines else ""

    if first in {"вопрос"}:
        body = "\n".join(lines[1:]).strip() or text
        return "QUESTION", body

    if first in {"блокер", "ошибка"}:
        body = "\n".join(lines[1:]).strip() or text
        return "BLOCKED", body

    if first in {"результат", "успех"}:
        body = "\n".join(lines[1:]).strip() or text
        return "RESULT", body

    if text.endswith("?"):
        return "QUESTION", text

    return "RESULT", text


def build_comment_from_message(message: dict[str, Any], source_queue: str) -> dict[str, Any]:
    issue_key = issue_key_from_message(message)
    raw_text = message_text(message)

    action = {
        "issue_key": issue_key,
        "text": "",
        "skip_comment": False,
        "transition_to_waiting": False,
        "assign_to_reporter": False,
        "add_labels": [],
        "remove_labels": [],
    }

    if source_queue == "jira.status":
        status = str(message.get("status", "")).strip()
        add_labels, remove_labels = labels_for_status_message(status)
        action["add_labels"] = add_labels
        action["remove_labels"] = remove_labels

        if not JIRA_FORWARD_STATUS_COMMENTS:
            action["skip_comment"] = True
            return action

        text = f"[BOT][STATUS] {status}"
        if raw_text:
            text += f"\n{raw_text}"
        action["text"] = text
        return action

    author_label = infer_author_label(message, source_queue)

    if is_agent_response(message, source_queue):
        kind, body = classify_agent_message(raw_text)

        if kind == "QUESTION":
            action["text"] = (
                build_relay_comment(author_label, body or "Требуется уточнение.")
                + "\n[BOT] Для продолжения внесите ответ в описание задачи и верните задачу исполнителю."
            )
            action["transition_to_waiting"] = True
            action["assign_to_reporter"] = True
            action["add_labels"] = [WF_LABEL_AGENT_QUESTION]
            action["remove_labels"] = [
                WF_LABEL_APPROVED_FOR_AGENT,
                WF_LABEL_GUARDIAN_REVIEW,
                WF_LABEL_AGENT_REPLIED,
            ]
            return action

        action["text"] = build_relay_comment(author_label, body or "Пустой ответ агента.")
        action["transition_to_waiting"] = True
        action["add_labels"] = [WF_LABEL_AGENT_REPLIED]
        action["remove_labels"] = [
            WF_LABEL_APPROVED_FOR_AGENT,
            WF_LABEL_GUARDIAN_REVIEW,
            WF_LABEL_AGENT_QUESTION,
        ]
        return action

    if bool(message.get("transition_to_waiting")) and str(message.get("origin_service", "")).strip().lower() == "guardian":
        action["text"] = build_relay_comment(author_label, raw_text or "Guardian вернул пустой комментарий.")
        action["transition_to_waiting"] = True
        action["add_labels"] = [WF_LABEL_NEED_INFO]
        action["remove_labels"] = [
            WF_LABEL_GUARDIAN_REVIEW,
            WF_LABEL_APPROVED_FOR_AGENT,
            WF_LABEL_AGENT_QUESTION,
        ]
        return action

    action["text"] = build_relay_comment(author_label, raw_text or "Пустое сообщение.")
    return action


def drain_inbound_messages(
    session: requests.Session,
    channel: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    for queue_name in INBOUND_QUEUES:
        while True:
            method, properties, body = channel.basic_get(queue=queue_name, auto_ack=False)
            if method is None:
                break

            try:
                message = json.loads(body.decode("utf-8"))
                issue_key = issue_key_from_message(message)

                if not issue_key:
                    log("skip inbound message: issue_key is empty")
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    continue

                if queue_name == "jira.human-review":
                    current_issue = fetch_issue_full(session, issue_key)
                    summary, description = extract_issue_summary_and_description(current_issue)
                    marker_line = build_security_review_marker(summary, description)

                    raw_text = message_text(message) or "Guardian не вернул пояснение."
                    combined_comment = build_security_review_comment(raw_text, marker_line)

                    post_comment_to_jira(session, issue_key, combined_comment)

                    if JIRA_SECURITY_REVIEWER_LOGIN:
                        assigned = assign_issue_to_user(session, issue_key, JIRA_SECURITY_REVIEWER_LOGIN)
                        if not assigned:
                            log(f"failed to assign {issue_key} to security reviewer {JIRA_SECURITY_REVIEWER_LOGIN}")

                    update_issue_labels(
                        session,
                        issue_key,
                        add_labels=[WF_LABEL_SECURITY_REVIEW],
                        remove_labels=[
                            WF_LABEL_GUARDIAN_REVIEW,
                            WF_LABEL_APPROVED_FOR_AGENT,
                            WF_LABEL_NEED_INFO,
                            WF_LABEL_AGENT_QUESTION,
                        ],
                    )

                    transitioned = transition_issue_to_status(
                        session,
                        issue_key,
                        JIRA_WAITING_STATUS_NAME,
                    )
                    if not transitioned:
                        log(
                            f"cannot transition {issue_key} "
                            f"to '{JIRA_WAITING_STATUS_NAME}' after inbound message from {queue_name}"
                        )

                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    continue

                action = build_comment_from_message(message, queue_name)

                if not bool(action.get("skip_comment")):
                    post_comment_to_jira(session, issue_key, str(action.get("text", "")).strip())

                if bool(action.get("assign_to_reporter")):
                    full_issue = fetch_issue_full(session, issue_key)
                    assigned = assign_issue_to_reporter(session, full_issue)
                    if not assigned:
                        log(f"failed to assign {issue_key} to reporter")

                update_issue_labels(
                    session,
                    issue_key,
                    add_labels=list(action.get("add_labels") or []),
                    remove_labels=list(action.get("remove_labels") or []),
                )

                if bool(action.get("transition_to_waiting")):
                    transitioned = transition_issue_to_status(
                        session,
                        issue_key,
                        JIRA_WAITING_STATUS_NAME,
                    )
                    if not transitioned:
                        log(
                            f"cannot transition {issue_key} "
                            f"to '{JIRA_WAITING_STATUS_NAME}' after inbound message from {queue_name}"
                        )

                channel.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as exc:
                log(f"failed to process message from {queue_name}: {exc}")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                break


def main() -> None:
    while True:
        connection = None
        try:
            session = create_jira_session()
            connection, channel = connect_amqp()

            next_poll_ts = 0.0

            while True:
                now = time.time()

                if now >= next_poll_ts:
                    try:
                        publish_new_issues(session, channel)
                    except Exception as exc:
                        log(f"failed to poll Jira: {exc}")
                    next_poll_ts = now + JIRA_POLL_SECONDS

                try:
                    drain_inbound_messages(session, channel)
                except Exception as exc:
                    log(f"failed to drain inbound messages: {exc}")

                time.sleep(2)

        except Exception as exc:
            log(f"fatal loop error: {exc}")
            time.sleep(5)
        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
