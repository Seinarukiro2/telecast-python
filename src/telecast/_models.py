"""Domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


class Priority(StrEnum):
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


PRIORITY_WEIGHTS: dict[Priority, int] = {
    Priority.HIGH: 5,
    Priority.NORMAL: 3,
    Priority.LOW: 1,
}


class TaskKind(StrEnum):
    SEND_MESSAGE = "send_message"
    EDIT_MESSAGE = "edit_message"
    SEND_PHOTO = "send_photo"
    SEND_DOCUMENT = "send_document"


class TaskState(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    SENT = "sent"
    FAILED = "failed"
    DEAD = "dead"


class CampaignStatus(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class LocaleStrategy(StrEnum):
    PER_USER = "per_user"
    FIXED = "fixed"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Task ──────────────────────────────────────────────────────────────


@dataclass
class Task:
    """A message to deliver via Telegram Bot API."""

    chat_id: int
    text: str = ""
    template_key: str = ""
    locale: str = ""
    vars: dict[str, Any] | None = None
    kind: TaskKind = TaskKind.SEND_MESSAGE
    parse_mode: str = ""
    disable_web_page_preview: bool = False
    disable_notification: bool = False
    reply_markup: Any | None = None
    reply_to_message_id: int = 0
    photo: str = ""
    document: str = ""
    caption: str = ""
    priority: Priority = Priority.NORMAL
    idempotency_key: str = ""
    not_before: datetime | None = None
    message_id: int = 0


@dataclass
class StoredTask:
    """Task as persisted in the database."""

    id: str
    chat_id: int
    kind: str
    raw_text: str
    template_key: str
    locale: str
    vars_json: str
    parse_mode: str
    disable_web_page_preview: bool
    disable_notification: bool
    reply_markup: str
    reply_to_message_id: int
    photo: str
    document: str
    caption: str
    priority: str
    idempotency_key: str
    not_before: str | None
    message_id: int
    state: str
    attempt: int
    max_retries: int
    next_retry_at: str | None
    leased_until: str | None
    created_at: str
    updated_at: str


# ── Campaign ──────────────────────────────────────────────────────────


@dataclass
class CampaignConfig:
    """Describes a broadcast campaign to create."""

    name: str
    template_key: str
    locale_strategy: LocaleStrategy = LocaleStrategy.PER_USER
    fixed_locale: str = ""
    vars: dict[str, Any] | None = None
    priority: Priority = Priority.NORMAL


@dataclass
class Recipient:
    """A single target within a campaign."""

    chat_id: int
    locale: str = ""
    vars: dict[str, Any] | None = None


@dataclass
class CampaignStats:
    """Campaign delivery progress snapshot."""

    id: str
    name: str
    status: str
    total: int = 0
    sent: int = 0
    failed: int = 0
    pending: int = 0


@dataclass
class CampaignRow:
    """Campaign as stored in the database."""

    id: str
    name: str
    template_key: str
    locale_strategy: str
    fixed_locale: str
    vars_json: str
    priority: str
    status: str
    total: int
    sent: int
    failed: int
    created_at: str
    updated_at: str


@dataclass
class CampaignRecipientRow:
    """Single recipient row from the database."""

    id: int
    campaign_id: str
    chat_id: int
    locale: str
    vars_json: str
    processed: bool
