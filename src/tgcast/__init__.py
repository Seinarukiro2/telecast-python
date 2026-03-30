"""tgcast — lightweight Telegram Bot API broadcast engine for Python.

Usage::

    from tgcast import Telecast, Task

    eng = Telecast(bot_token="123:ABC")
    eng.start()

    task_id = eng.enqueue(Task(chat_id=123456789, text="Hello!"))
    print(eng.task_status(task_id))

    eng.shutdown()
"""

from __future__ import annotations

import logging
from typing import Any

from tgcast._engine import Engine
from tgcast._models import (
    CampaignConfig,
    CampaignStats,
    LocaleStrategy,
    Priority,
    Recipient,
    StoredTask,
    Task,
    TaskKind,
    TaskState,
)
from tgcast._storage import DuplicateKeyError, Store
from tgcast._telegram import TelegramClient
from tgcast._templates import Renderer, TemplateEngine

__all__ = [
    "Telecast",
    "Task",
    "Priority",
    "TaskKind",
    "TaskState",
    "CampaignConfig",
    "CampaignStats",
    "Recipient",
    "LocaleStrategy",
    "DuplicateKeyError",
]

__version__ = "0.1.0"


class Telecast:
    """Telegram broadcast engine.

    Args:
        bot_token: Telegram Bot API token (required).
        store_dsn: SQLite database path. Default: ``"tgcast.db"``.
        templates_path: Path to YAML templates file.
        templates_data: Raw YAML bytes (takes priority over path).
        template_renderer: Custom :class:`Renderer` implementation.
        telegram_base_url: Override Telegram API URL (for testing).
        global_rps: Global messages/sec. Default: ``25``.
        per_chat_rps: Per-chat messages/sec. Default: ``1``.
        max_concurrency: Worker pool size. Default: ``8``.
        lease_ttl: Task lease duration in seconds. Default: ``30``.
        poll_interval: Scheduler poll interval in seconds. Default: ``0.5``.
        max_retries: Max retries before DLQ. Default: ``5``.
        base_backoff: Initial retry delay in seconds. Default: ``1``.
        max_backoff: Max retry delay in seconds. Default: ``300``.
        logger: Custom logger. Default: ``logging.getLogger("tgcast")``.
    """

    def __init__(
        self,
        bot_token: str,
        *,
        store_dsn: str = "telecast.db",
        templates_path: str = "",
        templates_data: bytes = b"",
        template_renderer: Renderer | None = None,
        telegram_base_url: str = "",
        global_rps: float = 25.0,
        per_chat_rps: float = 1.0,
        max_concurrency: int = 8,
        lease_ttl: float = 30.0,
        poll_interval: float = 0.5,
        max_retries: int = 5,
        base_backoff: float = 1.0,
        max_backoff: float = 300.0,
        logger: logging.Logger | None = None,
    ) -> None:
        if not bot_token:
            raise ValueError("bot_token is required")

        self._store: Store | None = None
        self._tg: TelegramClient | None = None
        self._max_retries = max_retries

        try:
            self._store = Store(store_dsn)
            self._tg = TelegramClient(bot_token, telegram_base_url)

            # Templates
            renderer: Renderer | None = template_renderer
            if renderer is None:
                te = TemplateEngine()
                if templates_data:
                    te.load_bytes(templates_data)
                elif templates_path:
                    te.load_file(templates_path)
                renderer = te

            self._engine = Engine(
                store=self._store,
                telegram=self._tg,
                renderer=renderer,
                global_rps=global_rps,
                per_chat_rps=per_chat_rps,
                max_concurrency=max_concurrency,
                lease_ttl=lease_ttl,
                poll_interval=poll_interval,
                max_retries=max_retries,
                base_backoff=base_backoff,
                max_backoff=max_backoff,
                logger=logger,
            )
        except Exception:
            # Clean up resources if init fails partway through
            if self._tg is not None:
                self._tg.close()
            if self._store is not None:
                self._store.close()
            raise

    def __enter__(self) -> Telecast:
        self.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.shutdown()

    # ── Lifecycle ─────────────────────────────────────────────────────

    def start(self) -> None:
        """Start background workers. Non-blocking."""
        self._engine.start()

    def shutdown(self, timeout: float = 10.0) -> None:
        """Stop engine and close resources."""
        self._engine.shutdown(timeout)
        if self._tg:
            self._tg.close()
        if self._store:
            self._store.close()

    # ── Tasks ─────────────────────────────────────────────────────────

    def enqueue(self, task: Task) -> str:
        """Enqueue a task for delivery. Returns the task ID.

        Raises:
            ValueError: If chat_id is 0 or no content provided.
            DuplicateKeyError: If idempotency_key already exists.
        """
        if task.chat_id == 0:
            raise ValueError("chat_id is required")
        has_content = task.text or task.template_key or task.photo or task.document
        if not has_content:
            raise ValueError("text, template_key, photo, or document is required")
        return self._store.task_enqueue(task, self._max_retries)  # type: ignore[union-attr]

    def task_status(self, task_id: str) -> StoredTask | None:
        """Get current task state. Returns None if not found."""
        return self._store.task_get(task_id)  # type: ignore[union-attr]

    # ── Campaigns ─────────────────────────────────────────────────────

    def create_campaign(self, config: CampaignConfig) -> str:
        """Create a campaign. Returns the campaign ID."""
        if not config.name or not config.template_key:
            raise ValueError("name and template_key are required")
        return self._store.campaign_create(config)  # type: ignore[union-attr]

    def add_recipients(self, campaign_id: str, recipients: list[Recipient]) -> int:
        """Add recipients to a campaign. Returns count added."""
        return self._store.campaign_recipients_add(campaign_id, recipients)  # type: ignore[union-attr]

    def start_campaign(self, campaign_id: str) -> None:
        """Start a campaign."""
        self._engine.start_campaign(campaign_id)

    def pause_campaign(self, campaign_id: str) -> None:
        """Pause a campaign."""
        self._engine.pause_campaign(campaign_id)

    def campaign_stats(self, campaign_id: str) -> CampaignStats | None:
        """Get campaign delivery progress."""
        return self._store.campaign_stats(campaign_id)  # type: ignore[union-attr]

    # ── DLQ ───────────────────────────────────────────────────────────

    def dlq_list(self, limit: int = 20, offset: int = 0) -> tuple[list[StoredTask], int]:
        """List dead-letter tasks with pagination."""
        return self._store.dlq_list(limit, offset)  # type: ignore[union-attr]

    def dlq_requeue(self, task_id: str) -> None:
        """Move a dead-letter task back to the queue."""
        self._store.dlq_requeue(task_id)  # type: ignore[union-attr]
