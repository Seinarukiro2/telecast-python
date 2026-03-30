"""Core engine: scheduler, workers, campaign loop."""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

from tgcast._models import (
    CampaignStatus,
    LocaleStrategy,
    Priority,
    StoredTask,
    Task,
    TaskKind,
)
from tgcast._ratelimit import RateLimiter
from tgcast._storage import Store
from tgcast._telegram import APIError, TelegramClient
from tgcast._templates import Renderer


def _backoff(attempt: int, base: float, maximum: float) -> float:
    """Exponential backoff with +/-25% jitter."""
    delay = min(base * (2 ** attempt), maximum)
    jitter = delay * 0.25 * (2 * random.random() - 1)
    return max(0, delay + jitter)


class Engine:
    """Task processing engine with scheduler, workers, and campaign loop."""

    def __init__(
        self,
        store: Store,
        telegram: TelegramClient,
        renderer: Renderer | None,
        *,
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
        self._store = store
        self._tg = telegram
        self._renderer = renderer
        self._limiter = RateLimiter(global_rps, per_chat_rps)
        self._max_concurrency = max_concurrency
        self._lease_ttl = lease_ttl
        self._poll_interval = poll_interval
        self._max_retries = max_retries
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff
        self._log = logger or logging.getLogger("tgcast")

        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._pool: ThreadPoolExecutor | None = None

        # Campaign single-flight
        self._campaign_locks: dict[str, threading.Lock] = {}
        self._campaign_locks_mu = threading.Lock()

    def start(self) -> None:
        """Start background threads. Non-blocking."""
        self._stop.clear()
        self._pool = ThreadPoolExecutor(max_workers=self._max_concurrency)

        threads = [
            threading.Thread(target=self._scheduler_loop, name="tgcast-scheduler", daemon=True),
            threading.Thread(target=self._campaign_loop, name="tgcast-campaigns", daemon=True),
            threading.Thread(target=self._lease_recovery_loop, name="tgcast-lease-recovery", daemon=True),
            threading.Thread(target=self._campaign_stats_loop, name="tgcast-campaign-stats", daemon=True),
        ]
        for t in threads:
            t.start()
        self._threads = threads
        self._log.info("tgcast engine started (workers=%d)", self._max_concurrency)

    def shutdown(self, timeout: float = 10.0) -> None:
        """Gracefully stop the engine."""
        self._stop.set()
        for t in self._threads:
            t.join(timeout=timeout)
        if self._pool:
            self._pool.shutdown(wait=True, cancel_futures=False)
        self._log.info("tgcast engine stopped")

    @property
    def running(self) -> bool:
        return not self._stop.is_set()

    # ── Scheduler ─────────────────────────────────────────────────────

    def _scheduler_loop(self) -> None:
        while not self._stop.is_set():
            try:
                tasks = self._store.task_lease_wrr(self._max_concurrency, self._lease_ttl)
                for task in tasks:
                    if self._pool:
                        self._pool.submit(self._process_task, task)
            except Exception:
                self._log.exception("scheduler error")

            self._stop.wait(self._poll_interval)

    def _process_task(self, task: StoredTask) -> None:
        try:
            # Render text / caption
            text = task.raw_text
            if not text and task.template_key and self._renderer:
                vars_dict: dict[str, Any] = {}
                if task.vars_json:
                    vars_dict = json.loads(task.vars_json)
                text = self._renderer.render(task.template_key, task.locale, vars_dict)

            # For photo/document, text is optional (caption)
            is_media = task.kind in (TaskKind.SEND_PHOTO, TaskKind.SEND_DOCUMENT)
            if not text and not is_media:
                self._log.error("task %s: no text to send", task.id)
                self._store.task_mark_dead(task.id)
                return

            # Rate limit
            wait = self._limiter.acquire(task.chat_id)
            if wait > 0:
                time.sleep(wait)

            # Send
            caption = task.caption or text or ""

            if task.kind == TaskKind.SEND_PHOTO:
                self._tg.send_photo(
                    chat_id=task.chat_id,
                    photo=task.photo,
                    caption=caption,
                    parse_mode=task.parse_mode,
                    disable_notification=task.disable_notification,
                    reply_markup=task.reply_markup,
                    reply_to_message_id=task.reply_to_message_id,
                )
            elif task.kind == TaskKind.SEND_DOCUMENT:
                self._tg.send_document(
                    chat_id=task.chat_id,
                    document=task.document,
                    caption=caption,
                    parse_mode=task.parse_mode,
                    disable_notification=task.disable_notification,
                    reply_markup=task.reply_markup,
                    reply_to_message_id=task.reply_to_message_id,
                )
            elif task.kind == TaskKind.EDIT_MESSAGE:
                self._tg.edit_message(
                    chat_id=task.chat_id,
                    message_id=task.message_id,
                    text=text,
                    parse_mode=task.parse_mode,
                    disable_web_page_preview=task.disable_web_page_preview,
                    reply_markup=task.reply_markup,
                )
            else:
                self._tg.send_message(
                    chat_id=task.chat_id,
                    text=text,
                    parse_mode=task.parse_mode,
                    disable_web_page_preview=task.disable_web_page_preview,
                    disable_notification=task.disable_notification,
                    reply_markup=task.reply_markup,
                    reply_to_message_id=task.reply_to_message_id,
                )

            self._store.task_ack(task.id)
            self._log.debug("task %s sent to %d", task.id, task.chat_id)

        except APIError as e:
            self._handle_api_error(task, e)
        except Exception:
            self._log.exception("task %s: unexpected error", task.id)
            self._handle_transient_error(task)

    def _handle_api_error(self, task: StoredTask, err: APIError) -> None:
        if err.is_rate_limited and err.retry_after:
            self._limiter.throttle(err.retry_after)
            retry_at = (
                datetime.now(timezone.utc) + timedelta(seconds=err.retry_after)
            ).isoformat()
            self._store.task_nack(task.id, retry_at)
            self._log.warning("429 throttled for %ds", err.retry_after)
        elif err.is_permanent:
            self._store.task_mark_dead(task.id)
            self._log.warning("task %s permanent error: %s", task.id, err)
        else:
            self._handle_transient_error(task)

    def _handle_transient_error(self, task: StoredTask) -> None:
        attempt = task.attempt + 1
        if attempt >= task.max_retries:
            self._store.task_mark_dead(task.id)
            self._log.warning("task %s exhausted retries, moved to DLQ", task.id)
        else:
            delay = _backoff(attempt, self._base_backoff, self._max_backoff)
            retry_at = (
                datetime.now(timezone.utc) + timedelta(seconds=delay)
            ).isoformat()
            self._store.task_nack(task.id, retry_at)
            self._log.debug("task %s retry #%d in %.1fs", task.id, attempt, delay)

    # ── Stale lease recovery ──────────────────────────────────────────

    def _lease_recovery_loop(self) -> None:
        """Periodically recover tasks with expired leases."""
        while not self._stop.is_set():
            try:
                recovered = self._store.task_recover_stale_leases()
                if recovered:
                    self._log.info("recovered %d stale leases", recovered)
            except Exception:
                self._log.exception("lease recovery error")
            self._stop.wait(self._lease_ttl / 2)

    # ── Campaign stats sync ───────────────────────────────────────────

    def _campaign_stats_loop(self) -> None:
        """Periodically sync campaign sent/failed from actual task states."""
        while not self._stop.is_set():
            try:
                for c in self._store.campaign_list_running():
                    self._store.campaign_sync_stats(c.id)
            except Exception:
                self._log.exception("campaign stats sync error")
            self._stop.wait(5.0)

    # ── Campaigns ─────────────────────────────────────────────────────

    def _campaign_loop(self) -> None:
        while not self._stop.is_set():
            try:
                campaigns = self._store.campaign_list_running()
                for c in campaigns:
                    self._process_campaign_batch(c.id)
            except Exception:
                self._log.exception("campaign loop error")
            self._stop.wait(2.0)

    def _process_campaign_batch(self, campaign_id: str) -> None:
        # Single-flight per campaign
        with self._campaign_locks_mu:
            if campaign_id not in self._campaign_locks:
                self._campaign_locks[campaign_id] = threading.Lock()
            lock = self._campaign_locks[campaign_id]

        if not lock.acquire(blocking=False):
            return
        try:
            self._do_campaign_batch(campaign_id)
        finally:
            lock.release()

    def _do_campaign_batch(self, campaign_id: str) -> None:
        campaign = self._store.campaign_get(campaign_id)
        if not campaign or campaign.status != CampaignStatus.RUNNING:
            return

        batch = self._store.campaign_recipients_next_batch(campaign_id, 100)
        if not batch:
            # Sync final stats before marking complete
            self._store.campaign_sync_stats(campaign_id)
            self._store.campaign_update_status(campaign_id, CampaignStatus.COMPLETED)
            self._log.info("campaign %s completed", campaign_id)
            return

        processed_ids: list[int] = []
        enqueued = 0
        for r in batch:
            # Determine locale
            locale = r.locale
            if campaign.locale_strategy == LocaleStrategy.FIXED:
                locale = campaign.fixed_locale

            # Merge vars
            vars_dict: dict[str, Any] = {}
            if campaign.vars_json:
                vars_dict = json.loads(campaign.vars_json)
            if r.vars_json:
                vars_dict.update(json.loads(r.vars_json))

            idem_key = f"campaign:{campaign_id}:{r.chat_id}"
            task = Task(
                chat_id=r.chat_id,
                template_key=campaign.template_key,
                locale=locale,
                vars=vars_dict or None,
                priority=Priority(campaign.priority),
                idempotency_key=idem_key,
            )

            try:
                self._store.task_enqueue(
                    task, max_retries=self._max_retries, campaign_id=campaign_id
                )
                enqueued += 1
            except Exception:
                enqueued += 1  # duplicate = already enqueued, still counts

            processed_ids.append(r.id)

        self._store.campaign_recipients_mark_processed(processed_ids)
        if enqueued:
            self._store.campaign_progress_update(campaign_id, sent_delta=enqueued)

    def start_campaign(self, campaign_id: str) -> None:
        self._store.campaign_update_status(campaign_id, CampaignStatus.RUNNING)
        self._log.info("campaign %s started", campaign_id)

    def pause_campaign(self, campaign_id: str) -> None:
        self._store.campaign_update_status(campaign_id, CampaignStatus.PAUSED)
        self._log.info("campaign %s paused", campaign_id)
