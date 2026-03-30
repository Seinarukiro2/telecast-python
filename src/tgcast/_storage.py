"""SQLite storage with WAL mode."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone

from tgcast._models import (
    PRIORITY_WEIGHTS,
    CampaignConfig,
    CampaignRecipientRow,
    CampaignRow,
    CampaignStats,
    CampaignStatus,
    LocaleStrategy,
    Priority,
    Recipient,
    StoredTask,
    Task,
    TaskKind,
    TaskState,
)

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS tasks (
    id                      TEXT PRIMARY KEY,
    chat_id                 INTEGER NOT NULL,
    kind                    TEXT NOT NULL DEFAULT 'send_message',
    raw_text                TEXT NOT NULL DEFAULT '',
    template_key            TEXT NOT NULL DEFAULT '',
    locale                  TEXT NOT NULL DEFAULT '',
    vars_json               TEXT NOT NULL DEFAULT '',
    parse_mode              TEXT NOT NULL DEFAULT '',
    disable_web_page_preview INTEGER NOT NULL DEFAULT 0,
    disable_notification    INTEGER NOT NULL DEFAULT 0,
    reply_markup            TEXT NOT NULL DEFAULT '',
    reply_to_message_id     INTEGER NOT NULL DEFAULT 0,
    photo                   TEXT NOT NULL DEFAULT '',
    document                TEXT NOT NULL DEFAULT '',
    caption                 TEXT NOT NULL DEFAULT '',
    priority                TEXT NOT NULL DEFAULT 'normal',
    idempotency_key         TEXT NOT NULL DEFAULT '',
    not_before              TEXT,
    message_id              INTEGER NOT NULL DEFAULT 0,
    state                   TEXT NOT NULL DEFAULT 'queued',
    attempt                 INTEGER NOT NULL DEFAULT 0,
    max_retries             INTEGER NOT NULL DEFAULT 5,
    next_retry_at           TEXT,
    leased_until            TEXT,
    campaign_id             TEXT NOT NULL DEFAULT '',
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency
    ON tasks(idempotency_key) WHERE idempotency_key != '';

CREATE INDEX IF NOT EXISTS idx_tasks_state_priority
    ON tasks(state, priority);

CREATE INDEX IF NOT EXISTS idx_tasks_campaign
    ON tasks(campaign_id) WHERE campaign_id != '';

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key        TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS campaigns (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    template_key    TEXT NOT NULL,
    locale_strategy TEXT NOT NULL DEFAULT 'per_user',
    fixed_locale    TEXT NOT NULL DEFAULT '',
    vars_json       TEXT NOT NULL DEFAULT '',
    priority        TEXT NOT NULL DEFAULT 'normal',
    status          TEXT NOT NULL DEFAULT 'created',
    total           INTEGER NOT NULL DEFAULT 0,
    sent            INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS campaign_recipients (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    chat_id     INTEGER NOT NULL,
    locale      TEXT NOT NULL DEFAULT '',
    vars_json   TEXT NOT NULL DEFAULT '',
    processed   INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_cr_campaign_processed
    ON campaign_recipients(campaign_id, processed);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id() -> str:
    return uuid.uuid4().hex


def _row_to_task(row: sqlite3.Row) -> StoredTask:
    return StoredTask(
        id=row["id"],
        chat_id=row["chat_id"],
        kind=row["kind"],
        raw_text=row["raw_text"],
        template_key=row["template_key"],
        locale=row["locale"],
        vars_json=row["vars_json"],
        parse_mode=row["parse_mode"],
        disable_web_page_preview=bool(row["disable_web_page_preview"]),
        disable_notification=bool(row["disable_notification"]),
        reply_markup=row["reply_markup"],
        reply_to_message_id=row["reply_to_message_id"],
        photo=row["photo"],
        document=row["document"],
        caption=row["caption"],
        priority=row["priority"],
        idempotency_key=row["idempotency_key"],
        not_before=row["not_before"],
        message_id=row["message_id"],
        state=row["state"],
        attempt=row["attempt"],
        max_retries=row["max_retries"],
        next_retry_at=row["next_retry_at"],
        leased_until=row["leased_until"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class Store:
    """Thread-safe SQLite store. Single writer, WAL mode."""

    def __init__(self, dsn: str = "telecast.db") -> None:
        self._dsn = dsn
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(dsn, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ── Tasks ─────────────────────────────────────────────────────────

    def task_enqueue(
        self, task: Task, max_retries: int = 5, campaign_id: str = ""
    ) -> str:
        tid = _gen_id()
        now = _now()
        vars_json = json.dumps(task.vars) if task.vars else ""
        reply_markup = json.dumps(task.reply_markup) if task.reply_markup else ""
        kind = task.kind or TaskKind.SEND_MESSAGE
        priority = task.priority or Priority.NORMAL
        not_before = task.not_before.isoformat() if task.not_before else None

        with self._lock:
            # Idempotency check
            if task.idempotency_key:
                row = self._conn.execute(
                    "SELECT 1 FROM idempotency_keys WHERE key = ?",
                    (task.idempotency_key,),
                ).fetchone()
                if row:
                    raise DuplicateKeyError(task.idempotency_key)
                self._conn.execute(
                    "INSERT INTO idempotency_keys (key, created_at) VALUES (?, ?)",
                    (task.idempotency_key, now),
                )

            self._conn.execute(
                """INSERT INTO tasks
                   (id, chat_id, kind, raw_text, template_key, locale, vars_json,
                    parse_mode, disable_web_page_preview, disable_notification,
                    reply_markup, reply_to_message_id, photo, document, caption,
                    priority, idempotency_key, not_before, message_id,
                    state, attempt, max_retries, campaign_id, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?)""",
                (
                    tid, task.chat_id, kind, task.text, task.template_key,
                    task.locale, vars_json, task.parse_mode,
                    int(task.disable_web_page_preview), int(task.disable_notification),
                    reply_markup, task.reply_to_message_id,
                    task.photo, task.document, task.caption,
                    priority, task.idempotency_key, not_before, task.message_id,
                    TaskState.QUEUED, max_retries, campaign_id, now, now,
                ),
            )
            self._conn.commit()
        return tid

    def task_lease_wrr(self, count: int, lease_seconds: float) -> list[StoredTask]:
        """Lease up to `count` tasks with weighted round-robin (5:3:1).

        Distributes slots proportionally: for 9 slots → 5 high, 3 normal, 1 low.
        If a priority has fewer tasks than its share, extras go to others.
        """
        now = _now()
        lease_until = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        ).isoformat()

        total_weight = sum(PRIORITY_WEIGHTS.values())  # 9
        targets: dict[str, int] = {}
        for p, w in PRIORITY_WEIGHTS.items():
            targets[p.value] = max(1, round(count * w / total_weight))

        with self._lock:
            tasks: list[StoredTask] = []
            remaining = count

            # Pass 1: allocate proportional shares
            for priority_val in ["high", "normal", "low"]:
                if remaining <= 0:
                    break
                limit = min(targets.get(priority_val, 1), remaining)
                rows = self._conn.execute(
                    """SELECT * FROM tasks
                       WHERE state IN ('queued', 'failed')
                         AND priority = ?
                         AND (not_before IS NULL OR not_before <= ?)
                         AND (next_retry_at IS NULL OR next_retry_at <= ?)
                       ORDER BY created_at ASC
                       LIMIT ?""",
                    (priority_val, now, now, limit),
                ).fetchall()

                for row in rows:
                    self._conn.execute(
                        "UPDATE tasks SET state = ?, leased_until = ?, updated_at = ? WHERE id = ?",
                        (TaskState.LEASED, lease_until, now, row["id"]),
                    )
                    tasks.append(_row_to_task(row))
                    remaining -= 1

            # Pass 2: fill remaining slots from any priority (starvation prevention)
            if remaining > 0:
                already = {t.id for t in tasks}
                rows = self._conn.execute(
                    """SELECT * FROM tasks
                       WHERE state IN ('queued', 'failed')
                         AND (not_before IS NULL OR not_before <= ?)
                         AND (next_retry_at IS NULL OR next_retry_at <= ?)
                       ORDER BY
                         CASE priority
                           WHEN 'high'   THEN 0
                           WHEN 'normal' THEN 1
                           WHEN 'low'    THEN 2
                         END,
                         created_at ASC
                       LIMIT ?""",
                    (now, now, remaining + len(already)),
                ).fetchall()

                for row in rows:
                    if remaining <= 0:
                        break
                    if row["id"] not in already:
                        self._conn.execute(
                            "UPDATE tasks SET state = ?, leased_until = ?, updated_at = ? WHERE id = ?",
                            (TaskState.LEASED, lease_until, now, row["id"]),
                        )
                        tasks.append(_row_to_task(row))
                        remaining -= 1

            if tasks:
                self._conn.commit()
        return tasks

    def task_recover_stale_leases(self) -> int:
        """Return expired leased tasks to queued state. Returns count recovered."""
        now = _now()
        with self._lock:
            cursor = self._conn.execute(
                """UPDATE tasks
                   SET state = ?, updated_at = ?
                   WHERE state = ? AND leased_until IS NOT NULL AND leased_until < ?""",
                (TaskState.QUEUED, now, TaskState.LEASED, now),
            )
            count = cursor.rowcount
            if count:
                self._conn.commit()
        return count

    def task_ack(self, task_id: str) -> None:
        """Mark task as sent."""
        now = _now()
        with self._lock:
            self._conn.execute(
                "UPDATE tasks SET state = ?, leased_until = NULL, updated_at = ? WHERE id = ? AND state = ?",
                (TaskState.SENT, now, task_id, TaskState.LEASED),
            )
            self._conn.commit()

    def task_nack(self, task_id: str, next_retry_at: str) -> None:
        """Mark task as failed, schedule retry."""
        now = _now()
        with self._lock:
            self._conn.execute(
                """UPDATE tasks
                   SET state = ?, attempt = attempt + 1,
                       next_retry_at = ?, leased_until = NULL, updated_at = ?
                   WHERE id = ? AND state = ?""",
                (TaskState.FAILED, next_retry_at, now, task_id, TaskState.LEASED),
            )
            self._conn.commit()

    def task_mark_dead(self, task_id: str) -> None:
        """Move task to DLQ."""
        now = _now()
        with self._lock:
            self._conn.execute(
                "UPDATE tasks SET state = ?, leased_until = NULL, updated_at = ? WHERE id = ?",
                (TaskState.DEAD, now, task_id),
            )
            self._conn.commit()

    def task_get(self, task_id: str) -> StoredTask | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return _row_to_task(row) if row else None

    # ── Campaign task tracking ────────────────────────────────────────

    def campaign_task_stats(self, campaign_id: str) -> tuple[int, int]:
        """Count sent and dead tasks for a campaign. Returns (sent, failed)."""
        with self._lock:
            row = self._conn.execute(
                """SELECT
                     COALESCE(SUM(CASE WHEN state = 'sent' THEN 1 ELSE 0 END), 0) AS sent,
                     COALESCE(SUM(CASE WHEN state = 'dead' THEN 1 ELSE 0 END), 0) AS failed
                   FROM tasks WHERE campaign_id = ?""",
                (campaign_id,),
            ).fetchone()
        return row["sent"], row["failed"]

    # ── DLQ ───────────────────────────────────────────────────────────

    def dlq_list(self, limit: int = 20, offset: int = 0) -> tuple[list[StoredTask], int]:
        with self._lock:
            total = self._conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE state = ?", (TaskState.DEAD,)
            ).fetchone()[0]
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE state = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (TaskState.DEAD, limit, offset),
            ).fetchall()
        return [_row_to_task(r) for r in rows], total

    def dlq_requeue(self, task_id: str) -> None:
        now = _now()
        with self._lock:
            self._conn.execute(
                """UPDATE tasks
                   SET state = ?, attempt = 0, next_retry_at = NULL,
                       leased_until = NULL, updated_at = ?
                   WHERE id = ? AND state = ?""",
                (TaskState.QUEUED, now, task_id, TaskState.DEAD),
            )
            self._conn.commit()

    # ── Gauges ────────────────────────────────────────────────────────

    def queue_depth(self) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE state IN ('queued', 'leased', 'failed')"
            ).fetchone()[0]

    def dlq_depth(self) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE state = 'dead'"
            ).fetchone()[0]

    # ── Campaigns ─────────────────────────────────────────────────────

    def campaign_create(self, cfg: CampaignConfig) -> str:
        cid = _gen_id()
        now = _now()
        vars_json = json.dumps(cfg.vars) if cfg.vars else ""
        ls = cfg.locale_strategy or LocaleStrategy.PER_USER
        priority = cfg.priority or Priority.NORMAL

        with self._lock:
            self._conn.execute(
                """INSERT INTO campaigns
                   (id, name, template_key, locale_strategy, fixed_locale,
                    vars_json, priority, status, total, sent, failed, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,0,0,0,?,?)""",
                (cid, cfg.name, cfg.template_key, ls, cfg.fixed_locale,
                 vars_json, priority, CampaignStatus.CREATED, now, now),
            )
            self._conn.commit()
        return cid

    def campaign_get(self, campaign_id: str) -> CampaignRow | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM campaigns WHERE id = ?", (campaign_id,)
            ).fetchone()
        if not row:
            return None
        return CampaignRow(
            id=row["id"], name=row["name"], template_key=row["template_key"],
            locale_strategy=row["locale_strategy"], fixed_locale=row["fixed_locale"],
            vars_json=row["vars_json"], priority=row["priority"],
            status=row["status"], total=row["total"], sent=row["sent"],
            failed=row["failed"], created_at=row["created_at"], updated_at=row["updated_at"],
        )

    def campaign_update_status(self, campaign_id: str, status: CampaignStatus) -> None:
        now = _now()
        with self._lock:
            self._conn.execute(
                "UPDATE campaigns SET status = ?, updated_at = ? WHERE id = ?",
                (status, now, campaign_id),
            )
            self._conn.commit()

    def campaign_list_running(self) -> list[CampaignRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM campaigns WHERE status = ?", (CampaignStatus.RUNNING,)
            ).fetchall()
        return [
            CampaignRow(
                id=r["id"], name=r["name"], template_key=r["template_key"],
                locale_strategy=r["locale_strategy"], fixed_locale=r["fixed_locale"],
                vars_json=r["vars_json"], priority=r["priority"], status=r["status"],
                total=r["total"], sent=r["sent"], failed=r["failed"],
                created_at=r["created_at"], updated_at=r["updated_at"],
            )
            for r in rows
        ]

    def campaign_recipients_add(self, campaign_id: str, recipients: list[Recipient]) -> int:
        now = _now()
        with self._lock:
            self._conn.executemany(
                """INSERT INTO campaign_recipients
                   (campaign_id, chat_id, locale, vars_json, processed)
                   VALUES (?, ?, ?, ?, 0)""",
                [
                    (campaign_id, r.chat_id, r.locale, json.dumps(r.vars) if r.vars else "")
                    for r in recipients
                ],
            )
            self._conn.execute(
                "UPDATE campaigns SET total = total + ?, updated_at = ? WHERE id = ?",
                (len(recipients), now, campaign_id),
            )
            self._conn.commit()
        return len(recipients)

    def campaign_recipients_next_batch(
        self, campaign_id: str, batch_size: int = 100
    ) -> list[CampaignRecipientRow]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM campaign_recipients
                   WHERE campaign_id = ? AND processed = 0
                   LIMIT ?""",
                (campaign_id, batch_size),
            ).fetchall()
        return [
            CampaignRecipientRow(
                id=r["id"], campaign_id=r["campaign_id"], chat_id=r["chat_id"],
                locale=r["locale"], vars_json=r["vars_json"], processed=bool(r["processed"]),
            )
            for r in rows
        ]

    def campaign_recipients_mark_processed(self, ids: list[int]) -> None:
        if not ids:
            return
        with self._lock:
            placeholders = ",".join("?" for _ in ids)
            self._conn.execute(
                f"UPDATE campaign_recipients SET processed = 1 WHERE id IN ({placeholders})",
                ids,
            )
            self._conn.commit()

    def campaign_progress_update(
        self, campaign_id: str, *, sent_delta: int = 0, failed_delta: int = 0
    ) -> None:
        now = _now()
        with self._lock:
            self._conn.execute(
                """UPDATE campaigns
                   SET sent = sent + ?, failed = failed + ?, updated_at = ?
                   WHERE id = ?""",
                (sent_delta, failed_delta, now, campaign_id),
            )
            self._conn.commit()

    def campaign_sync_stats(self, campaign_id: str) -> None:
        """Sync campaign sent/failed counters from actual task states."""
        sent, failed = self.campaign_task_stats(campaign_id)
        now = _now()
        with self._lock:
            self._conn.execute(
                "UPDATE campaigns SET sent = ?, failed = ?, updated_at = ? WHERE id = ?",
                (sent, failed, now, campaign_id),
            )
            self._conn.commit()

    def campaign_stats(self, campaign_id: str) -> CampaignStats | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM campaigns WHERE id = ?", (campaign_id,)
            ).fetchone()
        if not row:
            return None
        total = row["total"]
        sent = row["sent"]
        failed = row["failed"]
        return CampaignStats(
            id=row["id"], name=row["name"], status=row["status"],
            total=total, sent=sent, failed=failed,
            pending=total - sent - failed,
        )


class DuplicateKeyError(Exception):
    """Raised when an idempotency key already exists."""

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"duplicate idempotency key: {key}")
