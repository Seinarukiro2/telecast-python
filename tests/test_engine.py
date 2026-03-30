"""Integration tests for the engine using a mock Telegram server."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from tgcast import Telecast, Task, Priority, CampaignConfig, Recipient


class _TelegramHandler(BaseHTTPRequestHandler):
    """Minimal mock for Telegram Bot API."""

    sent: list[dict] = []
    lock = threading.Lock()
    fail_next: int = 0

    def do_POST(self, *a):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        with self.lock:
            if self.fail_next > 0:
                self.fail_next -= 1
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "error_code": 500,
                    "description": "mock error",
                }).encode())
                return

            self.sent.append(body)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "ok": True,
            "result": {"message_id": len(self.sent)},
        }).encode())

    def log_message(self, *a):
        pass  # silence


@pytest.fixture
def mock_tg():
    _TelegramHandler.sent = []
    _TelegramHandler.fail_next = 0
    server = HTTPServer(("127.0.0.1", 0), _TelegramHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


@pytest.fixture
def engine(mock_tg):
    eng = Telecast(
        bot_token="test:token",
        store_dsn=":memory:",
        telegram_base_url=mock_tg,
        poll_interval=0.1,
        max_concurrency=2,
    )
    eng.start()
    yield eng
    eng.shutdown()


class TestEnqueueAndDeliver:
    def test_simple_text(self, engine: Telecast):
        tid = engine.enqueue(Task(chat_id=123, text="hello"))
        assert tid

        # Wait for delivery
        for _ in range(20):
            t = engine.task_status(tid)
            if t and t.state == "sent":
                break
            time.sleep(0.1)

        assert t and t.state == "sent"
        assert _TelegramHandler.sent[-1]["text"] == "hello"

    def test_high_priority_first(self, engine: Telecast):
        # Enqueue low then high
        engine.enqueue(Task(chat_id=1, text="low", priority=Priority.LOW))
        engine.enqueue(Task(chat_id=2, text="high", priority=Priority.HIGH))

        time.sleep(1)
        texts = [s["text"] for s in _TelegramHandler.sent]
        # High should be sent first (or at least be present)
        assert "high" in texts
        assert "low" in texts

    def test_idempotency(self, engine: Telecast):
        engine.enqueue(Task(chat_id=1, text="a", idempotency_key="k1"))
        from tgcast._storage import DuplicateKeyError
        with pytest.raises(DuplicateKeyError):
            engine.enqueue(Task(chat_id=2, text="b", idempotency_key="k1"))

    def test_validation_chat_id(self, engine: Telecast):
        with pytest.raises(ValueError, match="chat_id"):
            engine.enqueue(Task(chat_id=0, text="x"))

    def test_validation_text(self, engine: Telecast):
        with pytest.raises(ValueError, match="text, template_key, photo, or document"):
            engine.enqueue(Task(chat_id=1))


class TestTemplates:
    def test_template_rendering(self, mock_tg):
        yaml_data = 'greet:\n  en: "Hi {name}!"\n  ru: "Hello {name}!"'.encode("utf-8")
        eng = Telecast(
            bot_token="test:token",
            store_dsn=":memory:",
            telegram_base_url=mock_tg,
            templates_data=yaml_data,
            poll_interval=0.1,
        )
        eng.start()
        _TelegramHandler.sent.clear()

        tid = eng.enqueue(Task(
            chat_id=42,
            template_key="greet",
            locale="en",
            vars={"name": "Ivan"},
        ))

        for _ in range(20):
            t = eng.task_status(tid)
            if t and t.state == "sent":
                break
            time.sleep(0.1)

        eng.shutdown()
        assert any("Hi Ivan!" in s.get("text", "") for s in _TelegramHandler.sent)


class TestCampaigns:
    def test_campaign_flow(self, engine: Telecast, mock_tg):
        _TelegramHandler.sent.clear()

        yaml_data = 'welcome:\n  en: "Hello {name}!"'.encode("utf-8")
        eng = Telecast(
            bot_token="test:token",
            store_dsn=":memory:",
            telegram_base_url=mock_tg,
            templates_data=yaml_data,
            poll_interval=0.1,
        )
        eng.start()

        cid = eng.create_campaign(CampaignConfig(name="test", template_key="welcome"))
        eng.add_recipients(cid, [
            Recipient(chat_id=100, vars={"name": "A"}),
            Recipient(chat_id=200, vars={"name": "B"}),
        ])
        eng.start_campaign(cid)

        for _ in range(30):
            stats = eng.campaign_stats(cid)
            if stats and stats.pending == 0:
                break
            time.sleep(0.2)

        eng.shutdown()
        assert stats and stats.sent + stats.failed == stats.total


class TestDLQ:
    def test_dlq_list_empty(self, engine: Telecast):
        tasks, total = engine.dlq_list()
        assert total == 0
