"""Custom template renderer — no YAML needed."""

import os
import signal
from telecast import Telecast, Task

class MarkdownRenderer:
    def render(self, key: str, locale: str, vars: dict) -> str:
        if key == "order_confirmation":
            return f"*Order #{vars['order_id']}*\nStatus: {vars['status']}"
        return f"Unknown template: {key}"

eng = Telecast(
    bot_token=os.environ["BOT_TOKEN"],
    store_dsn="telecast.db",
    template_renderer=MarkdownRenderer(),
)
eng.start()

eng.enqueue(Task(
    chat_id=123456789,
    template_key="order_confirmation",
    vars={"order_id": "42", "status": "shipped"},
    parse_mode="MarkdownV2",
))

signal.pause()
eng.shutdown()
