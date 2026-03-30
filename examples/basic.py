"""Basic usage: enqueue a message and check its status."""

import os
import signal
from telecast import Telecast, Task, Priority

eng = Telecast(
    bot_token=os.environ["BOT_TOKEN"],
    store_dsn="telecast.db",
)
eng.start()

# Enqueue messages
task_id = eng.enqueue(Task(
    chat_id=123456789,
    text="Hello from Telecast!",
    priority=Priority.HIGH,
))
print(f"Enqueued: {task_id}")

# Check status
status = eng.task_status(task_id)
print(f"State: {status.state}")

# Wait for Ctrl+C
signal.pause()
eng.shutdown()
