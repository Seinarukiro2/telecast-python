"""Using Telecast as a context manager."""

import os
import time
from telecast import Telecast, Task

with Telecast(bot_token=os.environ["BOT_TOKEN"], store_dsn=":memory:") as eng:
    task_id = eng.enqueue(Task(chat_id=123456789, text="Hello!"))
    time.sleep(2)
    status = eng.task_status(task_id)
    print(f"{task_id}: {status.state}")
