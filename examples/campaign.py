"""Campaign: broadcast a template to multiple recipients."""

import os
import time
from telecast import Telecast, CampaignConfig, Recipient

eng = Telecast(
    bot_token=os.environ["BOT_TOKEN"],
    store_dsn="telecast.db",
    templates_data=b"""
welcome:
  en: "Hello, {name}! Welcome aboard."
  ru: "Привет, {name}! Добро пожаловать."
""",
)
eng.start()

# Create campaign
campaign_id = eng.create_campaign(CampaignConfig(
    name="Welcome wave",
    template_key="welcome",
))
print(f"Campaign: {campaign_id}")

# Add recipients
eng.add_recipients(campaign_id, [
    Recipient(chat_id=100, locale="en", vars={"name": "Alice"}),
    Recipient(chat_id=200, locale="ru", vars={"name": "Боб"}),
    Recipient(chat_id=300, locale="en", vars={"name": "Charlie"}),
])

# Start and poll progress
eng.start_campaign(campaign_id)
for _ in range(30):
    stats = eng.campaign_stats(campaign_id)
    print(f"sent={stats.sent} failed={stats.failed} pending={stats.pending}")
    if stats.pending == 0:
        break
    time.sleep(1)

eng.shutdown()
