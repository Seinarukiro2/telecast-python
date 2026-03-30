from tgcast import Task, Priority, TaskKind, CampaignConfig, Recipient, LocaleStrategy


def test_task_defaults():
    t = Task(chat_id=1, text="hi")
    assert t.priority == Priority.NORMAL
    assert t.kind == TaskKind.SEND_MESSAGE
    assert t.idempotency_key == ""


def test_recipient_defaults():
    r = Recipient(chat_id=42)
    assert r.locale == ""
    assert r.vars is None


def test_campaign_config_defaults():
    c = CampaignConfig(name="test", template_key="welcome")
    assert c.locale_strategy == LocaleStrategy.PER_USER
    assert c.priority == Priority.NORMAL
