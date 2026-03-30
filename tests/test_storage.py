import time
import pytest
from telecast import Task, Priority, CampaignConfig, Recipient
from telecast._storage import Store, DuplicateKeyError


@pytest.fixture
def store():
    s = Store(":memory:")
    yield s
    s.close()


class TestTaskEnqueue:
    def test_enqueue_returns_id(self, store: Store):
        tid = store.task_enqueue(Task(chat_id=1, text="hello"))
        assert isinstance(tid, str) and len(tid) > 0

    def test_enqueue_idempotency(self, store: Store):
        store.task_enqueue(Task(chat_id=1, text="a", idempotency_key="k1"))
        with pytest.raises(DuplicateKeyError):
            store.task_enqueue(Task(chat_id=2, text="b", idempotency_key="k1"))

    def test_enqueue_no_key_no_dedup(self, store: Store):
        id1 = store.task_enqueue(Task(chat_id=1, text="a"))
        id2 = store.task_enqueue(Task(chat_id=1, text="a"))
        assert id1 != id2


class TestTaskLeaseWRR:
    def test_lease_returns_tasks(self, store: Store):
        store.task_enqueue(Task(chat_id=1, text="hi"))
        tasks = store.task_lease_wrr(10, 30)
        assert len(tasks) == 1

    def test_lease_respects_limit(self, store: Store):
        for i in range(5):
            store.task_enqueue(Task(chat_id=i, text="hi"))
        tasks = store.task_lease_wrr(2, 30)
        assert len(tasks) == 2

    def test_leased_not_re_leased(self, store: Store):
        store.task_enqueue(Task(chat_id=1, text="hi"))
        store.task_lease_wrr(10, 30)
        tasks2 = store.task_lease_wrr(10, 30)
        assert len(tasks2) == 0

    def test_weighted_distribution(self, store: Store):
        """With 9 tasks (3 per priority) and 9 slots, WRR should pick 5H:3N:1L."""
        for i in range(3):
            store.task_enqueue(Task(chat_id=i + 10, text="h", priority=Priority.HIGH))
            store.task_enqueue(Task(chat_id=i + 20, text="n", priority=Priority.NORMAL))
            store.task_enqueue(Task(chat_id=i + 30, text="l", priority=Priority.LOW))

        tasks = store.task_lease_wrr(9, 30)
        assert len(tasks) == 9
        priorities = [t.priority for t in tasks]
        # High gets at least 3 (all available), normal gets 3, low fills the rest
        assert priorities.count("high") >= 3
        assert priorities.count("normal") >= 2

    def test_low_priority_not_starved(self, store: Store):
        """If high queue is empty, low tasks still get picked up."""
        for i in range(5):
            store.task_enqueue(Task(chat_id=i, text="lo", priority=Priority.LOW))
        tasks = store.task_lease_wrr(5, 30)
        assert len(tasks) == 5
        assert all(t.priority == "low" for t in tasks)


class TestStaleLeaseRecovery:
    def test_recover_expired_leases(self, store: Store):
        tid = store.task_enqueue(Task(chat_id=1, text="hi"))
        # Lease with 0-second TTL (immediately stale)
        store.task_lease_wrr(1, 0)

        t = store.task_get(tid)
        assert t and t.state == "leased"

        # Small sleep so leased_until is definitely in the past
        time.sleep(0.05)

        recovered = store.task_recover_stale_leases()
        assert recovered == 1

        t = store.task_get(tid)
        assert t and t.state == "queued"

    def test_no_false_recovery(self, store: Store):
        store.task_enqueue(Task(chat_id=1, text="hi"))
        store.task_lease_wrr(1, 60)  # 60-second lease, not stale

        recovered = store.task_recover_stale_leases()
        assert recovered == 0


class TestTaskAckNack:
    def test_ack(self, store: Store):
        tid = store.task_enqueue(Task(chat_id=1, text="hi"))
        store.task_lease_wrr(10, 30)
        store.task_ack(tid)
        t = store.task_get(tid)
        assert t and t.state == "sent"
        assert t.leased_until is None

    def test_nack_and_retry(self, store: Store):
        tid = store.task_enqueue(Task(chat_id=1, text="hi"))
        store.task_lease_wrr(10, 30)
        store.task_nack(tid, "2099-01-01T00:00:00")
        t = store.task_get(tid)
        assert t and t.state == "failed"
        assert t.attempt == 1
        assert t.leased_until is None

    def test_mark_dead(self, store: Store):
        tid = store.task_enqueue(Task(chat_id=1, text="hi"))
        store.task_lease_wrr(10, 30)
        store.task_mark_dead(tid)
        t = store.task_get(tid)
        assert t and t.state == "dead"
        assert t.leased_until is None


class TestDLQ:
    def test_dlq_list_empty(self, store: Store):
        tasks, total = store.dlq_list()
        assert tasks == []
        assert total == 0

    def test_dlq_requeue(self, store: Store):
        tid = store.task_enqueue(Task(chat_id=1, text="hi"))
        store.task_lease_wrr(10, 30)
        store.task_mark_dead(tid)

        tasks, total = store.dlq_list()
        assert total == 1

        store.dlq_requeue(tid)
        t = store.task_get(tid)
        assert t and t.state == "queued"
        assert t.attempt == 0


class TestCampaignTaskTracking:
    def test_campaign_sync_stats(self, store: Store):
        cid = store.campaign_create(CampaignConfig(name="t", template_key="k"))
        store.campaign_recipients_add(cid, [Recipient(chat_id=1), Recipient(chat_id=2)])

        # Enqueue tasks linked to campaign
        t1 = store.task_enqueue(Task(chat_id=1, text="a"), campaign_id=cid)
        t2 = store.task_enqueue(Task(chat_id=2, text="b"), campaign_id=cid)

        # Simulate sending
        store.task_lease_wrr(10, 30)
        store.task_ack(t1)
        store.task_mark_dead(t2)

        # Sync
        store.campaign_sync_stats(cid)
        stats = store.campaign_stats(cid)
        assert stats and stats.sent == 1
        assert stats.failed == 1


class TestCampaigns:
    def test_create_and_get(self, store: Store):
        cid = store.campaign_create(CampaignConfig(name="test", template_key="welcome"))
        c = store.campaign_get(cid)
        assert c and c.name == "test"
        assert c.status == "created"

    def test_add_recipients(self, store: Store):
        cid = store.campaign_create(CampaignConfig(name="t", template_key="k"))
        count = store.campaign_recipients_add(cid, [
            Recipient(chat_id=1, locale="en"),
            Recipient(chat_id=2, locale="ru", vars={"x": 1}),
        ])
        assert count == 2
        c = store.campaign_get(cid)
        assert c and c.total == 2

    def test_recipients_batch_and_mark(self, store: Store):
        cid = store.campaign_create(CampaignConfig(name="t", template_key="k"))
        store.campaign_recipients_add(cid, [
            Recipient(chat_id=1),
            Recipient(chat_id=2),
        ])

        batch = store.campaign_recipients_next_batch(cid, 10)
        assert len(batch) == 2

        store.campaign_recipients_mark_processed([b.id for b in batch])

        batch2 = store.campaign_recipients_next_batch(cid, 10)
        assert len(batch2) == 0

    def test_campaign_stats(self, store: Store):
        cid = store.campaign_create(CampaignConfig(name="t", template_key="k"))
        store.campaign_recipients_add(cid, [Recipient(chat_id=1), Recipient(chat_id=2)])
        store.campaign_progress_update(cid, sent_delta=1, failed_delta=0)

        stats = store.campaign_stats(cid)
        assert stats and stats.total == 2
        assert stats.sent == 1
        assert stats.pending == 1

    def test_campaign_status_transitions(self, store: Store):
        from telecast._models import CampaignStatus
        cid = store.campaign_create(CampaignConfig(name="t", template_key="k"))

        store.campaign_update_status(cid, CampaignStatus.RUNNING)
        running = store.campaign_list_running()
        assert len(running) == 1

        store.campaign_update_status(cid, CampaignStatus.PAUSED)
        running = store.campaign_list_running()
        assert len(running) == 0


class TestGauges:
    def test_queue_depth(self, store: Store):
        assert store.queue_depth() == 0
        store.task_enqueue(Task(chat_id=1, text="a"))
        store.task_enqueue(Task(chat_id=2, text="b"))
        assert store.queue_depth() == 2

    def test_dlq_depth(self, store: Store):
        tid = store.task_enqueue(Task(chat_id=1, text="hi"))
        store.task_lease_wrr(10, 30)
        store.task_mark_dead(tid)
        assert store.dlq_depth() == 1
