import io
import json
import urllib.request

import pytest

from event_notifier import consumer as mod
from event_notifier.consumer import SendError, kafka_config_from_env, run, telegram_sender


class FakeMessage:
    def __init__(self, value, offset, error=None):
        self._value, self._offset, self._error = value, offset, error

    def error(self):
        return self._error

    def value(self):
        return self._value

    def topic(self):
        return "data.ingest.v1"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, messages, assigned=True):
        self.messages = list(messages)
        self.committed = []
        self._assigned = assigned

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, message):
        self.committed.append(message.offset())

    def assigned(self):
        return self._assigned

    def close(self):
        pass


class Clock:
    """Each call advances one second, like one empty poll."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        self.now += 1.0
        return self.now


def event(kind, **data):
    return json.dumps({"id": "e", "type": kind, "data": data}).encode()


FAILED = event("intake.intake.failed", status="failed", intake_key="k")
LANDED = event("intake.intake.landed", status="landed", intake_key="k")


def test_notifies_matching_events_and_commits_every_offset():
    sent = []
    fake = FakeConsumer(
        [FakeMessage(LANDED, 0), FakeMessage(FAILED, 1), FakeMessage(b"not json", 2)]
    )
    result = run(fake, sent.append, clock=Clock())
    assert (result.seen, result.notified, result.ignored, result.invalid) == (3, 1, 1, 1)
    assert result.status == "ok"
    assert len(sent) == 1 and "Intake failed" in sent[0]
    assert fake.committed == [0, 1, 2]


def test_malformed_matching_event_is_invalid_not_fatal():
    sent = []
    broken = event("payment.intent.succeeded", currency="thb")  # no amount
    fake = FakeConsumer([FakeMessage(broken, 0), FakeMessage(FAILED, 1)])
    result = run(fake, sent.append, clock=Clock())
    assert (result.status, result.invalid, result.notified) == ("ok", 1, 1)
    assert fake.committed == [0, 1]


def test_send_failure_stops_before_commit():
    def broken(_text):
        raise SendError("telegram send failed (URLError)")

    fake = FakeConsumer([FakeMessage(LANDED, 0), FakeMessage(FAILED, 1), FakeMessage(LANDED, 2)])
    result = run(fake, broken, clock=Clock())
    assert result.status == "error" and "URLError" in (result.error or "")
    assert fake.committed == [0]  # offset 1 is retried next run; 2 was never read
    assert result.seen == 2


def test_consume_error_is_reported():
    fake = FakeConsumer([FakeMessage(None, 0, error="boom")])
    result = run(fake, lambda _t: None, clock=Clock())
    assert (result.status, result.error) == ("error", "consume error")
    assert fake.committed == []


def test_join_timeout():
    result = run(FakeConsumer([], assigned=False), lambda _t: None, join_timeout=3, clock=Clock())
    assert (result.status, result.error) == ("error", "consumer group not joined")


def test_idle_group_ends_run_cleanly():
    result = run(FakeConsumer([]), lambda _t: None, idle_seconds=2, clock=Clock())
    assert result.status == "ok" and result.seen == 0


def test_run_budget_bounds_a_busy_group():
    fake = FakeConsumer([FakeMessage(LANDED, i) for i in range(100)])
    result = run(fake, lambda _t: None, run_seconds=10, clock=Clock())
    assert result.status == "ok" and 0 < result.seen < 100


def test_telegram_sender_hides_token_on_failure(monkeypatch):
    def fail(url, data, timeout):
        raise OSError(f"cannot reach {url}")

    monkeypatch.setattr(urllib.request, "urlopen", fail)
    with pytest.raises(SendError) as info:
        telegram_sender("SECRET-TOKEN", "1")("hi")
    assert "SECRET-TOKEN" not in str(info.value) and "OSError" in str(info.value)
    assert info.value.__cause__ is None


def test_telegram_sender_checks_ok(monkeypatch):
    calls = []

    def fake_urlopen(url, data, timeout):
        calls.append(data)
        return io.BytesIO(json.dumps({"ok": len(calls) == 1}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    send = telegram_sender("t", "42")
    send("hello")
    assert b"chat_id=42" in calls[0] and b"text=hello" in calls[0]
    with pytest.raises(SendError, match="not ok"):
        send("again")


def test_kafka_config_from_env():
    env = {
        "NOTIFIER_KAFKA_BOOTSTRAP": "b:1",
        "NOTIFIER_KAFKA_USERNAME": "event-notifier",
        "NOTIFIER_KAFKA_PASSWORD": "p",
        "NOTIFIER_KAFKA_CA_PEM": "pem",
    }
    config = kafka_config_from_env(env)
    assert config["sasl.mechanism"] == "SCRAM-SHA-256" and config["ssl.ca.pem"] == "pem"


def test_main_reports_missing_settings(monkeypatch, capsys):
    for key in list(mod.os.environ):
        if key.startswith("NOTIFIER_"):
            monkeypatch.delenv(key)
    assert mod.main(["run"]) == 2
    assert json.loads(capsys.readouterr().out)["error"] == "missing NOTIFIER_KAFKA_BOOTSTRAP"
