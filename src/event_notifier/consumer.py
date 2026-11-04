"""Event notifier: the first ADR-0008 consumer that acts on events (SPEC-001).

Reads commerce.events.v1 and data.ingest.v1 as consumer group `event-notifier`, sends one
Telegram message per event a rule in rules.py wants, and commits each offset only after
its message was sent (at-least-once: a crash between send and commit repeats one message).
A send failure stops the run without committing, so the next run retries that event.

Run by .github/workflows/notify.yml (jobs queue `event-notifier`). Public log: the result
JSON holds counts and an error class only, never event content or broker detail.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from event_notifier.rules import render

TOPICS = ("commerce.events.v1", "data.ingest.v1")
GROUP_ID = "event-notifier"
JOIN_TIMEOUT_SECONDS = 30.0
IDLE_SECONDS = 5.0
RUN_SECONDS = 240.0


class Message(Protocol):
    def error(self) -> Any: ...
    def value(self) -> bytes | None: ...
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...


class Consumer(Protocol):
    def poll(self, timeout: float) -> Message | None: ...
    def commit(self, message: Message) -> None: ...
    def assigned(self) -> bool: ...
    def close(self) -> None: ...


class BrokerError(RuntimeError):
    """The broker could not be joined or read; the message is safe for a public log."""


class SendError(RuntimeError):
    """A notification was not delivered; the message is safe for a public log."""


@dataclass
class RunResult:
    seen: int = 0
    notified: int = 0
    ignored: int = 0
    invalid: int = 0
    status: str = "ok"
    error: str | None = None


def run(
    consumer: Consumer,
    send: Callable[[str], None],
    *,
    run_seconds: float = RUN_SECONDS,
    idle_seconds: float = IDLE_SECONDS,
    join_timeout: float = JOIN_TIMEOUT_SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> RunResult:
    """Drain the group until it has been idle for `idle_seconds` or `run_seconds` pass.

    A broker or send failure ends the run with status "error" and the counts so far.
    """
    result = RunResult()
    try:
        _drain(consumer, send, result, run_seconds, idle_seconds, join_timeout, clock)
    except (BrokerError, SendError) as exc:
        result.status, result.error = "error", str(exc)
    return result


def _drain(
    consumer: Consumer,
    send: Callable[[str], None],
    result: RunResult,
    run_seconds: float,
    idle_seconds: float,
    join_timeout: float,
    clock: Callable[[], float],
) -> None:
    start = clock()
    idle_since: float | None = None
    while clock() - start < run_seconds:
        message = consumer.poll(1.0)
        if message is None:
            if not consumer.assigned():
                if clock() - start >= join_timeout:
                    raise BrokerError("consumer group not joined")
                continue
            idle_since = idle_since if idle_since is not None else clock()
            if clock() - idle_since >= idle_seconds:
                break
            continue
        idle_since = None
        if message.error() is not None:
            # Partition EOF is not raised (enable.partition.eof off); anything else is fatal.
            raise BrokerError("consume error")
        result.seen += 1
        try:
            envelope = json.loads(message.value() or b"")
        except (ValueError, UnicodeDecodeError):
            envelope = None
        text = None
        if isinstance(envelope, dict):
            try:
                text = render(envelope)
            except (KeyError, ValueError, TypeError, OverflowError):
                envelope = None  # a malformed event can never succeed: count it, commit, move on
        if not isinstance(envelope, dict):
            result.invalid += 1
        elif text is None:
            result.ignored += 1
        else:
            send(text)  # SendError: stop before committing, retried next run
            result.notified += 1
        consumer.commit(message)


class KafkaConsumer:
    """confluent-kafka adapter (kafka-python-ng cannot authenticate to Kafka 4.x brokers)."""

    def __init__(self, config: dict[str, Any], topics: tuple[str, ...] = TOPICS):
        from confluent_kafka import Consumer as _Consumer

        self._assigned = False
        self._consumer: Any = _Consumer(
            {
                **config,
                "group.id": GROUP_ID,
                "enable.auto.commit": False,
                # A new group starts at the end: init-group pins it there before the first run.
                "auto.offset.reset": "latest",
            }
        )
        self._consumer.subscribe(list(topics), on_assign=self._on_assign)

    def _on_assign(self, _consumer: Any, partitions: list[Any]) -> None:
        self._assigned = True

    def assigned(self) -> bool:
        return self._assigned

    def poll(self, timeout: float) -> Message | None:
        return self._consumer.poll(timeout)  # type: ignore[no-any-return]

    def commit(self, message: Message) -> None:
        for part in self._consumer.commit(message=message, asynchronous=False) or []:
            if part.error is not None:
                raise BrokerError("commit failed")

    def close(self) -> None:
        self._consumer.close()


def init_group(config: dict[str, Any], topics: tuple[str, ...] = TOPICS) -> list[str]:
    """Pin a new group at the current end of each topic, so it never replays old events.

    No-op when the group already has commits. Without this, `latest` would skip events
    produced between runs until the group's first commit.
    """
    from confluent_kafka import Consumer as _Consumer
    from confluent_kafka import TopicPartition

    consumer = _Consumer({**config, "group.id": GROUP_ID, "enable.auto.commit": False})
    try:
        ends = []
        for topic in topics:
            meta = consumer.list_topics(topic, timeout=15).topics[topic]
            for partition in meta.partitions:
                _low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=15
                )
                ends.append(TopicPartition(topic, partition, high))
        committed = consumer.committed(
            [TopicPartition(t.topic, t.partition) for t in ends], timeout=15
        )
        if any(t.offset >= 0 for t in committed):
            return ["already initialised"]
        consumer.commit(offsets=ends, asynchronous=False)
        return [f"{t.topic}[{t.partition}]@{t.offset}" for t in ends]
    finally:
        consumer.close()


def telegram_sender(token: str, chat_id: str, timeout: float = 15.0) -> Callable[[str], None]:
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    def send(text: str) -> None:
        body = urllib.parse.urlencode(
            {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}
        ).encode()
        try:
            with urllib.request.urlopen(url, data=body, timeout=timeout) as response:
                ok = json.loads(response.read()).get("ok")
        except Exception as exc:  # noqa: BLE001 - any failure: report its class only
            # The token is in the URL: never let the exception text reach the log.
            raise SendError(f"telegram send failed ({type(exc).__name__})") from None
        if not ok:
            raise SendError("telegram send failed (not ok)")

    return send


def kafka_config_from_env(environ: Mapping[str, str] = os.environ) -> dict[str, Any]:
    """SASL_SSL settings from NOTIFIER_KAFKA_* (Infisical /aiven: event-notifier)."""
    return {
        "bootstrap.servers": environ["NOTIFIER_KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": environ.get("NOTIFIER_KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": environ["NOTIFIER_KAFKA_USERNAME"],
        "sasl.password": environ["NOTIFIER_KAFKA_PASSWORD"],
        "ssl.ca.pem": environ["NOTIFIER_KAFKA_CA_PEM"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="event-notifier", description=__doc__)
    parser.add_argument("command", choices=["run", "init-group"])
    parser.add_argument("--run-seconds", type=float, default=RUN_SECONDS)
    args = parser.parse_args(argv)
    try:
        config = kafka_config_from_env()
        if args.command == "init-group":
            print(json.dumps({"status": "ok", "positions": init_group(config)}))
            return 0
        send = telegram_sender(
            os.environ["NOTIFIER_TELEGRAM_BOT_TOKEN"], os.environ["NOTIFIER_TELEGRAM_CHAT_ID"]
        )
        consumer = KafkaConsumer(config)
    except KeyError as exc:
        print(json.dumps({"status": "error", "error": f"missing {exc.args[0]}"}))
        return 2
    try:
        result = run(consumer, send, run_seconds=args.run_seconds)
    finally:
        consumer.close()
    print(json.dumps(asdict(result)))
    return 0 if result.status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
