# platform-event-notifier

The first [ADR-0008](https://github.com/bookchaowalit/architecture-docs/blob/main/docs/architecture/decisions/ADR-0008-messaging-queue-and-event-broker.md)
consumer that acts on events: consumer group `event-notifier` reads `commerce.events.v1` and
`data.ingest.v1` on the Aiven Kafka service and sends a Telegram message for

- `payment.intent.succeeded` (amount, provider, intent id), and
- an intake that ends `failed` or `rejected` (key, domain/dataset, attempt, error code).

Behaviour, guarantees and operations: [specs/001-event-notifier/spec.md](specs/001-event-notifier/spec.md).

## Run

```bash
uv sync --frozen --all-extras
.venv/bin/pytest
NOTIFIER_KAFKA_BOOTSTRAP=… NOTIFIER_KAFKA_CA_PEM=… NOTIFIER_KAFKA_USERNAME=… NOTIFIER_KAFKA_PASSWORD=… \
NOTIFIER_TELEGRAM_BOT_TOKEN=… NOTIFIER_TELEGRAM_CHAT_ID=… .venv/bin/event-notifier run
```

`event-notifier init-group` pins a new group at the topic ends (no-op once the group has commits).

Production runs come from `.github/workflows/notify.yml`, dispatched hourly at :58 UTC by the
platform-jobs-worker job `event-notifier`; heartbeat `event-notifier` on the uptime Worker.

To add a rule, add a branch to `render()` in `src/event_notifier/rules.py`, a test in
`tests/test_rules.py`, and, for a new topic, the topic to `TOPICS` plus a READ ACL for the
`event-notifier` Kafka user.
