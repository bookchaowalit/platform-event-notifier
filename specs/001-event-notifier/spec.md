# SPEC-001 Event notifier

Status: accepted 2026-11-04 (owner: "Build first consumer") · ADR-0008 consumer #1

## Problem

ADR-0008 M1–M5 put domain events on Kafka (payment, knowledge, intake), but the only reader is the
lake sink. Nothing acts on an event, so the owner still learns about a payment or a failed intake by
looking. The first consumer turns two kinds of events into a Telegram message.

## Behaviour

| Event | Topic | Message |
|---|---|---|
| `payment.intent.succeeded` | `commerce.events.v1` | 💰 amount (minor units → major; zero-decimal currencies as is), provider, intent id |
| `intake.intake.failed`, `intake.intake.rejected` | `data.ingest.v1` | ⚠️ intake key, domain/dataset, attempt, error code |

Every other event is read, counted as ignored and committed. A body that is not a JSON object is
counted as invalid and committed (it can never succeed); so is a matching event whose fields cannot be rendered (for example a payment without an amount), so one bad event never blocks the group.

## Guarantees

- Consumer group `event-notifier`, its own SASL user with READ on the two topics only.
- At-least-once: an offset is committed only after its message was sent. A crash between send and
  commit repeats that one message; a send failure stops the run without committing, so the next run
  retries it.
- The group was pinned at the topic ends on creation (`event-notifier init-group`), so it never
  replays history and never skips events produced between runs.
- Each run drains until 5 s idle or 240 s, then exits; a run that cannot join the group in 30 s fails.

## Operations

- `.github/workflows/notify.yml`, dispatched by platform-jobs-worker job `event-notifier` at :58 (after
  the intake :53 and knowledge :55 relays). End-to-end latency is up to one hour, bounded by the
  hourly relays.
- Heartbeat `event-notifier` (3 h) on the uptime Worker alerts on a failed or missing run.
- Public log: counts and an error class only. Secrets: Infisical `/aiven` (`KAFKA_EVENT_NOTIFIER_*`)
  and `/telegram` (`TELEGRAM_NOTI_*`), copied to repository secrets `NOTIFIER_*`.

## Tests

`tests/test_rules.py` (message text per rule, ignored events) and `tests/test_consumer.py` (commit
after send, no commit on send failure, consume error, join timeout, idle and time budget, token never
in an error).
