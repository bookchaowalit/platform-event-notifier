from event_notifier.rules import money, render


def envelope(kind, **data):
    return {"id": f"{kind}:1", "type": kind, "version": 1, "data": data}


def test_payment_succeeded_message():
    text = render(
        envelope(
            "payment.intent.succeeded",
            amount=150000,
            currency="thb",
            provider="stripe",
            intent_id="0b6c",
        )
    )
    assert text == "💰 Payment received: 1,500.00 THB via stripe\nintent 0b6c"


def test_zero_decimal_currency():
    assert money(5000, "jpy") == "5,000 JPY"
    assert money(199, "usd") == "1.99 USD"


def test_intake_failed_and_rejected_alert():
    text = render(
        envelope(
            "intake.intake.failed",
            status="failed",
            intake_key="x-feed-1",
            domain="social",
            dataset="x_posts",
            attempt_count=3,
            error_code="lake_timeout",
        )
    )
    assert text == "⚠️ Intake failed: x-feed-1\nsocial/x_posts, attempt 3, error lake_timeout"
    rejected = render(envelope("intake.intake.rejected", status="rejected", intake_key="k"))
    assert rejected is not None and "error" not in rejected


def test_other_events_are_ignored():
    assert render(envelope("intake.intake.landed", status="landed")) is None
    assert render(envelope("ops.heartbeat.emitted", run_id="1")) is None
    assert render({"type": "payment.intent.succeeded"}) is None  # no data
    assert render({"data": {}}) is None
