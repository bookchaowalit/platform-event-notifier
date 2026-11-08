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


# platform-credits SPEC-003 F7 / AC-81
def test_credits_mismatch_alert():
    text = render(
        {
            "type": "credits.topup.mismatch",
            "data": {
                "workspace_id": "w",
                "account_id": "11111111-aaaa-4bbb-8ccc-222222222222",
                "topup_id": "33333333-aaaa-4bbb-8ccc-444444444444",
                "intent_id": "55555555-aaaa-4bbb-8ccc-666666666666",
                "reason": "not_paid",
                "currency": "thb",
                "expected_amount": 50000,
                "observed_amount": None,
            },
        }
    )
    assert (
        text
        == "⚠️ Credits top-up mismatch (not_paid): expected 500.00 THB\ntopup 33333333 intent 55555555"
    )


def test_credits_refund_alert():
    text = render(
        {
            "type": "credits.topup.refunded",
            "data": {
                "account_id": "11111111-aaaa-4bbb-8ccc-222222222222",
                "topup_id": "33333333-aaaa-4bbb-8ccc-444444444444",
                "intent_id": "55555555-aaaa-4bbb-8ccc-666666666666",
                "currency": "thb",
                "amount": 50000,
                "refunded_amount": 20000,
            },
        }
    )
    assert text == (
        "⚠️ Credits top-up refunded: 200.00 THB of 500.00 THB — credits may need a manual reversal"
        "\ntopup 33333333 account 11111111"
    )


def test_other_credits_events_are_silent():
    for kind in ("credits.topup.settled", "credits.usage.captured"):
        assert render({"type": kind, "data": {"amount": 1, "currency": "thb"}}) is None
