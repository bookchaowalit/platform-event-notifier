"""Which events reach the owner, and as what text (SPEC-001).

A rule maps an ADR-0008 envelope to one plain-text Telegram message, or None to ignore it.
Messages carry only reference fields that the producing cell already put in the event.
"""

from __future__ import annotations

from typing import Any

# Stripe zero-decimal currencies: the minor unit is the whole unit.
ZERO_DECIMAL = {
    "bif", "clp", "djf", "gnf", "jpy", "kmf", "krw", "mga",
    "pyg", "rwf", "ugx", "vnd", "vuv", "xaf", "xof", "xpf",
}  # fmt: skip

INTAKE_ALERT_STATUSES = {"failed", "rejected"}


def money(amount: int, currency: str) -> str:
    code = currency.lower()
    if code in ZERO_DECIMAL:
        return f"{amount:,} {code.upper()}"
    return f"{amount / 100:,.2f} {code.upper()}"


def _payment_succeeded(data: dict[str, Any]) -> str:
    return (
        f"💰 Payment received: {money(int(data['amount']), str(data['currency']))}"
        f" via {data.get('provider', '?')}\nintent {data.get('intent_id', '?')}"
    )


def _intake_alert(data: dict[str, Any]) -> str:
    error = f", error {data['error_code']}" if data.get("error_code") else ""
    return (
        f"⚠️ Intake {data.get('status')}: {data.get('intake_key', '?')}"
        f"\n{data.get('domain', '?')}/{data.get('dataset', '?')}"
        f", attempt {data.get('attempt_count', 0)}{error}"
    )


def _short(value: Any) -> str:
    return str(value or "?")[:8]


def _credits_mismatch(data: dict[str, Any]) -> str:
    currency = str(data.get("currency") or "thb")
    expected = data.get("expected_amount")
    amount = f": expected {money(int(expected), currency)}" if isinstance(expected, int) else ""
    observed = data.get("observed_amount")
    seen = f", payment says {money(int(observed), currency)}" if isinstance(observed, int) else ""
    return (
        f"⚠️ Credits top-up mismatch ({data.get('reason', '?')}){amount}{seen}"
        f"\ntopup {_short(data.get('topup_id'))} intent {_short(data.get('intent_id'))}"
    )


def _credits_refunded(data: dict[str, Any]) -> str:
    currency = str(data.get("currency") or "thb")
    return (
        f"⚠️ Credits top-up refunded: {money(int(data['refunded_amount']), currency)}"
        f" of {money(int(data['amount']), currency)} — credits may need a manual reversal"
        f"\ntopup {_short(data.get('topup_id'))} account {_short(data.get('account_id'))}"
    )


def render(envelope: dict[str, Any]) -> str | None:
    """The message for one envelope, or None when no rule wants it."""
    kind = envelope.get("type")
    data = envelope.get("data")
    if not isinstance(kind, str) or not isinstance(data, dict):
        return None
    if kind == "payment.intent.succeeded":
        return _payment_succeeded(data)
    if kind.startswith("intake.intake.") and data.get("status") in INTAKE_ALERT_STATUSES:
        return _intake_alert(data)
    # platform-credits SPEC-003 F7: mismatches and refunds alert; settled / captured stay silent.
    if kind == "credits.topup.mismatch":
        return _credits_mismatch(data)
    if kind == "credits.topup.refunded":
        return _credits_refunded(data)
    return None
