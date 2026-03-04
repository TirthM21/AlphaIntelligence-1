from datetime import date

from src.data.event_calendar import EventCalendarModel
from src.reporting.newsletter_generator import NewsletterGenerator


def test_event_calendar_blocks_high_risk_symbol_with_earnings_and_cpi():
    model = EventCalendarModel(
        rules={
            "enabled": True,
            "earnings_window_days": 3,
            "macro_window_days": 2,
            "block_entry_levels": ["high"],
            "downweight": {"medium": 0.8, "high": 0.6},
        },
        payload={
            "earnings": [{"symbol": "AAPL", "date": "2026-01-11"}],
            "macro_events": [{"name": "US CPI", "date": "2026-01-10", "severity": "high"}],
        },
    )

    assessment = model.assess_symbol("AAPL", as_of=date(2026, 1, 9))

    assert assessment.risk_level == "high"
    assert assessment.blocked is True
    assert "Earnings in 2 day(s)" in assessment.reason_text
    assert "US CPI in 1 day(s)" in assessment.reason_text


def test_event_calendar_downweights_medium_risk_signal_without_blocking():
    model = EventCalendarModel(
        rules={
            "enabled": True,
            "earnings_window_days": 0,
            "macro_window_days": 2,
            "block_entry_levels": ["high"],
            "downweight": {"medium": 0.8, "high": 0.6},
        },
        payload={
            "earnings": [],
            "macro_events": [{"name": "FOMC", "date": "2026-01-10", "severity": "medium"}],
        },
    )

    signal = {"ticker": "MSFT", "score": 75.0, "reasons": ["Base setup"]}
    adjusted = model.apply_to_signal(signal, as_of=date(2026, 1, 9))

    assert adjusted is not None
    assert adjusted["score"] == 60.0
    assert adjusted["event_risk_level"] == "medium"
    assert "Event risk: medium" in adjusted["reasons"][-1]


def test_newsletter_event_risk_note_renders_reason_text():
    generator = NewsletterGenerator.__new__(NewsletterGenerator)

    note = generator._format_event_risk_note(
        {"event_risk_level": "high", "event_risk_reason": "Earnings in 1 day (2026-01-10)"}
    )

    assert note == "Event Risk (HIGH): Earnings in 1 day (2026-01-10)"
