"""Event calendar model for scan-time risk adjustments.

Tags symbols with earnings and macro event proximity, then applies configurable
entry controls (block/downweight) to scan signals.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


RISK_ORDER = {"none": 0, "low": 1, "medium": 2, "high": 3}


@dataclass(frozen=True)
class EventRiskAssessment:
    ticker: str
    risk_level: str
    reasons: List[str]
    blocked: bool
    score_multiplier: float

    @property
    def reason_text(self) -> str:
        if not self.reasons:
            return "No near-term event risk detected."
        return "; ".join(self.reasons)


class EventCalendarModel:
    """Assess event proximity risk and apply configured entry controls."""

    def __init__(self, rules: Optional[Dict[str, Any]] = None, payload: Optional[Dict[str, Any]] = None):
        rules = rules or {}
        self.enabled = bool(rules.get("enabled", True))
        self.earnings_window_days = int(rules.get("earnings_window_days", 3))
        self.macro_window_days = int(rules.get("macro_window_days", 2))
        self.block_entry_levels = {str(v).lower() for v in rules.get("block_entry_levels", ["high"])}
        self.downweight = {
            str(k).lower(): float(v)
            for k, v in (rules.get("downweight") or {"medium": 0.85, "high": 0.65}).items()
        }
        self.payload = payload or {}

    @staticmethod
    def _parse_date(raw: Any) -> Optional[date]:
        if raw is None:
            return None
        if isinstance(raw, date):
            return raw
        text = str(raw).strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(text[:10], fmt).date()
            except ValueError:
                continue
        return None

    def assess_symbol(self, ticker: str, as_of: Optional[date] = None) -> EventRiskAssessment:
        if not self.enabled:
            return EventRiskAssessment(ticker=ticker, risk_level="none", reasons=[], blocked=False, score_multiplier=1.0)

        today = as_of or date.today()
        symbol = str(ticker).upper()

        level = "none"
        reasons: List[str] = []

        for event in self.payload.get("earnings", []):
            if str(event.get("symbol", "")).upper() != symbol:
                continue
            event_date = self._parse_date(event.get("date"))
            if not event_date:
                continue
            delta = (event_date - today).days
            if 0 <= delta <= self.earnings_window_days:
                level = self._max_level(level, "high")
                reasons.append(f"Earnings in {delta} day(s) ({event_date.isoformat()})")

        for event in self.payload.get("macro_events", []):
            event_date = self._parse_date(event.get("date"))
            if not event_date:
                continue
            delta = (event_date - today).days
            if 0 <= delta <= self.macro_window_days:
                name = str(event.get("name") or event.get("category") or "Macro event")
                severity = str(event.get("severity") or "medium").lower()
                level = self._max_level(level, "high" if severity == "high" else "medium")
                reasons.append(f"{name} in {delta} day(s) ({event_date.isoformat()})")

        blocked = level in self.block_entry_levels
        multiplier = float(self.downweight.get(level, 1.0))

        return EventRiskAssessment(
            ticker=symbol,
            risk_level=level,
            reasons=reasons,
            blocked=blocked,
            score_multiplier=multiplier,
        )

    @staticmethod
    def _max_level(current: str, candidate: str) -> str:
        return candidate if RISK_ORDER.get(candidate, 0) > RISK_ORDER.get(current, 0) else current

    def apply_to_signal(self, signal: Dict[str, Any], as_of: Optional[date] = None) -> Optional[Dict[str, Any]]:
        """Attach event risk metadata and apply block/downweight rules.

        Returns None when the signal should be blocked.
        """
        assessment = self.assess_symbol(signal.get("ticker", ""), as_of=as_of)

        if assessment.blocked:
            return None

        updated = dict(signal)
        base_score = float(updated.get("score") or 0)
        adjusted_score = round(base_score * assessment.score_multiplier, 1)
        updated["score"] = adjusted_score
        updated["event_risk_level"] = assessment.risk_level
        updated["event_risk_reason"] = assessment.reason_text
        updated["event_score_multiplier"] = assessment.score_multiplier

        reasons = list(updated.get("reasons") or [])
        reasons.append(f"Event risk: {assessment.risk_level} — {assessment.reason_text}")
        updated["reasons"] = reasons
        return updated


def load_event_calendar_model(config_path: str = "config.yaml") -> EventCalendarModel:
    """Load event risk rules + mocked payload from config-defined source."""
    try:
        raw_config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
    except Exception:
        raw_config = {}

    rules = raw_config.get("event_risk") or {}
    payload = rules.get("mock_payload") or {}

    payload_path = rules.get("mock_payload_path")
    if payload_path:
        try:
            payload = yaml.safe_load(Path(payload_path).read_text(encoding="utf-8")) or payload
        except Exception:
            pass

    return EventCalendarModel(rules=rules, payload=payload)
