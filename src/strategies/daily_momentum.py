"""Daily momentum strategy adapter around screening signal engine."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.screening.signal_engine import score_buy_signal, score_sell_signal

from .base import BaseStrategy


class DailyMomentumStrategy(BaseStrategy):
    """Adapter for existing daily momentum signal-engine logic."""

    name = "daily_momentum"

    def generate_signals(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}
        analyses: List[Dict[str, Any]] = context.get("analyses", [])

        buy_signals: List[Dict[str, Any]] = []
        sell_signals: List[Dict[str, Any]] = []

        for analysis in analyses:
            ticker = analysis.get("ticker", "")
            phase_info = analysis.get("phase_info", {})
            phase = phase_info.get("phase")
            if phase in (1, 2):
                raw_buy = score_buy_signal(
                    ticker=ticker,
                    price_data=analysis["price_data"],
                    current_price=analysis["current_price"],
                    phase_info=phase_info,
                    rs_series=analysis["rs_series"],
                    fundamentals=analysis.get("quarterly_data"),
                    vcp_data=analysis.get("vcp_data"),
                )
                if raw_buy.get("is_buy"):
                    buy_signals.append(
                        self._normalize_signal(
                            ticker=ticker,
                            side="buy",
                            score=raw_buy.get("score", 0.0),
                            reason=raw_buy.get("reason", ""),
                            details=raw_buy.get("details", {}),
                            raw_signal=raw_buy,
                        )
                    )

            if phase in (3, 4):
                raw_sell = score_sell_signal(
                    ticker=ticker,
                    price_data=analysis["price_data"],
                    current_price=analysis["current_price"],
                    phase_info=phase_info,
                    rs_series=analysis["rs_series"],
                    fundamentals=analysis.get("quarterly_data"),
                )
                if raw_sell.get("is_sell"):
                    sell_signals.append(
                        self._normalize_signal(
                            ticker=ticker,
                            side="sell",
                            score=raw_sell.get("score", 0.0),
                            reason=raw_sell.get("reason", ""),
                            details=raw_sell.get("details", {}),
                            raw_signal=raw_sell,
                        )
                    )

        buy_signals.sort(key=lambda item: item["score"], reverse=True)
        sell_signals.sort(key=lambda item: item["score"], reverse=True)
        return self._payload(buy_signals, sell_signals)

    def score(self, candidate: Dict[str, Any]) -> float:
        return float(candidate.get("score", 0.0))

    def risk_rules(self) -> Dict[str, Any]:
        return {
            "max_positions": 15,
            "min_buy_score": 70,
            "min_sell_score": 60,
        }

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "family": "momentum",
            "timeframe": "daily",
            "source_module": "src.screening.signal_engine",
        }
