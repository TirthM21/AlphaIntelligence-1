"""Long-term strategy adapter around portfolio construction modules."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.long_term.portfolio_constructor import PortfolioConstructor

from .base import BaseStrategy


class LongTermStrategy(BaseStrategy):
    """Adapter for long-term portfolio construction and scoring outputs."""

    name = "long_term"

    def __init__(self) -> None:
        self.portfolio_constructor = PortfolioConstructor()

    def generate_signals(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}
        stocks = context.get("stocks", [])
        etfs = context.get("etfs", [])
        sector_map = context.get("sector_map", {})
        theme_map = context.get("theme_map", {})

        portfolio = self.portfolio_constructor.build_portfolio(
            stocks=stocks,
            etfs=etfs,
            sector_map=sector_map,
            theme_map=theme_map,
        )

        if not portfolio:
            return self._payload([], [])

        buy_signals: List[Dict[str, Any]] = []
        for ticker, allocation in portfolio.allocations.items():
            score = self.score(
                {
                    "ticker": ticker,
                    "allocation": allocation,
                    "portfolio_score": portfolio.total_score,
                }
            )
            buy_signals.append(
                self._normalize_signal(
                    ticker=ticker,
                    side="buy",
                    score=score,
                    reason="Long-term allocation candidate",
                    details={
                        "target_allocation": allocation,
                        "portfolio_total_score": portfolio.total_score,
                    },
                )
            )

        buy_signals.sort(key=lambda item: item["score"], reverse=True)
        return self._payload(buy_signals, [])

    def score(self, candidate: Dict[str, Any]) -> float:
        allocation = float(candidate.get("allocation", 0.0))
        portfolio_score = float(candidate.get("portfolio_score", 0.0))
        return round(allocation * 100.0 + portfolio_score, 4)

    def risk_rules(self) -> Dict[str, Any]:
        return {
            "max_stock_count": self.portfolio_constructor.rules.max_stock_count,
            "max_etf_count": self.portfolio_constructor.rules.max_etf_count,
            "max_single_stock_allocation": self.portfolio_constructor.rules.max_stock_position,
            "max_single_etf_allocation": self.portfolio_constructor.rules.max_etf_position,
        }

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "family": "compounder",
            "timeframe": "quarterly",
            "source_module": "src.long_term.portfolio_constructor",
        }
