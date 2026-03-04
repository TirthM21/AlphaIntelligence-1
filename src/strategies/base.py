"""Base strategy interface and shared normalization helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class BaseStrategy(ABC):
    """Protocol-style strategy interface for scan orchestration."""

    name: str

    @abstractmethod
    def generate_signals(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Generate a normalized payload with buy/sell signals and metadata."""

    @abstractmethod
    def score(self, candidate: Dict[str, Any]) -> float:
        """Compute a numeric score for a candidate item."""

    @abstractmethod
    def risk_rules(self) -> Dict[str, Any]:
        """Return risk settings used by the strategy."""

    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """Return strategy metadata for reporting and introspection."""

    def _normalize_signal(
        self,
        *,
        ticker: str,
        side: str,
        score: float,
        reason: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        raw_signal: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "ticker": ticker,
            "side": side,
            "score": float(score),
            "reason": reason or "",
            "details": details or {},
            "raw": raw_signal or {},
        }

    def _payload(self, buy_signals: List[Dict[str, Any]], sell_signals: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "strategy": self.name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "metadata": self.metadata(),
            "risk_rules": self.risk_rules(),
            "signals": {
                "buy": buy_signals,
                "sell": sell_signals,
            },
        }
