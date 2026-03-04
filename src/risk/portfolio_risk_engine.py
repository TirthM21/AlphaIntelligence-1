"""Portfolio risk engine for final signal selection.

Applies portfolio-aware filters to candidate buy signals:
- Position/portfolio constraints (single-name, sector/theme, beta, cash floor)
- Correlation clustering to avoid redundant exposures
- Volatility targeting using ATR or rolling realized volatility
- Persistent per-symbol decisions for auditability
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RiskConfig:
    """Configuration for portfolio-level risk controls."""

    max_single_name_weight: float = 0.10
    sector_cap: float = 0.35
    theme_cap: float = 0.35
    beta_cap: float = 1.20
    cash_floor: float = 0.10
    correlation_threshold: float = 0.80
    max_per_cluster: int = 2
    target_annualized_volatility: float = 0.25
    vol_lookback: int = 20
    atr_lookback: int = 14
    min_weight: float = 0.01


class PortfolioRiskEngine:
    """Evaluate candidate signals against portfolio risk rules."""

    def __init__(self, config: Optional[RiskConfig] = None):
        self.config = config or RiskConfig()

    def apply(
        self,
        candidate_signals: List[Dict],
        analysis_by_ticker: Dict[str, Dict],
        max_positions: Optional[int] = None,
    ) -> Tuple[List[Dict], List[Dict]]:
        """Return accepted signals and full decision log."""
        if not candidate_signals:
            return [], []

        ordered = sorted(candidate_signals, key=lambda x: x.get("score", 0), reverse=True)
        target_positions = max_positions or len(ordered)
        max_invested = max(0.0, 1.0 - self.config.cash_floor)
        base_weight = max_invested / max(target_positions, 1)

        accepted: List[Dict] = []
        decisions: List[Dict] = []
        sector_exposure: Dict[str, float] = {}
        theme_exposure: Dict[str, float] = {}
        accepted_betas: List[Tuple[float, float]] = []
        cluster_counts: Dict[str, int] = {}

        for signal in ordered:
            ticker = signal.get("ticker", "UNKNOWN")
            analysis = analysis_by_ticker.get(ticker, {})
            sector = self._extract_field(signal, analysis, "sector", fallback="Unknown")
            theme = self._extract_field(signal, analysis, "theme", fallback="General")
            beta = self._extract_beta(signal, analysis)
            returns = self._extract_returns(analysis)
            vol_multiplier, vol_method, realized_vol = self._volatility_multiplier(analysis)
            proposed_weight = base_weight * vol_multiplier

            decision = {
                "ticker": ticker,
                "score": signal.get("score"),
                "sector": sector,
                "theme": theme,
                "beta": beta,
                "vol_target_method": vol_method,
                "realized_vol": realized_vol,
                "base_weight": round(base_weight, 6),
                "vol_multiplier": round(vol_multiplier, 6),
                "proposed_weight": round(proposed_weight, 6),
                "accepted": False,
                "reason": "",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

            if proposed_weight < self.config.min_weight:
                decision["reason"] = f"rejected: proposed_weight<{self.config.min_weight:.2%}"
                decisions.append(decision)
                continue

            if proposed_weight > self.config.max_single_name_weight:
                decision["reason"] = "rejected: max_single_name_weight breach"
                decisions.append(decision)
                continue

            if sector_exposure.get(sector, 0.0) + proposed_weight > self.config.sector_cap:
                decision["reason"] = "rejected: sector_cap breach"
                decisions.append(decision)
                continue

            if theme_exposure.get(theme, 0.0) + proposed_weight > self.config.theme_cap:
                decision["reason"] = "rejected: theme_cap breach"
                decisions.append(decision)
                continue

            new_beta_avg = self._projected_portfolio_beta(accepted_betas, beta, proposed_weight)
            if new_beta_avg > self.config.beta_cap:
                decision["reason"] = "rejected: beta_cap breach"
                decisions.append(decision)
                continue

            cluster_id = self._cluster_id(ticker, returns, accepted)
            decision["cluster_id"] = cluster_id
            if cluster_counts.get(cluster_id, 0) >= self.config.max_per_cluster:
                decision["reason"] = "rejected: correlation_cluster limit"
                decisions.append(decision)
                continue

            enriched_signal = dict(signal)
            enriched_signal["risk_target_weight"] = round(proposed_weight, 6)
            enriched_signal["risk_cluster_id"] = cluster_id
            enriched_signal["risk_sector"] = sector
            enriched_signal["risk_theme"] = theme
            enriched_signal["risk_beta"] = beta
            enriched_signal["risk_realized_vol"] = realized_vol
            enriched_signal["risk_vol_method"] = vol_method
            enriched_signal["_risk_returns"] = returns

            accepted.append(enriched_signal)
            sector_exposure[sector] = sector_exposure.get(sector, 0.0) + proposed_weight
            theme_exposure[theme] = theme_exposure.get(theme, 0.0) + proposed_weight
            accepted_betas.append((beta, proposed_weight))
            cluster_counts[cluster_id] = cluster_counts.get(cluster_id, 0) + 1

            decision["accepted"] = True
            decision["reason"] = "accepted"
            decision["post_sector_exposure"] = round(sector_exposure.get(sector, 0.0), 6)
            decision["post_theme_exposure"] = round(theme_exposure.get(theme, 0.0), 6)
            decisions.append(decision)

            if len(accepted) >= target_positions:
                break

        for signal in accepted:
            signal.pop("_risk_returns", None)

        return accepted, decisions

    def persist_decisions(self, decisions: List[Dict], output_dir: str = "./data/risk_decisions") -> Optional[str]:
        """Persist full risk decisions for auditability."""
        if not decisions:
            return None

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"risk_decisions_{timestamp}.json"
        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "config": self.config.__dict__,
            "decisions": decisions,
            "accepted_count": sum(1 for d in decisions if d.get("accepted")),
            "rejected_count": sum(1 for d in decisions if not d.get("accepted")),
        }
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.info("Risk decisions saved: %s", out_path)
        return str(out_path)

    def _extract_returns(self, analysis: Dict) -> pd.Series:
        price_data = analysis.get("price_data")
        if isinstance(price_data, pd.DataFrame) and "Close" in price_data.columns:
            return price_data["Close"].pct_change().dropna().tail(63)
        return pd.Series(dtype=float)

    def _extract_field(self, signal: Dict, analysis: Dict, field: str, fallback: str) -> str:
        for source in (signal, analysis, analysis.get("fundamental_analysis", {}), analysis.get("quarterly_data", {})):
            if isinstance(source, dict) and source.get(field):
                return str(source.get(field))
        return fallback

    def _extract_beta(self, signal: Dict, analysis: Dict) -> float:
        for source in (signal, analysis, analysis.get("fundamental_analysis", {}), analysis.get("quarterly_data", {})):
            if isinstance(source, dict) and source.get("beta") is not None:
                try:
                    return float(source.get("beta"))
                except (TypeError, ValueError):
                    continue
        return 1.0

    def _volatility_multiplier(self, analysis: Dict) -> Tuple[float, str, float]:
        price_data = analysis.get("price_data")
        if not isinstance(price_data, pd.DataFrame) or len(price_data) < self.config.vol_lookback + 1:
            return 1.0, "none", 0.0

        realized_vol = self._atr_volatility(price_data)
        method = "atr"
        if realized_vol <= 0:
            returns = price_data["Close"].pct_change().dropna().tail(self.config.vol_lookback)
            if returns.empty:
                return 1.0, "none", 0.0
            realized_vol = float(returns.std() * np.sqrt(252))
            method = "rolling"

        if realized_vol <= 0:
            return 1.0, "none", 0.0

        multiplier = self.config.target_annualized_volatility / realized_vol
        multiplier = float(np.clip(multiplier, 0.5, 1.5))
        return multiplier, method, float(round(realized_vol, 6))

    def _atr_volatility(self, price_data: pd.DataFrame) -> float:
        required = {"High", "Low", "Close"}
        if not required.issubset(price_data.columns):
            return 0.0

        high = price_data["High"]
        low = price_data["Low"]
        close = price_data["Close"]
        prev_close = close.shift(1)

        true_range = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)

        atr = true_range.rolling(self.config.atr_lookback).mean().iloc[-1]
        latest_close = close.iloc[-1]
        if pd.isna(atr) or latest_close <= 0:
            return 0.0

        daily_vol = float(atr / latest_close)
        return daily_vol * np.sqrt(252)

    def _projected_portfolio_beta(
        self,
        weighted_betas: List[Tuple[float, float]],
        candidate_beta: float,
        candidate_weight: float,
    ) -> float:
        total_weight = candidate_weight + sum(weight for _, weight in weighted_betas)
        if total_weight <= 0:
            return 0.0

        weighted_sum = candidate_beta * candidate_weight
        weighted_sum += sum(beta * weight for beta, weight in weighted_betas)
        return weighted_sum / total_weight

    def _cluster_id(self, ticker: str, returns: pd.Series, accepted: List[Dict]) -> str:
        if returns.empty:
            return f"singleton:{ticker}"

        for existing in accepted:
            existing_returns = existing.get("_risk_returns")
            if not isinstance(existing_returns, pd.Series) or existing_returns.empty:
                continue

            joined = pd.concat([returns, existing_returns], axis=1, join="inner").dropna()
            if len(joined) < 20:
                continue

            corr = joined.iloc[:, 0].corr(joined.iloc[:, 1])
            if corr is not None and corr >= self.config.correlation_threshold:
                return existing.get("risk_cluster_id", f"singleton:{existing.get('ticker', 'unknown')}")

        cluster = f"singleton:{ticker}"
        return cluster
