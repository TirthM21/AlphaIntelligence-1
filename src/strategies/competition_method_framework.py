"""Competition method framework with 65-method library and regime-aware consensus."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class MethodDefinition:
    method_id: int
    name: str
    category: str
    status: str


# 65 methods as requested (implemented/partial/planned)
METHOD_LIBRARY: list[MethodDefinition] = [
    MethodDefinition(1, "Relative Strength Momentum", "momentum", "implemented"),
    MethodDefinition(2, "Time Series Momentum", "momentum", "implemented"),
    MethodDefinition(3, "Cross-Sectional Momentum", "momentum", "implemented"),
    MethodDefinition(4, "Moving Average Crossover", "momentum", "implemented"),
    MethodDefinition(5, "EMA Momentum", "momentum", "implemented"),
    MethodDefinition(6, "MACD Trend Strategy", "momentum", "implemented"),
    MethodDefinition(7, "Breakout Strategy", "momentum", "implemented"),
    MethodDefinition(8, "Donchian Channel", "momentum", "implemented"),
    MethodDefinition(9, "RSI Momentum", "momentum", "implemented"),
    MethodDefinition(10, "Volume Momentum", "momentum", "implemented"),
    MethodDefinition(11, "Relative Strength Rotation", "momentum", "partial"),
    MethodDefinition(12, "Trend Strength (ADX)", "momentum", "implemented"),
    MethodDefinition(13, "Z-Score Reversion", "mean_reversion", "implemented"),
    MethodDefinition(14, "Bollinger Band Reversion", "mean_reversion", "implemented"),
    MethodDefinition(15, "RSI Reversal", "mean_reversion", "implemented"),
    MethodDefinition(16, "VWAP Reversion", "mean_reversion", "implemented"),
    MethodDefinition(17, "Intraday Mean Reversion", "mean_reversion", "planned"),
    MethodDefinition(18, "Overnight Reversal", "mean_reversion", "planned"),
    MethodDefinition(19, "Volatility Reversion", "mean_reversion", "implemented"),
    MethodDefinition(20, "Sector Mean Reversion", "mean_reversion", "partial"),
    MethodDefinition(21, "Short-Term Reversal Factor", "mean_reversion", "implemented"),
    MethodDefinition(22, "Microstructure Reversion", "mean_reversion", "planned"),
    MethodDefinition(23, "Pairs Trading", "stat_arb", "partial"),
    MethodDefinition(24, "Cointegration Trading", "stat_arb", "planned"),
    MethodDefinition(25, "Triplet Trading", "stat_arb", "planned"),
    MethodDefinition(26, "Index Arbitrage", "stat_arb", "planned"),
    MethodDefinition(27, "ETF Arbitrage", "stat_arb", "planned"),
    MethodDefinition(28, "Lead-Lag Strategy", "stat_arb", "planned"),
    MethodDefinition(29, "Beta-Neutral Strategy", "stat_arb", "partial"),
    MethodDefinition(30, "Market Neutral Portfolio", "stat_arb", "partial"),
    MethodDefinition(31, "Statistical Factor Model", "stat_arb", "partial"),
    MethodDefinition(32, "Kalman Filter Pairs Trading", "stat_arb", "partial"),
    MethodDefinition(33, "Value Factor", "factor", "partial"),
    MethodDefinition(34, "Momentum Factor", "factor", "implemented"),
    MethodDefinition(35, "Size Factor", "factor", "partial"),
    MethodDefinition(36, "Quality Factor", "factor", "partial"),
    MethodDefinition(37, "Low Volatility", "factor", "implemented"),
    MethodDefinition(38, "Dividend Yield Factor", "factor", "partial"),
    MethodDefinition(39, "Earnings Revision", "factor", "planned"),
    MethodDefinition(40, "Profitability Factor", "factor", "partial"),
    MethodDefinition(41, "Asset Growth Factor", "factor", "planned"),
    MethodDefinition(42, "Composite Factor Model", "factor", "partial"),
    MethodDefinition(43, "Linear Regression Alpha", "ml", "implemented"),
    MethodDefinition(44, "LASSO Feature Selection", "ml", "planned"),
    MethodDefinition(45, "Random Forest Predictor", "ml", "planned"),
    MethodDefinition(46, "Gradient Boosting (XGBoost)", "ml", "planned"),
    MethodDefinition(47, "Support Vector Machine", "ml", "planned"),
    MethodDefinition(48, "Neural Networks", "ml", "planned"),
    MethodDefinition(49, "LSTM Time-Series Model", "ml", "planned"),
    MethodDefinition(50, "Transformer Momentum Model", "ml", "planned"),
    MethodDefinition(51, "Reinforcement Learning Trader", "ml", "planned"),
    MethodDefinition(52, "Clustering for Stock Selection", "ml", "partial"),
    MethodDefinition(53, "Autoencoder Feature Extraction", "ml", "planned"),
    MethodDefinition(54, "Ensemble Model", "ml", "implemented"),
    MethodDefinition(55, "Mean-Variance Optimization", "risk", "partial"),
    MethodDefinition(56, "Risk Parity", "risk", "partial"),
    MethodDefinition(57, "Kelly Criterion", "risk", "partial"),
    MethodDefinition(58, "Volatility Targeting", "risk", "implemented"),
    MethodDefinition(59, "Maximum Diversification", "risk", "planned"),
    MethodDefinition(60, "Drawdown Control", "risk", "implemented"),
    MethodDefinition(61, "Stop-Loss Optimization", "risk", "implemented"),
    MethodDefinition(62, "Position Sizing by Sharpe", "risk", "partial"),
    MethodDefinition(63, "Regime Detection", "risk", "implemented"),
    MethodDefinition(64, "Correlation Clustering", "risk", "partial"),
    MethodDefinition(65, "Portfolio Rebalancing Algorithm", "risk", "partial"),
]


def method_status_summary() -> dict[str, int]:
    summary = {"implemented": 0, "partial": 0, "planned": 0}
    for method in METHOD_LIBRARY:
        summary[method.status] = summary.get(method.status, 0) + 1
    return summary


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["High"], df["Low"], df["Close"]
    plus_dm = high.diff().clip(lower=0)
    minus_dm = (-low.diff()).clip(lower=0)
    tr = pd.concat([(high - low), (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().replace(0, np.nan)
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    return dx.rolling(period).mean()


def detect_regime(df: pd.DataFrame) -> str:
    close = df["Close"].dropna()
    if len(close) < 120:
        return "unknown"
    returns = close.pct_change().dropna()
    trend_63 = close.iloc[-1] / close.iloc[-63] - 1 if len(close) >= 63 else 0.0
    vol_21 = returns.tail(21).std()
    vol_126 = returns.tail(126).std() if len(returns) >= 126 else returns.std()

    if vol_126 and vol_21 > 1.8 * vol_126:
        return "volatility_shock"
    if trend_63 > 0.04:
        return "bull"
    if trend_63 < -0.04:
        return "bear"
    return "sideways"


def compute_method_votes(df: pd.DataFrame, benchmark_return_6m: float = 0.0) -> dict[int, int]:
    """Return votes per method id: +1 buy, -1 sell/avoid, 0 neutral."""
    data = df.copy().dropna(subset=["Close", "High", "Low", "Volume"])
    if len(data) < 220:
        return {}

    close = data["Close"]
    high = data["High"]
    low = data["Low"]
    volume = data["Volume"]

    ma50 = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()
    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_sig = macd.ewm(span=9, adjust=False).mean()
    rsi14 = _rsi(close, 14)
    adx14 = _adx(data, 14)

    ret_3m = close.iloc[-1] / close.iloc[-63] - 1
    ret_6m = close.iloc[-1] / close.iloc[-126] - 1
    ret_12m = close.iloc[-1] / close.iloc[-252] - 1
    ret_1d = close.iloc[-1] / close.iloc[-2] - 1

    std20 = close.rolling(20).std()
    z20 = (close - close.rolling(20).mean()) / std20.replace(0, np.nan)
    bb_upper = close.rolling(20).mean() + 2 * std20
    bb_lower = close.rolling(20).mean() - 2 * std20

    votes: dict[int, int] = {}
    votes[1] = 1 if ret_3m > 0.08 else -1 if ret_3m < -0.08 else 0
    votes[2] = 1 if ret_12m > 0 else -1
    votes[3] = 1 if ret_6m > benchmark_return_6m else -1
    votes[4] = 1 if ma50.iloc[-1] > ma200.iloc[-1] else -1
    votes[5] = 1 if ema20.iloc[-1] > ema50.iloc[-1] else -1
    votes[6] = 1 if macd.iloc[-1] > macd_sig.iloc[-1] else -1
    votes[7] = 1 if close.iloc[-1] > high.rolling(20).max().iloc[-2] else 0
    votes[8] = 1 if close.iloc[-1] > high.rolling(20).max().iloc[-2] else -1 if close.iloc[-1] < low.rolling(20).min().iloc[-2] else 0
    votes[9] = 1 if rsi14.iloc[-1] > 60 else -1 if rsi14.iloc[-1] < 40 else 0
    votes[10] = 1 if (close.iloc[-1] > close.iloc[-2] and volume.iloc[-1] > 2 * volume.tail(20).mean()) else 0
    votes[12] = 1 if adx14.iloc[-1] > 25 and ret_3m > 0 else -1 if adx14.iloc[-1] > 25 and ret_3m < 0 else 0
    votes[13] = 1 if z20.iloc[-1] < -2 else -1 if z20.iloc[-1] > 2 else 0
    votes[14] = 1 if close.iloc[-1] < bb_lower.iloc[-1] else -1 if close.iloc[-1] > bb_upper.iloc[-1] else 0
    votes[15] = 1 if rsi14.iloc[-1] < 30 else -1 if rsi14.iloc[-1] > 70 else 0

    typical_price = (high + low + close) / 3
    vwap20 = (typical_price * volume).rolling(20).sum() / volume.rolling(20).sum().replace(0, np.nan)
    deviation = (close.iloc[-1] / vwap20.iloc[-1] - 1) if pd.notna(vwap20.iloc[-1]) else 0
    votes[16] = 1 if deviation < -0.02 else -1 if deviation > 0.02 else 0

    vol21 = close.pct_change().rolling(21).std()
    vol126 = close.pct_change().rolling(126).std()
    votes[19] = 1 if vol21.iloc[-1] > 1.5 * vol126.iloc[-1] and ret_1d < 0 else 0
    votes[21] = 1 if ret_1d < -0.02 else -1 if ret_1d > 0.02 else 0

    # Factor / ML / Risk approximations
    votes[34] = votes[1]
    votes[37] = 1 if vol126.iloc[-1] < close.pct_change().std() else 0

    # Linear regression alpha: slope on log prices over 120 bars
    y = np.log(close.tail(120).values)
    x = np.arange(len(y))
    slope = np.polyfit(x, y, 1)[0]
    votes[43] = 1 if slope > 0 else -1

    # Ensemble model (majority of implemented directional models)
    directional_ids = [1, 2, 3, 4, 5, 6, 9, 12, 13, 14, 15, 16, 21, 34, 43]
    ensemble_score = sum(votes.get(i, 0) for i in directional_ids)
    votes[54] = 1 if ensemble_score >= 3 else -1 if ensemble_score <= -3 else 0

    # Vol targeting / drawdown control / stop optimization / regime detection
    votes[58] = 1 if vol21.iloc[-1] < 0.03 else -1
    drawdown_63 = close.iloc[-1] / close.tail(63).max() - 1
    votes[60] = -1 if drawdown_63 < -0.08 else 1
    atr_proxy = (high - low).rolling(14).mean().iloc[-1]
    votes[61] = 1 if atr_proxy / close.iloc[-1] < 0.04 else -1
    regime = detect_regime(data)
    votes[63] = 1 if regime == "bull" else -1 if regime in {"bear", "volatility_shock"} else 0

    return votes


def regime_weighted_score(votes: dict[int, int], regime: str) -> float:
    if not votes:
        return 0.0
    momentum_ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 34}
    reversion_ids = {13, 14, 15, 16, 19, 21}
    risk_ids = {58, 60, 61, 63}

    score = 0.0
    for method_id, vote in votes.items():
        if method_id in momentum_ids:
            weight = 1.2 if regime == "bull" else 0.8
        elif method_id in reversion_ids:
            weight = 1.2 if regime in {"bear", "volatility_shock"} else 0.9
        elif method_id in risk_ids:
            weight = 1.1
        else:
            weight = 1.0
        score += vote * weight
    return round(score, 4)


def library_payload() -> dict[str, Any]:
    return {
        "summary": method_status_summary(),
        "methods": [
            {"method_id": m.method_id, "name": m.name, "category": m.category, "status": m.status}
            for m in METHOD_LIBRARY
        ],
    }
