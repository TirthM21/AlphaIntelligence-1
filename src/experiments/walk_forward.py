"""Walk-forward backtesting over rolling windows for parameter experiments."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class WindowResult:
    """Per-window metric output for one parameter set."""

    parameter_id: str
    window_index: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    sharpe: float
    max_drawdown: float
    hit_rate: float
    turnover: float
    total_return: float


def _rolling_windows(total_len: int, train_window: int, test_window: int, step: int) -> Iterable[Tuple[int, int, int, int]]:
    cursor = 0
    while cursor + train_window + test_window <= total_len:
        train_start = cursor
        train_end = cursor + train_window
        test_end = train_end + test_window
        yield train_start, train_end, train_end, test_end
        cursor += step


def _strategy_signal(prices: pd.DataFrame, params: Dict[str, Any]) -> pd.Series:
    close = prices["close"]
    volume = prices["volume"]

    short_ma = int(params.get("short_ma", 50))
    long_ma = int(params.get("long_ma", 200))
    breakout_window = int(params.get("breakout_window", 20))
    vol_short = int(params.get("vol_short", 20))
    vol_long = int(params.get("vol_long", 60))
    volume_multiplier = float(params.get("volume_multiplier", 1.2))

    trend_ok = close.rolling(short_ma).mean() > close.rolling(long_ma).mean()
    breakout = close >= close.rolling(breakout_window).max().shift(1)
    ret = close.pct_change()
    contraction = ret.rolling(vol_short).std() < ret.rolling(vol_long).std()
    volume_ok = volume > volume_multiplier * volume.rolling(vol_short).mean()
    return (trend_ok & breakout & contraction & volume_ok).fillna(False)


def _calc_metrics(close: pd.Series, signal: pd.Series) -> Dict[str, float]:
    daily_ret = close.pct_change().fillna(0)
    position = signal.shift(1).astype("boolean").fillna(False).astype(float)
    strat_ret = daily_ret * position
    equity = (1 + strat_ret).cumprod()

    ret_std = strat_ret.std()
    sharpe = float(np.sqrt(252) * strat_ret.mean() / ret_std) if ret_std and not np.isnan(ret_std) else 0.0
    drawdown = equity / equity.cummax() - 1
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0

    invested_mask = position > 0
    invested_days = int(invested_mask.sum())
    hit_rate = float((strat_ret[invested_mask] > 0).mean()) if invested_days else 0.0
    turnover = float(position.diff().abs().sum())
    total_return = float(equity.iloc[-1] - 1) if not equity.empty else 0.0

    return {
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "hit_rate": hit_rate,
        "turnover": turnover,
        "total_return": total_return,
        "mean_daily_return": float(strat_ret.mean()),
        "std_daily_return": float(strat_ret.std(ddof=1)) if len(strat_ret) > 1 else 0.0,
        "days": int(len(strat_ret)),
    }


def run_walk_forward_backtest(
    prices: pd.DataFrame,
    parameter_sets: List[Dict[str, Any]],
    train_window: int = 252,
    test_window: int = 63,
    step: int = 63,
) -> Dict[str, Any]:
    """Evaluate multiple parameter sets using rolling train/test windows."""
    if prices.empty:
        raise ValueError("prices dataframe is empty")
    required_cols = {"date", "close", "volume"}
    missing = required_cols - set(prices.columns)
    if missing:
        raise ValueError(f"prices dataframe missing columns: {sorted(missing)}")

    ordered = prices.sort_values("date").reset_index(drop=True)
    windows = list(_rolling_windows(len(ordered), train_window, test_window, step))
    if not windows:
        raise ValueError("not enough rows for requested walk-forward windows")

    window_rows: List[WindowResult] = []
    aggregate_rows: List[Dict[str, Any]] = []

    for i, params in enumerate(parameter_sets):
        pid = str(params.get("id") or f"set_{i + 1}")
        run_metrics: List[Dict[str, float]] = []

        for window_idx, (tr_s, tr_e, te_s, te_e) in enumerate(windows, start=1):
            window_frame = ordered.iloc[tr_s:te_e].copy()
            signal = _strategy_signal(window_frame, params)
            test_close = window_frame["close"].iloc[te_s - tr_s : te_e - tr_s]
            test_signal = signal.iloc[te_s - tr_s : te_e - tr_s]
            metrics = _calc_metrics(test_close, test_signal)
            run_metrics.append(metrics)

            train_start = ordered.iloc[tr_s]["date"]
            train_end = ordered.iloc[tr_e - 1]["date"]
            test_start = ordered.iloc[te_s]["date"]
            test_end = ordered.iloc[te_e - 1]["date"]

            window_rows.append(
                WindowResult(
                    parameter_id=pid,
                    window_index=window_idx,
                    train_start=str(pd.Timestamp(train_start).date()),
                    train_end=str(pd.Timestamp(train_end).date()),
                    test_start=str(pd.Timestamp(test_start).date()),
                    test_end=str(pd.Timestamp(test_end).date()),
                    sharpe=metrics["sharpe"],
                    max_drawdown=metrics["max_drawdown"],
                    hit_rate=metrics["hit_rate"],
                    turnover=metrics["turnover"],
                    total_return=metrics["total_return"],
                )
            )

        metric_frame = pd.DataFrame(run_metrics)
        avg = metric_frame.mean(numeric_only=True)
        aggregate_rows.append(
            {
                "parameter_id": pid,
                "window_count": int(len(run_metrics)),
                "sharpe": float(avg.get("sharpe", 0.0)),
                "max_drawdown": float(avg.get("max_drawdown", 0.0)),
                "hit_rate": float(avg.get("hit_rate", 0.0)),
                "turnover": float(avg.get("turnover", 0.0)),
                "total_return": float(avg.get("total_return", 0.0)),
                "sharpe_std": float(metric_frame["sharpe"].std(ddof=1) or 0.0),
                "params": params,
            }
        )

    aggregate_df = pd.DataFrame(aggregate_rows).sort_values("sharpe", ascending=False)
    window_df = pd.DataFrame([row.__dict__ for row in window_rows])

    return {
        "aggregate_metrics": aggregate_df,
        "window_metrics": window_df,
        "parameter_sets": parameter_sets,
        "config": {
            "train_window": train_window,
            "test_window": test_window,
            "step": step,
            "rows": len(ordered),
        },
    }
