"""Grid-driven walk-forward experiment runner for strategy research."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import product
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml

from src.backtesting.dashboard_data import backtest_long_only, fetch_price_history


DEFAULT_OUTPUT_DIR = Path("data/experiments")


@dataclass
class WalkForwardWindow:
    """Index boundaries for one walk-forward window."""

    window_index: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int


def _rolling_windows(total_len: int, train_window: int, test_window: int, step: int) -> List[WalkForwardWindow]:
    windows: List[WalkForwardWindow] = []
    cursor = 0
    idx = 1
    while cursor + train_window + test_window <= total_len:
        train_start = cursor
        train_end = cursor + train_window
        test_start = train_end
        test_end = test_start + test_window
        windows.append(
            WalkForwardWindow(
                window_index=idx,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
        cursor += step
        idx += 1
    return windows


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    return [value]


def build_parameter_sets(parameter_grid: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Create cartesian parameter combinations from a YAML-style grid."""
    if not parameter_grid:
        raise ValueError("parameter_grid is empty")

    keys = sorted(parameter_grid.keys())
    values = [_as_list(parameter_grid[key]) for key in keys]
    combinations = list(product(*values))

    parameter_sets: List[Dict[str, Any]] = []
    for idx, combo in enumerate(combinations, start=1):
        params = {key: combo[i] for i, key in enumerate(keys)}
        params["id"] = f"grid_{idx:03d}"
        parameter_sets.append(params)

    return parameter_sets


def load_experiment_config(config_path: str) -> Dict[str, Any]:
    """Load YAML config that includes data scope, windows, and parameter grids."""
    payload = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("Experiment config YAML must be a mapping")

    if "parameter_grid" not in payload:
        raise ValueError("Experiment config missing 'parameter_grid'")

    payload.setdefault("symbol", "RELIANCE")
    payload.setdefault("years", 5)
    payload.setdefault("train_window", 252)
    payload.setdefault("test_window", 63)
    payload.setdefault("step", 63)

    return payload


def _strategy_signal(prices: pd.DataFrame, params: Dict[str, Any]) -> pd.Series:
    close = prices["close"]
    volume = prices["volume"]

    buy_threshold = float(params.get("buy_threshold", 1.0))
    volume_threshold = float(params.get("volume_threshold", 1.2))
    strictness = float(params.get("minervini_strictness", 1.0))

    sma50 = close.rolling(50).mean()
    sma150 = close.rolling(150).mean()
    sma200 = close.rolling(200).mean()
    high_52w = close.rolling(252).max()
    low_52w = close.rolling(252).min()

    conditions = pd.DataFrame(
        {
            "close_gt_50": close > sma50,
            "50_gt_150": sma50 > sma150,
            "150_gt_200": sma150 > sma200,
            "high_proximity": close >= 0.75 * high_52w,
            "low_extension": close >= 1.3 * low_52w,
        }
    ).fillna(False)

    required = min(conditions.shape[1], max(1, math.ceil(strictness * conditions.shape[1])))
    template_ok = conditions.sum(axis=1) >= required

    breakout = close >= buy_threshold * close.rolling(20).max().shift(1)
    volume_ok = volume > volume_threshold * volume.rolling(20).mean()
    ret = close.pct_change()
    contraction = ret.rolling(20).std() < ret.rolling(60).std()

    return (template_ok & breakout & volume_ok & contraction).fillna(False)


def _window_metrics(close: pd.Series, signal: pd.Series) -> Dict[str, float]:
    daily_ret = close.pct_change().fillna(0)
    position = signal.shift(1).astype("boolean").fillna(False).astype(float)
    strat_ret = daily_ret * position
    equity = (1 + strat_ret).cumprod()

    rets_std = strat_ret.std()
    sharpe = float(np.sqrt(252) * strat_ret.mean() / rets_std) if rets_std and not np.isnan(rets_std) else 0.0
    drawdown = equity / equity.cummax() - 1

    return {
        "return": float(equity.iloc[-1] - 1) if not equity.empty else 0.0,
        "max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "sharpe": sharpe,
    }


def run_grid_walk_forward(prices: pd.DataFrame, parameter_sets: List[Dict[str, Any]], train_window: int, test_window: int, step: int) -> Dict[str, pd.DataFrame]:
    """Evaluate each parameter set over walk-forward windows."""
    ordered = prices.sort_values("date").reset_index(drop=True)
    windows = _rolling_windows(len(ordered), train_window=train_window, test_window=test_window, step=step)
    if not windows:
        raise ValueError("Not enough price rows for requested walk-forward setup")

    window_rows: List[Dict[str, Any]] = []
    aggregate_rows: List[Dict[str, Any]] = []

    for params in parameter_sets:
        pid = str(params["id"])
        per_window = []
        for window in windows:
            segment = ordered.iloc[window.train_start : window.test_end].copy()
            signal = _strategy_signal(segment, params)
            test_slice = segment.iloc[window.test_start - window.train_start : window.test_end - window.train_start]
            test_signal = signal.iloc[window.test_start - window.train_start : window.test_end - window.train_start]

            equity, trades = backtest_long_only(test_slice, test_signal)
            metrics = _window_metrics(test_slice["close"], test_signal)
            metrics["trades"] = float(trades)
            per_window.append(metrics)

            window_rows.append(
                {
                    "parameter_id": pid,
                    "window_index": window.window_index,
                    "train_start": str(pd.Timestamp(ordered.iloc[window.train_start]["date"]).date()),
                    "train_end": str(pd.Timestamp(ordered.iloc[window.train_end - 1]["date"]).date()),
                    "test_start": str(pd.Timestamp(ordered.iloc[window.test_start]["date"]).date()),
                    "test_end": str(pd.Timestamp(ordered.iloc[window.test_end - 1]["date"]).date()),
                    "total_return": metrics["return"],
                    "max_drawdown": metrics["max_drawdown"],
                    "sharpe": metrics["sharpe"],
                    "trades": trades,
                    "equity_end": float(equity.iloc[-1]) if not equity.empty else 1.0,
                }
            )

        frame = pd.DataFrame(per_window)
        aggregate_rows.append(
            {
                "parameter_id": pid,
                "total_return": float(frame["return"].mean()),
                "max_drawdown": float(frame["max_drawdown"].mean()),
                "sharpe": float(frame["sharpe"].mean()),
                "stability": float(1.0 / (1.0 + frame["return"].std(ddof=1))) if len(frame) > 1 else 1.0,
                "return_std": float(frame["return"].std(ddof=1) if len(frame) > 1 else 0.0),
                "window_count": int(len(frame)),
                "params": params,
            }
        )

    aggregate_df = pd.DataFrame(aggregate_rows)
    window_df = pd.DataFrame(window_rows)

    ranked = summarize_experiments(aggregate_df)
    return {"aggregate": ranked, "windows": window_df}


def summarize_experiments(aggregate_df: pd.DataFrame) -> pd.DataFrame:
    """Rank experiments using return, drawdown quality, and stability."""
    if aggregate_df.empty:
        return aggregate_df

    ranked = aggregate_df.copy()
    ret_rank = ranked["total_return"].rank(pct=True)
    dd_quality = ranked["max_drawdown"].rank(pct=True)  # less negative drawdowns rank higher
    stability_rank = ranked["stability"].rank(pct=True)

    ranked["composite_score"] = 0.45 * ret_rank + 0.30 * dd_quality + 0.25 * stability_rank
    ranked = ranked.sort_values("composite_score", ascending=False).reset_index(drop=True)
    return ranked


def persist_experiment_outputs(
    config: Dict[str, Any],
    aggregate_df: pd.DataFrame,
    window_df: pd.DataFrame,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> Dict[str, str]:
    """Persist run config, metrics, and artifact references under data/experiments."""
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_root = output_dir / run_id
    config_dir = run_root / "config"
    metrics_dir = run_root / "metrics"

    config_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    config_path = config_dir / "experiment_config.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    parameter_path = config_dir / "parameter_sets.json"
    parameter_sets = build_parameter_sets(config["parameter_grid"])
    parameter_path.write_text(json.dumps(parameter_sets, indent=2), encoding="utf-8")

    aggregate_csv = metrics_dir / "aggregate_metrics.csv"
    aggregate_json = metrics_dir / "aggregate_metrics.json"
    windows_csv = metrics_dir / "window_metrics.csv"
    windows_json = metrics_dir / "window_metrics.json"
    summary_md = metrics_dir / "summary_report.md"

    aggregate_df.to_csv(aggregate_csv, index=False)
    aggregate_df.to_json(aggregate_json, orient="records", indent=2)
    window_df.to_csv(windows_csv, index=False)
    window_df.to_json(windows_json, orient="records", indent=2)

    summary_md.write_text(_build_summary_markdown(aggregate_df), encoding="utf-8")

    artifact_manifest = run_root / "artifacts.json"
    artifact_manifest.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "created_at_utc": datetime.utcnow().isoformat(),
                "artifacts": {
                    "config": str(config_path),
                    "parameter_sets": str(parameter_path),
                    "aggregate_csv": str(aggregate_csv),
                    "aggregate_json": str(aggregate_json),
                    "window_csv": str(windows_csv),
                    "window_json": str(windows_json),
                    "summary_report": str(summary_md),
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "run_dir": str(run_root),
        "config": str(config_path),
        "parameter_sets": str(parameter_path),
        "aggregate_csv": str(aggregate_csv),
        "aggregate_json": str(aggregate_json),
        "window_csv": str(windows_csv),
        "window_json": str(windows_json),
        "summary_report": str(summary_md),
        "artifact_manifest": str(artifact_manifest),
    }


def _build_summary_markdown(aggregate_df: pd.DataFrame) -> str:
    view = aggregate_df[
        [
            "parameter_id",
            "composite_score",
            "total_return",
            "max_drawdown",
            "stability",
            "sharpe",
            "window_count",
        ]
    ].copy()

    lines = [
        "# Experiment Summary Report",
        "",
        "Ranked by a composite score of return (45%), drawdown quality (30%), and stability (25%).",
        "",
        "```",
        view.to_string(index=False),
        "```",
    ]
    return "\n".join(lines)


def run_from_config(config_path: str, output_dir: str = str(DEFAULT_OUTPUT_DIR)) -> Dict[str, str]:
    """High-level runner: load YAML config, execute grid walk-forward, persist outputs."""
    config = load_experiment_config(config_path)
    parameter_sets = build_parameter_sets(config["parameter_grid"])

    to_date = date.today()
    from_date = to_date - timedelta(days=365 * int(config["years"]))
    prices = fetch_price_history(config["symbol"], from_date, to_date)

    result = run_grid_walk_forward(
        prices=prices,
        parameter_sets=parameter_sets,
        train_window=int(config["train_window"]),
        test_window=int(config["test_window"]),
        step=int(config["step"]),
    )

    return persist_experiment_outputs(
        config=config,
        aggregate_df=result["aggregate"],
        window_df=result["windows"],
        output_dir=Path(output_dir),
    )
