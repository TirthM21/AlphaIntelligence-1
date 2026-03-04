"""Persistence helpers for experiment runs and metrics."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, Optional

import pandas as pd


def persist_experiment_run(
    symbol: str,
    results: Dict[str, Any],
    base_dir: str = "experiments",
    db_path: Optional[str] = None,
) -> Dict[str, str]:
    """Persist config snapshots, metadata, and metric outputs for one run."""
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(base_dir) / run_id
    config_dir = run_dir / "config_snapshots"
    metadata_dir = run_dir / "metadata"
    metrics_dir = run_dir / "metrics"

    for path in (config_dir, metadata_dir, metrics_dir):
        path.mkdir(parents=True, exist_ok=True)

    params_path = config_dir / "parameter_sets.json"
    params_path.write_text(json.dumps(results["parameter_sets"], indent=2), encoding="utf-8")

    metadata_payload = {
        "run_id": run_id,
        "created_at_utc": datetime.utcnow().isoformat(),
        "symbol": symbol,
        "walk_forward_config": results["config"],
        "window_count": int(len(results["window_metrics"])),
    }
    metadata_path = metadata_dir / "run_metadata.json"
    metadata_path.write_text(json.dumps(metadata_payload, indent=2), encoding="utf-8")

    aggregate_df: pd.DataFrame = results["aggregate_metrics"].copy()
    window_df: pd.DataFrame = results["window_metrics"].copy()

    aggregate_json = metrics_dir / "aggregate_metrics.json"
    aggregate_csv = metrics_dir / "aggregate_metrics.csv"
    window_json = metrics_dir / "window_metrics.json"
    window_csv = metrics_dir / "window_metrics.csv"

    aggregate_df.to_json(aggregate_json, orient="records", indent=2)
    aggregate_df.to_csv(aggregate_csv, index=False)
    window_df.to_json(window_json, orient="records", indent=2)
    window_df.to_csv(window_csv, index=False)

    if db_path:
        _persist_to_db(db_path=db_path, run_id=run_id, symbol=symbol, aggregate_df=aggregate_df)

    return {
        "run_dir": str(run_dir),
        "parameter_sets": str(params_path),
        "metadata": str(metadata_path),
        "aggregate_json": str(aggregate_json),
        "aggregate_csv": str(aggregate_csv),
        "window_json": str(window_json),
        "window_csv": str(window_csv),
    }


def _persist_to_db(db_path: str, run_id: str, symbol: str, aggregate_df: pd.DataFrame) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS experiment_run_metrics (
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                parameter_id TEXT NOT NULL,
                sharpe REAL,
                max_drawdown REAL,
                hit_rate REAL,
                turnover REAL,
                total_return REAL,
                created_at_utc TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, symbol, parameter_id)
            )
            """
        )
        for row in aggregate_df.to_dict(orient="records"):
            connection.execute(
                """
                INSERT OR REPLACE INTO experiment_run_metrics
                (run_id, symbol, parameter_id, sharpe, max_drawdown, hit_rate, turnover, total_return)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    symbol,
                    str(row.get("parameter_id")),
                    float(row.get("sharpe", 0.0)),
                    float(row.get("max_drawdown", 0.0)),
                    float(row.get("hit_rate", 0.0)),
                    float(row.get("turnover", 0.0)),
                    float(row.get("total_return", 0.0)),
                ),
            )
        connection.commit()
    finally:
        connection.close()
