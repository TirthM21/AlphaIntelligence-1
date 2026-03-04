from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

from src.experiments.reporting import generate_experiment_comparison_report
from src.experiments.storage import persist_experiment_run
from src.experiments.walk_forward import run_walk_forward_backtest


def _prices(n: int = 420) -> pd.DataFrame:
    dates = pd.date_range(datetime(2022, 1, 1), periods=n, freq="B")
    trend = np.linspace(100, 160, n)
    wave = 2.0 * np.sin(np.arange(n) / 8)
    close = trend + wave
    return pd.DataFrame(
        {
            "date": dates,
            "close": close,
            "volume": np.linspace(1_000_000, 2_000_000, n),
        }
    )


def test_walk_forward_runs_for_multiple_parameter_sets():
    params = [
        {"id": "a", "short_ma": 30, "long_ma": 120, "breakout_window": 10},
        {"id": "b", "short_ma": 50, "long_ma": 200, "breakout_window": 20},
    ]
    result = run_walk_forward_backtest(_prices(), params, train_window=200, test_window=50, step=50)
    assert not result["aggregate_metrics"].empty
    assert set(result["aggregate_metrics"]["parameter_id"]) == {"a", "b"}
    assert {"sharpe", "max_drawdown", "hit_rate", "turnover"}.issubset(result["aggregate_metrics"].columns)


def test_persist_outputs_and_report(tmp_path):
    params = [{"id": "baseline"}, {"id": "candidate"}]
    result = run_walk_forward_backtest(_prices(), params, train_window=200, test_window=50, step=50)
    paths = persist_experiment_run("TEST", result, base_dir=str(tmp_path), db_path=str(tmp_path / "metrics.db"))

    assert Path(paths["run_dir"]).exists()
    assert pd.read_csv(paths["aggregate_csv"]).shape[0] == 2

    report = generate_experiment_comparison_report(
        result["aggregate_metrics"],
        baseline_parameter_id="baseline",
        output_path=str(tmp_path / "report.md"),
    )
    assert report["report_path"]
    assert "Experiment Comparison Report" in report["report_markdown"]
