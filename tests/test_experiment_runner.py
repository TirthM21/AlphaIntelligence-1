from datetime import datetime

import numpy as np
import pandas as pd

from src.research.experiment_runner import build_parameter_sets, run_grid_walk_forward, summarize_experiments


def _prices(n: int = 430) -> pd.DataFrame:
    dates = pd.date_range(datetime(2021, 1, 1), periods=n, freq="B")
    close = np.linspace(100, 170, n) + np.sin(np.arange(n) / 6)
    volume = np.linspace(1_000_000, 1_500_000, n)
    return pd.DataFrame({"date": dates, "close": close, "volume": volume})


def test_build_parameter_sets_creates_cartesian_grid():
    grid = {"buy_threshold": [0.99, 1.0], "volume_threshold": [1.1, 1.2], "minervini_strictness": [0.7]}
    params = build_parameter_sets(grid)
    assert len(params) == 4
    assert all("id" in p for p in params)


def test_run_grid_walk_forward_returns_ranked_summary():
    params = build_parameter_sets(
        {
            "buy_threshold": [0.99, 1.0],
            "volume_threshold": [1.1],
            "minervini_strictness": [0.7, 1.0],
        }
    )
    result = run_grid_walk_forward(_prices(), params, train_window=220, test_window=50, step=50)
    assert not result["aggregate"].empty
    assert "composite_score" in result["aggregate"].columns
    assert not result["windows"].empty


def test_summarize_experiments_uses_non_return_fields():
    aggregate = pd.DataFrame(
        [
            {"parameter_id": "a", "total_return": 0.2, "max_drawdown": -0.4, "stability": 0.2, "sharpe": 1.0, "window_count": 4},
            {"parameter_id": "b", "total_return": 0.18, "max_drawdown": -0.1, "stability": 0.9, "sharpe": 0.8, "window_count": 4},
        ]
    )
    ranked = summarize_experiments(aggregate)
    assert ranked.iloc[0]["parameter_id"] == "b"
