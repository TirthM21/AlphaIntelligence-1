from datetime import datetime

import numpy as np
import pandas as pd

from src.backtesting.dashboard_data import (
    backtest_long_only,
    kalman_dynamic_hedge_backtest,
    scalar_kalman_filter,
    sepa_vcp_signal,
)


def _mock_prices(n=300):
    idx = pd.date_range(datetime(2023, 1, 1), periods=n, freq="B")
    trend = np.linspace(100, 180, n)
    noise = np.sin(np.arange(n) / 8)
    close = trend + noise
    df = pd.DataFrame(
        {
            "date": idx,
            "open": close * 0.99,
            "high": close * 1.01,
            "low": close * 0.98,
            "close": close,
            "volume": np.linspace(1_000_000, 2_000_000, n),
        }
    )
    return df


def test_scalar_kalman_filter_len():
    arr = np.array([1.0, 1.1, 1.2, 1.25])
    out = scalar_kalman_filter(arr)
    assert len(out) == len(arr)


def test_sepa_signal_and_long_only_backtest_runs():
    px = _mock_prices()
    signal = sepa_vcp_signal(px)
    equity, trades = backtest_long_only(px, signal)
    assert len(equity) == len(px)
    assert trades >= 0
    assert equity.iloc[-1] > 0


def test_kalman_pair_backtest_runs():
    px = _mock_prices()
    y = px["close"]
    x = px["close"] * 0.95
    equity, trades = kalman_dynamic_hedge_backtest(y, x)
    assert len(equity) == len(px)
    assert trades >= 0
    assert equity.iloc[-1] > 0
