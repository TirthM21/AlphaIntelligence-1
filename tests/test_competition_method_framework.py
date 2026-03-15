import numpy as np
import pandas as pd

from src.strategies.competition_method_framework import (
    METHOD_LIBRARY,
    compute_method_votes,
    detect_regime,
    library_payload,
    method_status_summary,
    regime_weighted_score,
)


def _make_trending_df(n: int = 320, drift: float = 0.001) -> pd.DataFrame:
    np.random.seed(7)
    returns = np.random.normal(loc=drift, scale=0.01, size=n)
    close = 100 * np.cumprod(1 + returns)
    high = close * (1 + np.random.uniform(0.001, 0.02, size=n))
    low = close * (1 - np.random.uniform(0.001, 0.02, size=n))
    open_ = (high + low) / 2
    volume = np.random.randint(100000, 200000, size=n)
    return pd.DataFrame({"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume})


def test_method_library_has_65_methods():
    assert len(METHOD_LIBRARY) == 65
    assert METHOD_LIBRARY[0].method_id == 1
    assert METHOD_LIBRARY[-1].method_id == 65


def test_status_summary_and_payload_shape():
    summary = method_status_summary()
    payload = library_payload()

    assert sum(summary.values()) == 65
    assert payload["summary"] == summary
    assert len(payload["methods"]) == 65


def test_detect_regime_and_votes_generate_signal_dict():
    df = _make_trending_df(drift=0.0015)
    regime = detect_regime(df)
    votes = compute_method_votes(df, benchmark_return_6m=0.03)

    assert regime in {"bull", "bear", "sideways", "volatility_shock", "unknown"}
    assert isinstance(votes, dict)
    assert 1 in votes and 63 in votes


def test_regime_weighted_score_returns_float():
    score = regime_weighted_score({1: 1, 13: -1, 58: 1, 63: 1}, regime="bull")
    assert isinstance(score, float)
