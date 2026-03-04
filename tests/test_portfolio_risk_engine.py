import pandas as pd

from src.risk.portfolio_risk_engine import PortfolioRiskEngine, RiskConfig


def _price_frame(seed: float, drift: float = 0.001):
    closes = [seed]
    for i in range(1, 80):
        closes.append(closes[-1] * (1 + drift + (0.002 if i % 2 == 0 else -0.001)))
    series = pd.Series(closes)
    return pd.DataFrame({
        "Close": series,
        "High": series * 1.01,
        "Low": series * 0.99,
    })


def test_risk_engine_applies_sector_cap_and_persists_decisions(tmp_path):
    engine = PortfolioRiskEngine(
        RiskConfig(max_single_name_weight=0.30, sector_cap=0.35, theme_cap=1.0, cash_floor=0.0)
    )

    candidates = [
        {"ticker": "AAA", "score": 95, "sector": "Tech", "theme": "AI", "beta": 1.0},
        {"ticker": "BBB", "score": 90, "sector": "Tech", "theme": "AI", "beta": 1.0},
        {"ticker": "CCC", "score": 85, "sector": "Banks", "theme": "Fin", "beta": 1.0},
    ]
    analyses = {
        "AAA": {"price_data": _price_frame(100)},
        "BBB": {"price_data": _price_frame(120)},
        "CCC": {"price_data": _price_frame(140)},
    }

    accepted, decisions = engine.apply(candidates, analyses, max_positions=3)

    assert [s["ticker"] for s in accepted] == ["AAA", "CCC"]
    rejected = [d for d in decisions if not d["accepted"]]
    assert any("sector_cap" in d["reason"] for d in rejected)

    out = engine.persist_decisions(decisions, output_dir=str(tmp_path))
    assert out is not None


def test_risk_engine_clusters_highly_correlated_names():
    engine = PortfolioRiskEngine(
        RiskConfig(
            max_single_name_weight=0.5,
            sector_cap=1.0,
            theme_cap=1.0,
            cash_floor=0.0,
            max_per_cluster=1,
            correlation_threshold=0.7,
        )
    )

    frame = _price_frame(100)
    candidates = [
        {"ticker": "AAA", "score": 90, "beta": 1.0},
        {"ticker": "BBB", "score": 89, "beta": 1.0},
    ]
    analyses = {
        "AAA": {"price_data": frame},
        "BBB": {"price_data": frame.copy()},
    }

    accepted, decisions = engine.apply(candidates, analyses, max_positions=2)
    assert len(accepted) == 1
    assert any("correlation_cluster" in d["reason"] for d in decisions if not d["accepted"])
