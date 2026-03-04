"""Tests for strategy registry and normalized payload schema."""

from types import SimpleNamespace

from src.strategies.registry import STRATEGY_REGISTRY, create_strategy


REQUIRED_SIGNAL_KEYS = {"ticker", "side", "score", "reason", "details", "raw"}


def _assert_normalized_payload(payload):
    assert {"strategy", "generated_at", "metadata", "risk_rules", "signals"}.issubset(payload.keys())
    assert {"buy", "sell"}.issubset(payload["signals"].keys())

    for side in ("buy", "sell"):
        for signal in payload["signals"][side]:
            assert REQUIRED_SIGNAL_KEYS.issubset(signal.keys())
            assert signal["side"] == side
            assert isinstance(signal["score"], float)


def test_each_registered_strategy_can_be_instantiated():
    for name in STRATEGY_REGISTRY:
        strategy = create_strategy(name)
        assert strategy.metadata()["name"] == name


def test_daily_momentum_strategy_normalized_payload(monkeypatch):
    from src.strategies import daily_momentum as daily_module

    monkeypatch.setattr(
        daily_module,
        "score_buy_signal",
        lambda **_: {"is_buy": True, "score": 82, "reason": "buy", "details": {"x": 1}},
    )
    monkeypatch.setattr(
        daily_module,
        "score_sell_signal",
        lambda **_: {"is_sell": True, "score": 63, "reason": "sell", "details": {"y": 2}},
    )

    strategy = create_strategy("daily_momentum")
    payload = strategy.generate_signals(
        {
            "analyses": [
                {
                    "ticker": "ABC.NS",
                    "price_data": object(),
                    "current_price": 100.0,
                    "phase_info": {"phase": 2},
                    "rs_series": object(),
                },
                {
                    "ticker": "XYZ.NS",
                    "price_data": object(),
                    "current_price": 90.0,
                    "phase_info": {"phase": 3},
                    "rs_series": object(),
                },
            ]
        }
    )

    _assert_normalized_payload(payload)
    assert len(payload["signals"]["buy"]) == 1
    assert len(payload["signals"]["sell"]) == 1


def test_long_term_strategy_normalized_payload(monkeypatch):
    from src.strategies import long_term as long_term_module

    fake_portfolio = SimpleNamespace(
        allocations={"RELIANCE.NS": 0.12, "TCS.NS": 0.08},
        total_score=77.5,
    )

    monkeypatch.setattr(
        long_term_module.PortfolioConstructor,
        "build_portfolio",
        lambda self, stocks, etfs, sector_map, theme_map: fake_portfolio,
    )

    strategy = create_strategy("long_term")
    payload = strategy.generate_signals(
        {
            "stocks": [{"ticker": "RELIANCE.NS", "score": 81}],
            "etfs": [{"ticker": "NIFTYBEES.NS", "score": 70}],
            "sector_map": {"RELIANCE.NS": "Energy"},
            "theme_map": {"NIFTYBEES.NS": "Index"},
        }
    )

    _assert_normalized_payload(payload)
    assert len(payload["signals"]["buy"]) == 2
    assert payload["signals"]["sell"] == []
