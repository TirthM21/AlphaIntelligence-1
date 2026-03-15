"""Strategy method catalogue used for simulations, APIs, and dashboards."""

from __future__ import annotations

from typing import Any


def get_strategy_method_catalogue() -> dict[str, list[dict[str, Any]]]:
    """Return supported value-investing and algorithmic methods.

    The catalogue is intentionally static to keep downstream integrations
    deterministic for CI/workflows and front-end rendering.
    """

    return {
        "value_investing": [
            {
                "id": "intrinsic_discount",
                "name": "Intrinsic Discount Screen",
                "objective": "Find profitable companies trading below intrinsic value estimates.",
                "signals": ["free_cash_flow_yield", "roe", "debt_to_equity"],
            },
            {
                "id": "piotroski_quality_value",
                "name": "Piotroski Quality Value",
                "objective": "Prefer financially improving companies with strong balance-sheet quality.",
                "signals": ["f_score", "operating_cash_flow", "gross_margin_trend"],
            },
            {
                "id": "magic_formula_india",
                "name": "Magic Formula (India Adapted)",
                "objective": "Blend earnings yield with return on capital for robust value ranking.",
                "signals": ["earnings_yield", "return_on_capital", "sector_rank"],
            },
        ],
        "algorithmic": [
            {
                "id": "sepa_breakout",
                "name": "SEPA Momentum Breakout",
                "objective": "Capture phase-2 breakouts on relative strength and volume expansion.",
                "signals": ["price_breakout", "volume_spike", "relative_strength"],
            },
            {
                "id": "trend_following",
                "name": "Multi-Horizon Trend Following",
                "objective": "Stay with directional strength using moving-average and ADX filters.",
                "signals": ["ema_stack", "adx", "atr_trail"],
            },
            {
                "id": "mean_reversion",
                "name": "Volatility-Adjusted Mean Reversion",
                "objective": "Fade short-term extremes while controlling downside with volatility regimes.",
                "signals": ["z_score", "bollinger_deviation", "realized_volatility"],
            },
            {
                "id": "pairs_stat_arb",
                "name": "Pairs Statistical Arbitrage",
                "objective": "Exploit temporary spreads in co-integrated sector leaders.",
                "signals": ["hedge_ratio", "spread_zscore", "cointegration_pvalue"],
            },
            {
                "id": "event_driven_rotation",
                "name": "Event-Driven Sector Rotation",
                "objective": "Rebalance around earnings, tax deadlines, and macro catalysts.",
                "signals": ["event_risk_level", "sector_relative_momentum", "liquidity_shift"],
            },
            {
                "id": "cross_sectional_momentum",
                "name": "Cross-Sectional Momentum Ranking",
                "objective": "Rank winners versus laggards across sectors with volatility scaling.",
                "signals": ["relative_return_3m", "relative_return_6m", "volatility_adjustment"],
            },
            {
                "id": "quality_low_vol",
                "name": "Quality + Low Volatility Blend",
                "objective": "Tilt toward durable balance sheets while minimizing drawdown sensitivity.",
                "signals": ["earnings_stability", "beta", "downside_deviation"],
            },
        ],
    }


def list_strategy_tracks() -> list[str]:
    catalog = get_strategy_method_catalogue()
    return sorted(catalog.keys())
