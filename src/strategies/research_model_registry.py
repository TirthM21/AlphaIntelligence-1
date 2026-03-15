"""Registry of quant/ML research model families for competition signal stacking."""

from __future__ import annotations

from typing import Any


def get_research_model_registry() -> list[dict[str, Any]]:
    """Return model families and implementation status.

    Note: Only a subset is currently wired into production signal generation.
    Remaining entries are tracked to drive incremental implementation.
    """

    return [
        {"name": "SEPA Momentum Breakout", "category": "trend", "status": "implemented"},
        {"name": "Long-Term Quality Value", "category": "fundamental", "status": "implemented"},
        {"name": "Kalman Trend Filter", "category": "state_space", "status": "partial"},
        {"name": "Piotroski F-Score", "category": "fundamental", "status": "partial"},
        {"name": "XGBoost Cross-Sectional Ranker", "category": "ml", "status": "planned"},
        {"name": "LightGBM Return Classifier", "category": "ml", "status": "planned"},
        {"name": "LSTM Sequence Forecaster", "category": "deep_learning", "status": "planned"},
        {"name": "Temporal Fusion Transformer", "category": "deep_learning", "status": "planned"},
        {"name": "Graph Neural Sector Propagation", "category": "graph_ml", "status": "planned"},
        {"name": "Regime-Switching HMM", "category": "regime", "status": "planned"},
        {"name": "Bayesian Portfolio Allocator", "category": "bayesian", "status": "planned"},
        {"name": "Risk-Parity + CVaR Optimizer", "category": "portfolio", "status": "planned"},
    ]


def summarize_registry_status() -> dict[str, int]:
    summary = {"implemented": 0, "partial": 0, "planned": 0}
    for model in get_research_model_registry():
        status = model.get("status", "planned")
        summary[status] = summary.get(status, 0) + 1
    return summary
