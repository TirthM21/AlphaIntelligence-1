"""Strategy registry and factory helpers."""

from __future__ import annotations

from typing import Dict, Type

from .base import BaseStrategy
from .daily_momentum import DailyMomentumStrategy
from .long_term import LongTermStrategy

STRATEGY_REGISTRY: Dict[str, Type[BaseStrategy]] = {
    DailyMomentumStrategy.name: DailyMomentumStrategy,
    LongTermStrategy.name: LongTermStrategy,
}


def available_strategies() -> list[str]:
    return sorted(STRATEGY_REGISTRY.keys())


def create_strategy(name: str) -> BaseStrategy:
    try:
        strategy_cls = STRATEGY_REGISTRY[name]
    except KeyError as exc:
        raise ValueError(f"Unknown strategy '{name}'. Available: {', '.join(available_strategies())}") from exc
    return strategy_cls()
