"""Paper broker for deterministic simulation of execution outcomes."""

import random
from datetime import datetime, timedelta
from typing import Optional

from .order_book import FillEvent, OrderSide, OrderStatus, OrderType, SimulatedOrder


class PaperBroker:
    """Simulates order placement and fills with configurable slippage and latency."""

    def __init__(
        self,
        slippage_bps: float = 4.0,
        latency_ms: int = 300,
        latency_jitter_ms: int = 75,
        partial_fill_ratio: float = 1.0,
    ):
        self.slippage_bps = max(0.0, float(slippage_bps))
        self.latency_ms = max(0, int(latency_ms))
        self.latency_jitter_ms = max(0, int(latency_jitter_ms))
        self.partial_fill_ratio = min(1.0, max(0.1, float(partial_fill_ratio)))

    def submit_order(
        self,
        *,
        ticker: str,
        side: str,
        quantity: float,
        order_type: str,
        signal_price: float,
        market_price: float,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        submitted_at: Optional[datetime] = None,
    ) -> SimulatedOrder:
        """Submit and simulate immediate market-state based fills."""
        now = submitted_at or datetime.utcnow()
        order = SimulatedOrder(
            ticker=ticker,
            side=OrderSide(side),
            quantity=float(quantity),
            order_type=OrderType(order_type),
            signal_price=float(signal_price),
            submitted_at=now,
            limit_price=limit_price,
            stop_price=stop_price,
        )

        if self._is_triggered(order, market_price):
            self._fill_order(order, market_price)

        return order

    def _is_triggered(self, order: SimulatedOrder, market_price: float) -> bool:
        if order.order_type == OrderType.MARKET:
            return True

        if order.order_type == OrderType.LIMIT:
            if order.limit_price is None:
                return False
            if order.side == OrderSide.BUY:
                return market_price <= order.limit_price
            return market_price >= order.limit_price

        if order.stop_price is None:
            return False
        if order.side == OrderSide.BUY:
            return market_price >= order.stop_price
        return market_price <= order.stop_price

    def _fill_order(self, order: SimulatedOrder, market_price: float):
        fill_qty = order.quantity * self.partial_fill_ratio
        if fill_qty >= order.quantity:
            fill_qty = order.quantity
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.PARTIAL

        slippage_multiplier = 1 + (self.slippage_bps / 10000)
        if order.side == OrderSide.BUY:
            fill_price = market_price * slippage_multiplier
        else:
            fill_price = market_price / slippage_multiplier

        jitter = random.randint(-self.latency_jitter_ms, self.latency_jitter_ms)
        latency = max(0, self.latency_ms + jitter)
        fill_time = order.submitted_at + timedelta(milliseconds=latency)

        order.fills.append(
            FillEvent(
                quantity=fill_qty,
                price=fill_price,
                timestamp=fill_time,
                latency_ms=latency,
            )
        )
