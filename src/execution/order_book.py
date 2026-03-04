"""Execution order book primitives for paper trading simulations."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional


class OrderType(str, Enum):
    """Supported simulated order types."""

    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"


class OrderSide(str, Enum):
    """Order direction."""

    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    """Lifecycle states for simulated orders."""

    NEW = "NEW"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"


@dataclass
class FillEvent:
    """Single fill event for a simulated order."""

    quantity: float
    price: float
    timestamp: datetime
    latency_ms: int


@dataclass
class SimulatedOrder:
    """In-memory representation of simulated order state and fills."""

    ticker: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    signal_price: float
    submitted_at: datetime
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    status: OrderStatus = OrderStatus.NEW
    fills: List[FillEvent] = field(default_factory=list)

    @property
    def filled_quantity(self) -> float:
        return sum(fill.quantity for fill in self.fills)

    @property
    def fill_ratio(self) -> float:
        if self.quantity <= 0:
            return 0.0
        return self.filled_quantity / self.quantity

    @property
    def average_fill_price(self) -> Optional[float]:
        total_qty = self.filled_quantity
        if total_qty <= 0:
            return None
        notional = sum(fill.quantity * fill.price for fill in self.fills)
        return notional / total_qty

    @property
    def time_to_fill_ms(self) -> Optional[int]:
        if not self.fills:
            return None
        last_fill = max(self.fills, key=lambda f: f.timestamp)
        return int((last_fill.timestamp - self.submitted_at).total_seconds() * 1000)

    @property
    def slippage_bps(self) -> Optional[float]:
        avg_fill = self.average_fill_price
        if avg_fill is None or self.signal_price <= 0:
            return None

        if self.side == OrderSide.BUY:
            return ((avg_fill - self.signal_price) / self.signal_price) * 10000
        return ((self.signal_price - avg_fill) / self.signal_price) * 10000
