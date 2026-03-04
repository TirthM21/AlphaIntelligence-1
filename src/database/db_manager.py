"""Database management for AlphaIntelligence Capital.

Handles subscribers, signal tracking, position management, and fund performance."""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()


class Subscriber(Base):
    """Newsletter subscriber model."""

    __tablename__ = "subscribers"

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    name = Column(String(100), nullable=True)
    is_active = Column(Boolean, default=True)
    subscribed_at = Column(DateTime, default=datetime.utcnow)


class Recommendation(Base):
    """Stores generated buy/sell signals for historical tracking."""

    __tablename__ = "recommendations"

    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False)
    signal_type = Column(String(10), nullable=False)  # 'BUY' or 'SELL'
    price_at_signal = Column(Float, nullable=False)
    score = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    benchmark_price_at_signal = Column(Float)  # For benchmarking


class Position(Base):
    """Tracks individual open/closed positions for P&L tracking."""

    __tablename__ = "positions"

    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False)
    entry_price = Column(Float, nullable=False)
    entry_date = Column(DateTime, default=datetime.utcnow)
    exit_price = Column(Float, nullable=True)
    exit_date = Column(DateTime, nullable=True)
    stop_loss = Column(Float, nullable=True)
    signal_score = Column(Float, nullable=True)
    status = Column(String(10), default="OPEN")  # OPEN or CLOSED
    pnl_pct = Column(Float, nullable=True)  # Filled on close
    strategy = Column(String(20), default="DAILY")  # DAILY or QUARTERLY
    exit_reason = Column(
        String(50), nullable=True
    )  # SELL_SIGNAL, STOP_LOSS, SMA_VIOLATION, MANUAL
    benchmark_entry_price = Column(Float, nullable=True)
    benchmark_exit_price = Column(Float, nullable=True)
    entry_order_id = Column(Integer, ForeignKey("simulated_orders.id"), nullable=True)
    exit_order_id = Column(Integer, ForeignKey("simulated_orders.id"), nullable=True)


class SimulatedOrder(Base):
    """Simulated order linked to a position lifecycle."""

    __tablename__ = "simulated_orders"

    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False)
    strategy = Column(String(20), default="DAILY")
    side = Column(String(10), nullable=False)  # BUY / SELL
    order_type = Column(String(10), nullable=False)  # MARKET / LIMIT / STOP
    status = Column(String(10), nullable=False, default="NEW")
    quantity = Column(Float, nullable=False, default=1.0)
    filled_quantity = Column(Float, nullable=False, default=0.0)
    signal_price = Column(Float, nullable=False)
    limit_price = Column(Float, nullable=True)
    stop_price = Column(Float, nullable=True)
    avg_fill_price = Column(Float, nullable=True)
    slippage_bps = Column(Float, nullable=True)
    fill_ratio = Column(Float, nullable=True)
    time_to_fill_ms = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class SimulatedFill(Base):
    """Individual fill records for simulated orders."""

    __tablename__ = "simulated_fills"

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("simulated_orders.id"), nullable=False)
    quantity = Column(Float, nullable=False)
    fill_price = Column(Float, nullable=False)
    latency_ms = Column(Integer, nullable=True)
    filled_at = Column(DateTime, default=datetime.utcnow)


class FundPerformance(Base):
    """Daily fund-level performance snapshot."""

    __tablename__ = "fund_performance"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    total_pnl_pct = Column(Float, default=0.0)
    open_positions = Column(Integer, default=0)
    closed_positions = Column(Integer, default=0)
    win_rate = Column(Float, default=0.0)
    avg_gain = Column(Float, default=0.0)
    avg_loss = Column(Float, default=0.0)
    best_trade = Column(String(20), nullable=True)
    worst_trade = Column(String(20), nullable=True)
    sharpe_ratio = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    alpha_vs_benchmark = Column(Float, default=0.0)
    benchmark_return = Column(Float, default=0.0)
    strategy = Column(String(20), default="DAILY")
    avg_slippage_bps = Column(Float, nullable=True)
    avg_fill_ratio = Column(Float, nullable=True)
    avg_time_to_fill_ms = Column(Float, nullable=True)


class DBManager:
    """Handles connection and operations for configured SQLAlchemy database."""

    def __init__(self, db_url: Optional[str] = None):
        self.db_url = db_url or os.getenv("DATABASE_URL")
        if not self.db_url:
            logger.warning("DATABASE_URL not set. Database features will be disabled.")
            return

        try:
            # Fix for neon connection strings that might need 'postgresql://' instead of 'postgres://'
            if self.db_url.startswith("postgres://"):
                self.db_url = self.db_url.replace("postgres://", "postgresql://", 1)

            self.engine = create_engine(self.db_url)
            self.Session = sessionmaker(bind=self.engine)

            # Create tables
            Base.metadata.create_all(self.engine)
            logger.info("Database connection established and tables verified.")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            self.db_url = None

    @staticmethod
    def _to_native_float(value: Optional[float]) -> Optional[float]:
        """Convert numpy/pandas scalar types into plain Python floats for DB writes."""
        if value is None:
            return None

        # numpy scalar values expose `.item()`; convert without importing numpy.
        if hasattr(value, "item"):
            try:
                value = value.item()
            except Exception:
                pass

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def add_subscriber(self, email: str, name: Optional[str] = None) -> bool:
        """Add a new newsletter subscriber."""
        if not self.db_url:
            return False

        with self.Session() as session:
            try:
                existing = session.query(Subscriber).filter_by(email=email).first()
                if existing:
                    if not existing.is_active:
                        existing.is_active = True
                        session.commit()
                        return True
                    return False

                new_sub = Subscriber(email=email, name=name)
                session.add(new_sub)
                session.commit()
                return True
            except Exception as e:
                logger.error(f"Failed to add subscriber: {e}")
                session.rollback()
                return False

    def get_active_subscribers(self) -> List[str]:
        """Get all active subscriber emails."""
        if not self.db_url:
            return []

        with self.Session() as session:
            try:
                subs = session.query(Subscriber.email).filter_by(is_active=True).all()
                return [s.email for s in subs]
            except Exception as e:
                logger.error(f"Failed to fetch subscribers: {e}")
                return []

    def unsubscribe(self, email: str) -> bool:
        """Deactivate a subscriber."""
        if not self.db_url:
            return False

        with self.Session() as session:
            try:
                sub = session.query(Subscriber).filter_by(email=email).first()
                if sub:
                    sub.is_active = False
                    session.commit()
                    return True
                return False
            except Exception as e:
                logger.error(f"Unsubscribe failed: {e}")
                session.rollback()
                return False

    def record_recommendations(self, signals: List[Dict], benchmark_price: float):
        """Save a batch of signals to the database for tracking."""
        if not self.db_url or not signals:
            return

        with self.Session() as session:
            try:
                normalized_benchmark_price = self._to_native_float(benchmark_price)
                inserted = 0
                skipped = 0
                for s in signals:
                    price_at_signal = self._to_native_float(s.get("current_price"))
                    if price_at_signal is None:
                        skipped += 1
                        continue
                    rec = Recommendation(
                        ticker=s["ticker"],
                        signal_type="BUY" if s.get("is_buy") else "SELL",
                        price_at_signal=price_at_signal,
                        score=self._to_native_float(s.get("score")),
                        benchmark_price_at_signal=normalized_benchmark_price,
                    )
                    session.add(rec)
                    inserted += 1
                session.commit()
                logger.info(
                    f"Recorded {inserted} recommendations for tracking (skipped {skipped})."
                )
            except Exception as e:
                logger.error(f"Failed to record recommendations: {e}")
                session.rollback()

    def get_recommendation_performance(self) -> List[Dict]:
        """Fetch historical recommendations for comparison."""
        if not self.db_url:
            return []

        with self.Session() as session:
            recs = session.query(Recommendation).all()
            return [
                {
                    "ticker": r.ticker,
                    "type": r.signal_type,
                    "entry_price": r.price_at_signal,
                    "benchmark_entry": r.benchmark_price_at_signal,
                    "date": r.timestamp,
                }
                for r in recs
            ]

    # ========== POSITION TRACKING ==========

    def open_position(
        self,
        ticker: str,
        entry_price: float,
        stop_loss: float = None,
        signal_score: float = None,
        strategy: str = "DAILY",
        benchmark_price: float = None,
        entry_order_id: int = None,
    ) -> bool:
        """Open a new position. Skips if already open for this ticker+strategy."""
        if not self.db_url:
            return False

        with self.Session() as session:
            try:
                existing = session.query(Position).filter_by(
                    ticker=ticker, strategy=strategy, status="OPEN"
                ).first()
                if existing:
                    logger.debug(f"Position already open for {ticker} ({strategy})")
                    return False

                normalized_entry_price = self._to_native_float(entry_price)
                if normalized_entry_price is None:
                    logger.error(f"Invalid entry_price for {ticker}: {entry_price}")
                    return False

                pos = Position(
                    ticker=ticker,
                    entry_price=normalized_entry_price,
                    stop_loss=self._to_native_float(stop_loss),
                    signal_score=self._to_native_float(signal_score),
                    strategy=strategy,
                    benchmark_entry_price=self._to_native_float(benchmark_price),
                    entry_order_id=entry_order_id,
                )
                session.add(pos)
                session.commit()
                logger.info(f"📈 Opened {strategy} position: {ticker} @ {entry_price:.2f}")
                return True
            except Exception as e:
                logger.error(f"Failed to open position for {ticker}: {e}")
                session.rollback()
                return False

    def close_position(
        self,
        ticker: str,
        exit_price: float,
        exit_reason: str = "SELL_SIGNAL",
        strategy: str = "DAILY",
        benchmark_price: float = None,
        exit_order_id: int = None,
    ) -> Optional[Dict]:
        """Close an open position. Returns the closed position data or None."""
        if not self.db_url:
            return None

        with self.Session() as session:
            try:
                pos = session.query(Position).filter_by(
                    ticker=ticker, strategy=strategy, status="OPEN"
                ).first()
                if not pos:
                    logger.debug(f"No open position for {ticker} ({strategy})")
                    return None

                normalized_exit_price = self._to_native_float(exit_price)
                pos.exit_price = normalized_exit_price
                pos.exit_date = datetime.utcnow()
                pos.status = "CLOSED"
                pos.exit_reason = exit_reason
                pos.benchmark_exit_price = self._to_native_float(benchmark_price)
                pos.exit_order_id = exit_order_id
                if normalized_exit_price is not None and pos.entry_price:
                    pos.pnl_pct = (
                        (normalized_exit_price - pos.entry_price) / pos.entry_price
                    ) * 100
                else:
                    pos.pnl_pct = None

                session.commit()

                result = {
                    "ticker": ticker,
                    "entry_price": pos.entry_price,
                    "exit_price": normalized_exit_price,
                    "pnl_pct": pos.pnl_pct,
                    "exit_reason": exit_reason,
                    "hold_days": (pos.exit_date - pos.entry_date).days,
                }
                pnl_display = f"{pos.pnl_pct:+.1f}%" if pos.pnl_pct is not None else "N/A"
                emoji = "💰" if (pos.pnl_pct is not None and pos.pnl_pct > 0) else "📉"
                logger.info(
                    f"{emoji} Closed {strategy} position: {ticker} | P&L: {pnl_display} | Reason: {exit_reason}"
                )
                return result
            except Exception as e:
                logger.error(f"Failed to close position for {ticker}: {e}")
                session.rollback()
                return None

    def get_open_positions(self, strategy: str = None) -> List[Dict]:
        """Get all open positions, optionally filtered by strategy."""
        if not self.db_url:
            return []

        with self.Session() as session:
            try:
                query = session.query(Position).filter_by(status="OPEN")
                if strategy:
                    query = query.filter_by(strategy=strategy)
                positions = query.all()
                return [
                    {
                        "ticker": p.ticker,
                        "entry_price": p.entry_price,
                        "entry_date": p.entry_date,
                        "stop_loss": p.stop_loss,
                        "signal_score": p.signal_score,
                        "strategy": p.strategy,
                        "benchmark_entry_price": p.benchmark_entry_price,
                        "entry_order_id": p.entry_order_id,
                    }
                    for p in positions
                ]
            except Exception as e:
                logger.error(f"Failed to fetch open positions: {e}")
                return []

    def get_closed_positions(self, strategy: str = None, limit: int = 100) -> List[Dict]:
        """Get closed positions for performance analysis."""
        if not self.db_url:
            return []

        with self.Session() as session:
            try:
                query = session.query(Position).filter_by(status="CLOSED")
                if strategy:
                    query = query.filter_by(strategy=strategy)
                positions = query.order_by(Position.exit_date.desc()).limit(limit).all()
                return [
                    {
                        "ticker": p.ticker,
                        "entry_price": p.entry_price,
                        "exit_price": p.exit_price,
                        "entry_date": p.entry_date,
                        "exit_date": p.exit_date,
                        "pnl_pct": p.pnl_pct,
                        "exit_reason": p.exit_reason,
                        "strategy": p.strategy,
                        "hold_days": (
                            (p.exit_date - p.entry_date).days
                            if p.exit_date and p.entry_date
                            else 0
                        ),
                        "benchmark_entry_price": p.benchmark_entry_price,
                        "benchmark_exit_price": p.benchmark_exit_price,
                        "entry_order_id": p.entry_order_id,
                        "exit_order_id": p.exit_order_id,
                    }
                    for p in positions
                ]
            except Exception as e:
                logger.error(f"Failed to fetch closed positions: {e}")
                return []

    def record_simulated_order(self, order_data: Dict, fills: List[Dict]) -> Optional[int]:
        """Persist a simulated order and its fill events."""
        if not self.db_url:
            return None

        with self.Session() as session:
            try:
                order = SimulatedOrder(
                    ticker=order_data["ticker"],
                    strategy=order_data.get("strategy", "DAILY"),
                    side=order_data["side"],
                    order_type=order_data["order_type"],
                    status=order_data.get("status", "NEW"),
                    quantity=self._to_native_float(order_data.get("quantity", 1.0)) or 1.0,
                    filled_quantity=self._to_native_float(order_data.get("filled_quantity", 0.0)) or 0.0,
                    signal_price=self._to_native_float(order_data.get("signal_price")) or 0.0,
                    limit_price=self._to_native_float(order_data.get("limit_price")),
                    stop_price=self._to_native_float(order_data.get("stop_price")),
                    avg_fill_price=self._to_native_float(order_data.get("avg_fill_price")),
                    slippage_bps=self._to_native_float(order_data.get("slippage_bps")),
                    fill_ratio=self._to_native_float(order_data.get("fill_ratio")),
                    time_to_fill_ms=order_data.get("time_to_fill_ms"),
                    created_at=order_data.get("created_at", datetime.utcnow()),
                )
                session.add(order)
                session.flush()

                for fill in fills:
                    session.add(
                        SimulatedFill(
                            order_id=order.id,
                            quantity=self._to_native_float(fill.get("quantity")) or 0.0,
                            fill_price=self._to_native_float(fill.get("fill_price")) or 0.0,
                            latency_ms=fill.get("latency_ms"),
                            filled_at=fill.get("filled_at", datetime.utcnow()),
                        )
                    )

                session.commit()
                return order.id
            except Exception as e:
                logger.error(f"Failed to record simulated order: {e}")
                session.rollback()
                return None

    def reconcile_positions_from_fills(self, strategy: str = "DAILY") -> int:
        """Recompute position pricing and PnL using simulated fills."""
        if not self.db_url:
            return 0

        with self.Session() as session:
            try:
                positions = session.query(Position).filter_by(strategy=strategy).all()
                updated = 0

                for pos in positions:
                    changed = False

                    if pos.entry_order_id:
                        entry_order = session.get(SimulatedOrder, pos.entry_order_id)
                        if entry_order and entry_order.avg_fill_price:
                            entry_fill = self._to_native_float(entry_order.avg_fill_price)
                            if entry_fill and pos.entry_price != entry_fill:
                                pos.entry_price = entry_fill
                                changed = True

                    if pos.exit_order_id:
                        exit_order = session.get(SimulatedOrder, pos.exit_order_id)
                        if exit_order and exit_order.avg_fill_price:
                            exit_fill = self._to_native_float(exit_order.avg_fill_price)
                            if exit_fill and pos.exit_price != exit_fill:
                                pos.exit_price = exit_fill
                                changed = True

                    if pos.exit_price and pos.entry_price and pos.entry_price > 0:
                        pnl_pct = ((pos.exit_price - pos.entry_price) / pos.entry_price) * 100
                        pnl_pct = self._to_native_float(pnl_pct)
                        if pnl_pct != pos.pnl_pct:
                            pos.pnl_pct = pnl_pct
                            changed = True

                    if changed:
                        updated += 1

                session.commit()
                return updated
            except Exception as e:
                logger.error(f"Failed position reconciliation: {e}")
                session.rollback()
                return 0

    def get_execution_quality_metrics(
        self, strategy: str = "DAILY", limit: int = 500
    ) -> Dict[str, Optional[float]]:
        """Aggregate execution-quality metrics from simulated orders."""
        if not self.db_url:
            return {
                "avg_slippage_bps": None,
                "avg_fill_ratio": None,
                "avg_time_to_fill_ms": None,
            }

        with self.Session() as session:
            try:
                orders = (
                    session.query(SimulatedOrder)
                    .filter_by(strategy=strategy)
                    .order_by(SimulatedOrder.created_at.desc())
                    .limit(limit)
                    .all()
                )
                if not orders:
                    return {
                        "avg_slippage_bps": None,
                        "avg_fill_ratio": None,
                        "avg_time_to_fill_ms": None,
                    }

                slippages = [o.slippage_bps for o in orders if o.slippage_bps is not None]
                fill_ratios = [o.fill_ratio for o in orders if o.fill_ratio is not None]
                ttfs = [o.time_to_fill_ms for o in orders if o.time_to_fill_ms is not None]

                def _avg(values):
                    return sum(values) / len(values) if values else None

                return {
                    "avg_slippage_bps": _avg(slippages),
                    "avg_fill_ratio": _avg(fill_ratios),
                    "avg_time_to_fill_ms": _avg(ttfs),
                }
            except Exception as e:
                logger.error(f"Failed to compute execution metrics: {e}")
                return {
                    "avg_slippage_bps": None,
                    "avg_fill_ratio": None,
                    "avg_time_to_fill_ms": None,
                }

    def record_daily_performance(self, perf_data: Dict) -> bool:
        """Record a daily fund performance snapshot."""
        if not self.db_url:
            return False

        with self.Session() as session:
            try:
                today = datetime.utcnow().date()
                existing = session.query(FundPerformance).filter_by(
                    date=today, strategy=perf_data.get("strategy", "DAILY")
                ).first()

                allowed_cols = {c.name for c in FundPerformance.__table__.columns}
                sanitized_data = {
                    k: v for k, v in perf_data.items() if k in allowed_cols and k != "date"
                }

                float_cols = {
                    "total_pnl_pct",
                    "win_rate",
                    "avg_gain",
                    "avg_loss",
                    "sharpe_ratio",
                    "max_drawdown",
                    "alpha_vs_benchmark",
                    "benchmark_return",
                }
                int_cols = {"open_positions", "closed_positions"}
                nullable_cols = {
                    c.name for c in FundPerformance.__table__.columns if c.nullable
                }

                for key in list(sanitized_data.keys()):
                    value = sanitized_data[key]

                    if value is None and key in nullable_cols:
                        continue

                    if key in float_cols:
                        sanitized_data[key] = self._to_native_float(value)
                        continue

                    if key in int_cols:
                        if value is None and key in nullable_cols:
                            continue
                        if hasattr(value, "item"):
                            try:
                                value = value.item()
                            except Exception:
                                pass
                        try:
                            sanitized_data[key] = int(value)
                        except (TypeError, ValueError):
                            sanitized_data[key] = None if key in nullable_cols else 0
                        continue

                    if hasattr(value, "item"):
                        try:
                            sanitized_data[key] = value.item()
                        except Exception:
                            pass

                if existing:
                    for key, val in sanitized_data.items():
                        setattr(existing, key, val)
                else:
                    perf = FundPerformance(date=today, **sanitized_data)
                    session.add(perf)

                session.commit()
                return True
            except Exception as e:
                logger.error(f"Failed to record daily performance: {e}")
                session.rollback()
                return False

    def get_performance_history(self, strategy: str = "DAILY", limit: int = 30) -> List[Dict]:
        """Get recent fund performance history."""
        if not self.db_url:
            return []

        with self.Session() as session:
            try:
                records = (
                    session.query(FundPerformance)
                    .filter_by(strategy=strategy)
                    .order_by(FundPerformance.date.desc())
                    .limit(limit)
                    .all()
                )
                return [
                    {
                        "date": r.date,
                        "total_pnl_pct": r.total_pnl_pct,
                        "open_positions": r.open_positions,
                        "closed_positions": r.closed_positions,
                        "win_rate": r.win_rate,
                        "avg_gain": r.avg_gain,
                        "avg_loss": r.avg_loss,
                        "sharpe_ratio": r.sharpe_ratio,
                        "max_drawdown": r.max_drawdown,
                        "alpha_vs_benchmark": r.alpha_vs_benchmark,
                        "benchmark_return": r.benchmark_return,
                        "best_trade": r.best_trade,
                        "worst_trade": r.worst_trade,
                    }
                    for r in records
                ]
            except Exception as e:
                logger.error(f"Failed to fetch performance history: {e}")
                return []
