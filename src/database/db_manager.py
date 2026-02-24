"""Database management for AlphaIntelligence Capital.

Handles subscribers, signal tracking, position management, and fund performance."""

import logging
import os
from datetime import datetime
from typing import List, Optional, Dict
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Float, ForeignKey, Date, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

Base = declarative_base()

class Subscriber(Base):
    """Newsletter subscriber model."""
    __tablename__ = 'subscribers'
    
    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    name = Column(String(100), nullable=True)
    is_active = Column(Boolean, default=True)
    subscribed_at = Column(DateTime, default=datetime.utcnow)

class Recommendation(Base):
    """Stores generated buy/sell signals for historical tracking."""
    __tablename__ = 'recommendations'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False)
    signal_type = Column(String(10), nullable=False) # 'BUY' or 'SELL'
    price_at_signal = Column(Float, nullable=False)
    score = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    spy_price_at_signal = Column(Float) # For benchmarking

class Position(Base):
    """Tracks individual open/closed positions for P&L tracking."""
    __tablename__ = 'positions'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False)
    entry_price = Column(Float, nullable=False)
    entry_date = Column(DateTime, default=datetime.utcnow)
    exit_price = Column(Float, nullable=True)
    exit_date = Column(DateTime, nullable=True)
    stop_loss = Column(Float, nullable=True)
    signal_score = Column(Float, nullable=True)
    status = Column(String(10), default='OPEN')  # OPEN or CLOSED
    pnl_pct = Column(Float, nullable=True)  # Filled on close
    strategy = Column(String(20), default='DAILY')  # DAILY or QUARTERLY
    exit_reason = Column(String(50), nullable=True)  # SELL_SIGNAL, STOP_LOSS, SMA_VIOLATION, MANUAL
    spy_entry_price = Column(Float, nullable=True)
    spy_exit_price = Column(Float, nullable=True)

class FundPerformance(Base):
    """Daily fund-level performance snapshot."""
    __tablename__ = 'fund_performance'
    
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
    alpha_vs_spy = Column(Float, default=0.0)
    spy_return = Column(Float, default=0.0)
    strategy = Column(String(20), default='DAILY')

class DBManager:
    """Handles connection and operations for the Neon Postgres database."""
    
    def __init__(self, db_url: Optional[str] = None):
        self.db_url = db_url or os.getenv('DATABASE_URL')
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
        if not self.db_url: return False
        
        session = self.Session()
        try:
            # Check if exists
            existing = session.query(Subscriber).filter_by(email=email).first()
            if existing:
                if not existing.is_active:
                    existing.is_active = True
                    session.commit()
                    return True
                return False # Already subscribed
            
            new_sub = Subscriber(email=email, name=name)
            session.add(new_sub)
            session.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to add subscriber: {e}")
            session.rollback()
            return False
        finally:
            session.close()

    def get_active_subscribers(self) -> List[str]:
        """Get all active subscriber emails."""
        if not self.db_url: return []
        
        session = self.Session()
        try:
            subs = session.query(Subscriber.email).filter_by(is_active=True).all()
            return [s.email for s in subs]
        except Exception as e:
            logger.error(f"Failed to fetch subscribers: {e}")
            return []
        finally:
            session.close()
            
    def unsubscribe(self, email: str) -> bool:
        """Deactivate a subscriber."""
        if not self.db_url: return False
        
        session = self.Session()
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
        finally:
            session.close()

    def record_recommendations(self, signals: List[Dict], spy_price: float):
        """Save a batch of signals to the database for tracking."""
        if not self.db_url or not signals: return
        
        session = self.Session()
        try:
            normalized_spy_price = self._to_native_float(spy_price)
            inserted = 0
            skipped = 0
            for s in signals:
                price_at_signal = self._to_native_float(s.get('current_price'))
                if price_at_signal is None:
                    skipped += 1
                    continue
                rec = Recommendation(
                    ticker=s['ticker'],
                    signal_type='BUY' if s.get('is_buy') else 'SELL',
                    price_at_signal=price_at_signal,
                    score=self._to_native_float(s.get('score')),
                    spy_price_at_signal=normalized_spy_price
                )
                session.add(rec)
                inserted += 1
            session.commit()
            logger.info(f"Recorded {inserted} recommendations for tracking (skipped {skipped}).")
        except Exception as e:
            logger.error(f"Failed to record recommendations: {e}")
            session.rollback()
        finally:
            session.close()

    def get_recommendation_performance(self) -> List[Dict]:
        """Fetch historical recommendations for comparison."""
        if not self.db_url: return []
        
        session = self.Session()
        try:
            recs = session.query(Recommendation).all()
            return [
                {
                    'ticker': r.ticker,
                    'type': r.signal_type,
                    'entry_price': r.price_at_signal,
                    'spy_entry': r.spy_price_at_signal,
                    'date': r.timestamp
                } for r in recs
            ]
        finally:
            session.close()

    # ========== POSITION TRACKING ==========

    def open_position(self, ticker: str, entry_price: float, stop_loss: float = None,
                      signal_score: float = None, strategy: str = 'DAILY',
                      spy_price: float = None) -> bool:
        """Open a new position. Skips if already open for this ticker+strategy."""
        if not self.db_url: return False
        
        session = self.Session()
        try:
            existing = session.query(Position).filter_by(
                ticker=ticker, strategy=strategy, status='OPEN'
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
                spy_entry_price=self._to_native_float(spy_price)
            )
            session.add(pos)
            session.commit()
            logger.info(f"📈 Opened {strategy} position: {ticker} @ ${entry_price:.2f}")
            return True
        except Exception as e:
            logger.error(f"Failed to open position for {ticker}: {e}")
            session.rollback()
            return False
        finally:
            session.close()

    def close_position(self, ticker: str, exit_price: float, exit_reason: str = 'SELL_SIGNAL',
                       strategy: str = 'DAILY', spy_price: float = None) -> Optional[Dict]:
        """Close an open position. Returns the closed position data or None."""
        if not self.db_url: return None
        
        session = self.Session()
        try:
            pos = session.query(Position).filter_by(
                ticker=ticker, strategy=strategy, status='OPEN'
            ).first()
            if not pos:
                logger.debug(f"No open position for {ticker} ({strategy})")
                return None
            
            normalized_exit_price = self._to_native_float(exit_price)
            pos.exit_price = normalized_exit_price
            pos.exit_date = datetime.utcnow()
            pos.status = 'CLOSED'
            pos.exit_reason = exit_reason
            pos.spy_exit_price = self._to_native_float(spy_price)
            if normalized_exit_price is not None and pos.entry_price:
                pos.pnl_pct = ((normalized_exit_price - pos.entry_price) / pos.entry_price) * 100
            else:
                pos.pnl_pct = None
            
            session.commit()
            
            result = {
                'ticker': ticker,
                'entry_price': pos.entry_price,
                'exit_price': normalized_exit_price,
                'pnl_pct': pos.pnl_pct,
                'exit_reason': exit_reason,
                'hold_days': (pos.exit_date - pos.entry_date).days
            }
            pnl_display = f"{pos.pnl_pct:+.1f}%" if pos.pnl_pct is not None else "N/A"
            emoji = '💰' if (pos.pnl_pct is not None and pos.pnl_pct > 0) else '📉'
            logger.info(f"{emoji} Closed {strategy} position: {ticker} | P&L: {pnl_display} | Reason: {exit_reason}")
            return result
        except Exception as e:
            logger.error(f"Failed to close position for {ticker}: {e}")
            session.rollback()
            return None
        finally:
            session.close()

    def get_open_positions(self, strategy: str = None) -> List[Dict]:
        """Get all open positions, optionally filtered by strategy."""
        if not self.db_url: return []
        
        session = self.Session()
        try:
            query = session.query(Position).filter_by(status='OPEN')
            if strategy:
                query = query.filter_by(strategy=strategy)
            positions = query.all()
            return [{
                'ticker': p.ticker,
                'entry_price': p.entry_price,
                'entry_date': p.entry_date,
                'stop_loss': p.stop_loss,
                'signal_score': p.signal_score,
                'strategy': p.strategy,
                'spy_entry_price': p.spy_entry_price
            } for p in positions]
        except Exception as e:
            logger.error(f"Failed to fetch open positions: {e}")
            return []
        finally:
            session.close()

    def get_closed_positions(self, strategy: str = None, limit: int = 100) -> List[Dict]:
        """Get closed positions for performance analysis."""
        if not self.db_url: return []
        
        session = self.Session()
        try:
            query = session.query(Position).filter_by(status='CLOSED')
            if strategy:
                query = query.filter_by(strategy=strategy)
            positions = query.order_by(Position.exit_date.desc()).limit(limit).all()
            return [{
                'ticker': p.ticker,
                'entry_price': p.entry_price,
                'exit_price': p.exit_price,
                'entry_date': p.entry_date,
                'exit_date': p.exit_date,
                'pnl_pct': p.pnl_pct,
                'exit_reason': p.exit_reason,
                'strategy': p.strategy,
                'hold_days': (p.exit_date - p.entry_date).days if p.exit_date and p.entry_date else 0,
                'spy_entry_price': p.spy_entry_price,
                'spy_exit_price': p.spy_exit_price
            } for p in positions]
        except Exception as e:
            logger.error(f"Failed to fetch closed positions: {e}")
            return []
        finally:
            session.close()

    def record_daily_performance(self, perf_data: Dict) -> bool:
        """Record a daily fund performance snapshot."""
        if not self.db_url: return False
        
        session = self.Session()
        try:
            today = datetime.utcnow().date()
            existing = session.query(FundPerformance).filter_by(
                date=today, strategy=perf_data.get('strategy', 'DAILY')
            ).first()
            
            if existing:
                # Update existing record
                for key, val in perf_data.items():
                    if hasattr(existing, key) and key != 'date':
                        setattr(existing, key, val)
            else:
                perf = FundPerformance(date=today, **perf_data)
                session.add(perf)
            
            session.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to record daily performance: {e}")
            session.rollback()
            return False
        finally:
            session.close()

    def get_performance_history(self, strategy: str = 'DAILY', limit: int = 30) -> List[Dict]:
        """Get recent fund performance history."""
        if not self.db_url: return []
        
        session = self.Session()
        try:
            records = session.query(FundPerformance).filter_by(
                strategy=strategy
            ).order_by(FundPerformance.date.desc()).limit(limit).all()
            return [{
                'date': r.date,
                'total_pnl_pct': r.total_pnl_pct,
                'open_positions': r.open_positions,
                'closed_positions': r.closed_positions,
                'win_rate': r.win_rate,
                'avg_gain': r.avg_gain,
                'avg_loss': r.avg_loss,
                'sharpe_ratio': r.sharpe_ratio,
                'max_drawdown': r.max_drawdown,
                'alpha_vs_spy': r.alpha_vs_spy,
                'spy_return': r.spy_return,
                'best_trade': r.best_trade,
                'worst_trade': r.worst_trade
            } for r in records]
        except Exception as e:
            logger.error(f"Failed to fetch performance history: {e}")
            return []
        finally:
            session.close()
