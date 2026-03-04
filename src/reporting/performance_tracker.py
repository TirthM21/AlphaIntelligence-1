"""AlphaIntelligence Capital — Performance Tracker.

Tracks hedge fund performance by:
1. Opening positions when BUY signals fire
2. Closing positions when SELL signals fire or stop-loss/SMA violations occur
3. Computing fund-level metrics: P&L, win rate, Sharpe, drawdown, alpha vs Nifty 50
"""

import logging
import time
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional

from ..database.db_manager import DBManager
from ..data.price_service import PriceService
from ..execution import PaperBroker

logger = logging.getLogger(__name__)


class PerformanceTracker:
    """Tracks and reports fund performance based on daily/quarterly signals."""

    def __init__(self, strategy: str = 'DAILY'):
        """Initialize tracker.
        
        Args:
            strategy: 'DAILY' for short-term momentum, 'QUARTERLY' for compounders
        """
        self.strategy = strategy
        self.db = DBManager()
        self._benchmark_price = None
        self.price_service = PriceService()
        self.paper_broker = PaperBroker()

    @property
    def benchmark_price(self) -> float:
        """Get current Nifty 50 price (cached per session)."""
        if self._benchmark_price is None:
            for attempt in range(3):
                bench_price = self.price_service.get_current_price("^NSEI")
                if bench_price and bench_price > 0:
                    self._benchmark_price = float(bench_price)
                    break
                if attempt < 2:
                    time.sleep(1)
            if self._benchmark_price is None:
                logger.info("Nifty 50 price unavailable after 3 attempts; continuing with benchmark=0.0")
                self._benchmark_price = 0.0
        return self._benchmark_price or 0.0

    def process_signals(self, buy_signals: List[Dict], sell_signals: List[Dict],
                        benchmark_price: float = None):
        """Process buy/sell signals from a scan run.
        
        - Opens positions for new BUY signals
        - Closes positions for SELL signals that match open positions
        
        Args:
            buy_signals: List of buy signal dicts from the scanner
            sell_signals: List of sell signal dicts from the scanner
            benchmark_price: Current Nifty 50 price for benchmarking
        """
        if benchmark_price:
            self._benchmark_price = benchmark_price

        opened = 0
        closed = 0

        # Validate sources and fetch authoritative market prices from yfinance
        sell_tickers = set()
        price_tickers = []

        for signal in sell_signals:
            ticker = signal.get('ticker', '')
            if not ticker:
                continue
            valid, source = self.price_service.validate_price_payload_source(
                signal,
                context=f"sell signal {ticker}",
            )
            if not valid:
                logger.error("Rejecting SELL signal payload for %s due to blocked price source=%s", ticker, source)
                continue
            sell_tickers.add(ticker)
            price_tickers.append(ticker)

        valid_buy_signals = []
        for signal in buy_signals:
            ticker = signal.get('ticker', '')
            if not ticker or ticker in sell_tickers:
                continue
            valid, source = self.price_service.validate_price_payload_source(
                signal,
                context=f"buy signal {ticker}",
            )
            if not valid:
                logger.error("Rejecting BUY signal payload for %s due to blocked price source=%s", ticker, source)
                continue
            valid_buy_signals.append(signal)
            price_tickers.append(ticker)

        latest_prices = self.price_service.get_batch_current_prices(price_tickers)

        # Process SELL signals first (close positions)
        for ticker in sell_tickers:
            current_price = latest_prices.get(ticker, 0)
            if current_price > 0:
                sell_order = self.paper_broker.submit_order(
                    ticker=ticker,
                    side='SELL',
                    quantity=1.0,
                    order_type='MARKET',
                    signal_price=current_price,
                    market_price=current_price,
                )
                exit_order_id = self._store_simulated_order(sell_order)
                fill_price = sell_order.average_fill_price or current_price
                result = self.db.close_position(
                    ticker=ticker,
                    exit_price=fill_price,
                    exit_reason='SELL_SIGNAL',
                    strategy=self.strategy,
                    benchmark_price=self.benchmark_price,
                    exit_order_id=exit_order_id,
                )
                if result:
                    closed += 1

        # Process BUY signals (open positions)
        for signal in valid_buy_signals:
            ticker = signal.get('ticker', '')
            current_price = latest_prices.get(ticker, 0)
            stop_loss = signal.get('stop_loss')
            score = signal.get('score', 0)

            if current_price > 0:
                buy_order = self.paper_broker.submit_order(
                    ticker=ticker,
                    side='BUY',
                    quantity=1.0,
                    order_type='MARKET',
                    signal_price=current_price,
                    market_price=current_price,
                )
                entry_order_id = self._store_simulated_order(buy_order)
                fill_price = buy_order.average_fill_price or current_price
                success = self.db.open_position(
                    ticker=ticker,
                    entry_price=fill_price,
                    stop_loss=stop_loss,
                    signal_score=score,
                    strategy=self.strategy,
                    benchmark_price=self.benchmark_price,
                    entry_order_id=entry_order_id,
                )
                if success:
                    opened += 1

        logger.info(f"📊 Signal processing complete: {opened} opened, {closed} closed ({self.strategy})")

    def check_stop_losses(self) -> List[Dict]:
        """Check all open positions for stop-loss violations.
        
        Fetches current prices and closes positions that hit their stop loss.
        
        Returns:
            List of closed position dicts
        """
        open_positions = self.db.get_open_positions(strategy=self.strategy)
        if not open_positions:
            return []

        closed = []
        tickers = [p['ticker'] for p in open_positions]

        # Batch fetch current prices
        current_prices = self._batch_fetch_prices(tickers)

        for pos in open_positions:
            ticker = pos['ticker']
            current_price = current_prices.get(ticker)
            if not current_price or current_price <= 0:
                continue

            stop_loss = pos.get('stop_loss')

            # Check stop loss
            if stop_loss and current_price <= stop_loss:
                sell_order = self.paper_broker.submit_order(
                    ticker=ticker,
                    side='SELL',
                    quantity=1.0,
                    order_type='STOP',
                    signal_price=current_price,
                    market_price=current_price,
                    stop_price=stop_loss,
                )
                exit_order_id = self._store_simulated_order(sell_order)
                fill_price = sell_order.average_fill_price or current_price
                result = self.db.close_position(
                    ticker=ticker,
                    exit_price=fill_price,
                    exit_reason='STOP_LOSS',
                    strategy=self.strategy,
                    benchmark_price=self.benchmark_price,
                    exit_order_id=exit_order_id,
                )
                if result:
                    closed.append(result)
                    logger.warning(f"🛑 Stop loss hit: {ticker} @ {fill_price:.2f} (stop: {stop_loss:.2f})")

        if closed:
            logger.info(f"🛑 {len(closed)} position(s) closed via stop loss")
        return closed

    def compute_fund_metrics(self) -> Dict:
        """Compute comprehensive fund-level performance metrics.
        
        Returns:
            Dict with all fund metrics
        """
        self.reconcile_daily_positions()
        open_positions = self.db.get_open_positions(strategy=self.strategy)
        closed_positions = self.db.get_closed_positions(strategy=self.strategy, limit=500)

        # Current prices for open positions
        if open_positions:
            tickers = [p['ticker'] for p in open_positions]
            current_prices = self._batch_fetch_prices(tickers)
        else:
            current_prices = {}

        # ---- Open position unrealized P&L ----
        unrealized_pnl = []
        for pos in open_positions:
            ticker = pos['ticker']
            current = current_prices.get(ticker)
            if current and pos['entry_price'] > 0:
                pnl = ((current - pos['entry_price']) / pos['entry_price']) * 100
                unrealized_pnl.append({
                    'ticker': ticker,
                    'entry': pos['entry_price'],
                    'current': current,
                    'pnl_pct': pnl,
                    'entry_date': pos['entry_date']
                })

        # ---- Closed position realized P&L ----
        realized_pnl = [p['pnl_pct'] for p in closed_positions if p.get('pnl_pct') is not None]
        
        wins = [p for p in realized_pnl if p > 0]
        losses = [p for p in realized_pnl if p <= 0]

        win_rate = (len(wins) / len(realized_pnl) * 100) if realized_pnl else 0
        avg_gain = np.mean(wins) if wins else 0
        avg_loss = np.mean(losses) if losses else 0
        total_realized_pnl = sum(realized_pnl) if realized_pnl else 0
        total_unrealized_pnl = sum(p['pnl_pct'] for p in unrealized_pnl) if unrealized_pnl else 0

        # Best/worst trades
        best_trade = None
        worst_trade = None
        if closed_positions:
            best = max(closed_positions, key=lambda x: x.get('pnl_pct', 0), default=None)
            worst = min(closed_positions, key=lambda x: x.get('pnl_pct', 0), default=None)
            if best:
                best_trade = f"{best['ticker']} ({best['pnl_pct']:+.1f}%)"
            if worst:
                worst_trade = f"{worst['ticker']} ({worst['pnl_pct']:+.1f}%)"

        # Sharpe ratio (simplified — daily returns from closed trades)
        sharpe = self._compute_sharpe(realized_pnl)

        # Max drawdown
        max_dd = self._compute_max_drawdown(realized_pnl)

        # Alpha vs Nifty 50
        alpha = self._compute_alpha(closed_positions)

        # Nifty 50 return (over same period as our trades)
        benchmark_return = self._compute_benchmark_return(closed_positions)

        execution_metrics = self.db.get_execution_quality_metrics(strategy=self.strategy, limit=500)

        metrics = {
            'strategy': self.strategy,
            'total_pnl_pct': round(total_realized_pnl + total_unrealized_pnl, 2),
            'realized_pnl_pct': round(total_realized_pnl, 2),
            'unrealized_pnl_pct': round(total_unrealized_pnl, 2),
            'open_positions': len(open_positions),
            'closed_positions': len(closed_positions),
            'win_rate': round(win_rate, 1),
            'avg_gain': round(avg_gain, 2),
            'avg_loss': round(avg_loss, 2),
            'best_trade': best_trade,
            'worst_trade': worst_trade,
            'sharpe_ratio': round(sharpe, 2) if sharpe else None,
            'max_drawdown': round(max_dd, 2) if max_dd else None,
            'alpha_vs_benchmark': round(alpha, 2),
            'benchmark_return': round(benchmark_return, 2),
            'open_position_details': unrealized_pnl,
            'total_trades': len(realized_pnl),
            'avg_slippage_bps': (
                round(execution_metrics['avg_slippage_bps'], 2)
                if execution_metrics.get('avg_slippage_bps') is not None
                else None
            ),
            'avg_fill_ratio': (
                round(execution_metrics['avg_fill_ratio'], 4)
                if execution_metrics.get('avg_fill_ratio') is not None
                else None
            ),
            'avg_time_to_fill_ms': (
                round(execution_metrics['avg_time_to_fill_ms'], 1)
                if execution_metrics.get('avg_time_to_fill_ms') is not None
                else None
            ),
        }

        # Record to database
        self.db.record_daily_performance(metrics)

        return metrics

    def get_newsletter_section(self) -> str:
        """Generate a markdown section for the newsletter with fund performance.
        
        Returns:
            Markdown string with performance data
        """
        metrics = self.compute_fund_metrics()
        
        lines = []
        strategy_label = "Short-Term Alpha" if self.strategy == 'DAILY' else "Long-Term Compounder"
        lines.append(f"## 📊 Fund Performance — {strategy_label}")
        lines.append("")

        # Summary stats
        lines.append(f"| Metric | Value |")
        lines.append(f"|:---|:---|")
        lines.append(f"| **Total P&L** | {metrics['total_pnl_pct']:+.2f}% |")
        lines.append(f"| **Realized** | {metrics['realized_pnl_pct']:+.2f}% |")
        lines.append(f"| **Unrealized** | {metrics['unrealized_pnl_pct']:+.2f}% |")
        lines.append(f"| **Win Rate** | {metrics['win_rate']:.1f}% ({len([x for x in (self.db.get_closed_positions(self.strategy) or []) if x.get('pnl_pct',0)>0])}/{metrics['closed_positions']}) |")
        lines.append(f"| **Avg Win** | {metrics['avg_gain']:+.2f}% |")
        lines.append(f"| **Avg Loss** | {metrics['avg_loss']:+.2f}% |")
        if metrics.get('sharpe_ratio') is not None:
            lines.append(f"| **Sharpe Ratio** | {metrics['sharpe_ratio']:.2f} |")
        if metrics.get('max_drawdown') is not None:
            lines.append(f"| **Max Drawdown** | {metrics['max_drawdown']:.2f}% |")
        lines.append(f"| **Alpha vs Nifty 50** | {metrics['alpha_vs_benchmark']:+.2f}% |")
        lines.append(f"| **Nifty 50 Return** | {metrics['benchmark_return']:+.2f}% |")
        if metrics.get('best_trade'):
            lines.append(f"| **Best Trade** | {metrics['best_trade']} |")
        if metrics.get('worst_trade'):
            lines.append(f"| **Worst Trade** | {metrics['worst_trade']} |")
        if metrics.get('avg_slippage_bps') is not None:
            lines.append(f"| **Avg Slippage** | {metrics['avg_slippage_bps']:+.2f} bps |")
        if metrics.get('avg_fill_ratio') is not None:
            lines.append(f"| **Avg Fill Ratio** | {metrics['avg_fill_ratio'] * 100:.1f}% |")
        if metrics.get('avg_time_to_fill_ms') is not None:
            lines.append(f"| **Avg Time to Fill** | {metrics['avg_time_to_fill_ms']:.1f} ms |")
        lines.append("")

        # Open positions table
        open_details = metrics.get('open_position_details', [])
        if open_details:
            lines.append(f"### 📈 Open Positions ({len(open_details)})")
            lines.append("| Ticker | Entry | Current | P&L | Days |")
            lines.append("|:---|:---|:---|:---|:---|")
            for pos in sorted(open_details, key=lambda x: x['pnl_pct'], reverse=True):
                days_held = (datetime.utcnow() - pos['entry_date']).days if pos.get('entry_date') else 0
                emoji = "🟢" if pos['pnl_pct'] > 0 else "🔴"
                lines.append(
                    f"| {emoji} {pos['ticker']} | {pos['entry']:.2f} | {pos['current']:.2f} | "
                    f"{pos['pnl_pct']:+.1f}% | {days_held}d |"
                )
            lines.append("")

        # Recent closed trades
        closed = self.db.get_closed_positions(strategy=self.strategy, limit=5)
        if closed:
            lines.append("### 🏁 Recent Closed Trades")
            lines.append("| Ticker | Entry | Exit | P&L | Reason | Days |")
            lines.append("|:---|:---|:---|:---|:---|:---|")
            for trade in closed:
                emoji = "💰" if trade['pnl_pct'] > 0 else "📉"
                lines.append(
                    f"| {emoji} {trade['ticker']} | {trade['entry_price']:.2f} | "
                    f"{trade['exit_price']:.2f} | {trade['pnl_pct']:+.1f}% | "
                    f"{trade['exit_reason']} | {trade['hold_days']}d |"
                )
            lines.append("")

        return "\n".join(lines)

    def reconcile_daily_positions(self) -> int:
        """Run daily reconciliation to update P&L from simulated fills."""
        updated = self.db.reconcile_positions_from_fills(strategy=self.strategy)
        if updated:
            logger.info("🔁 Reconciled %s position(s) from simulated fills (%s)", updated, self.strategy)
        return updated

    def _store_simulated_order(self, order) -> Optional[int]:
        """Persist a simulated order and return DB id."""
        order_data = {
            'ticker': order.ticker,
            'strategy': self.strategy,
            'side': order.side.value,
            'order_type': order.order_type.value,
            'status': order.status.value,
            'quantity': order.quantity,
            'filled_quantity': order.filled_quantity,
            'signal_price': order.signal_price,
            'limit_price': order.limit_price,
            'stop_price': order.stop_price,
            'avg_fill_price': order.average_fill_price,
            'slippage_bps': order.slippage_bps,
            'fill_ratio': order.fill_ratio,
            'time_to_fill_ms': order.time_to_fill_ms,
            'created_at': order.submitted_at,
        }
        fills = [
            {
                'quantity': fill.quantity,
                'fill_price': fill.price,
                'latency_ms': fill.latency_ms,
                'filled_at': fill.timestamp,
            }
            for fill in order.fills
        ]
        return self.db.record_simulated_order(order_data, fills)

    # ==================== PRIVATE HELPERS ====================

    def _batch_fetch_prices(self, tickers: List[str]) -> Dict[str, float]:
        """Batch fetch current prices via PriceService (yfinance-only)."""
        return self.price_service.get_batch_current_prices(tickers)


    def _compute_sharpe(self, returns: List[float], risk_free_rate: float = 0.05) -> Optional[float]:
        """Compute simplified Sharpe ratio from trade returns."""
        if len(returns) < 3:
            return None
        
        arr = np.array(returns)
        excess = arr - (risk_free_rate / 252)  # Daily risk-free rate
        
        if np.std(excess) == 0:
            return None
        
        return float(np.mean(excess) / np.std(excess) * np.sqrt(252))

    def _compute_max_drawdown(self, returns: List[float]) -> Optional[float]:
        """Compute max drawdown from sequential trade returns."""
        if len(returns) < 2:
            return None

        # Build equity curve from trade returns
        equity = [100]  # Start at 100
        for r in returns:
            equity.append(equity[-1] * (1 + r / 100))

        peak = equity[0]
        max_dd = 0
        for val in equity[1:]:
            if val > peak:
                peak = val
            dd = (peak - val) / peak * 100
            if dd > max_dd:
                max_dd = dd

        return max_dd

    def _compute_alpha(self, closed_positions: List[Dict]) -> float:
        """Compute alpha vs Nifty 50 from closed positions."""
        if not closed_positions:
            return 0.0

        alphas = []
        for pos in closed_positions:
            if (pos.get('benchmark_entry_price') and pos.get('benchmark_exit_price') 
                    and pos.get('pnl_pct') is not None
                    and pos['benchmark_entry_price'] > 0):
                bench_return = ((pos['benchmark_exit_price'] - pos['benchmark_entry_price']) 
                              / pos['benchmark_entry_price']) * 100
                alpha = pos['pnl_pct'] - bench_return
                alphas.append(alpha)

        return float(np.mean(alphas)) if alphas else 0.0

    def _compute_benchmark_return(self, closed_positions: List[Dict]) -> float:
        """Compute average Nifty 50 return over the same periods as our trades."""
        if not closed_positions:
            return 0.0

        bench_returns = []
        for pos in closed_positions:
            if (pos.get('benchmark_entry_price') and pos.get('benchmark_exit_price')
                    and pos['benchmark_entry_price'] > 0):
                bench_r = ((pos['benchmark_exit_price'] - pos['benchmark_entry_price'])
                         / pos['benchmark_entry_price']) * 100
                bench_returns.append(bench_r)

        return float(np.mean(bench_returns)) if bench_returns else 0.0
