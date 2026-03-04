from src.database.db_manager import DBManager
from src.execution.paper_broker import PaperBroker
from src.reporting.performance_tracker import PerformanceTracker


def test_paper_broker_market_order_fill_metrics():
    broker = PaperBroker(slippage_bps=10, latency_ms=100, latency_jitter_ms=0, partial_fill_ratio=1.0)
    order = broker.submit_order(
        ticker="RELIANCE",
        side="BUY",
        quantity=1,
        order_type="MARKET",
        signal_price=100.0,
        market_price=100.0,
    )

    assert order.status.value == "FILLED"
    assert order.average_fill_price is not None
    assert order.average_fill_price > 100.0
    assert order.fill_ratio == 1.0
    assert order.time_to_fill_ms == 100
    assert order.slippage_bps is not None


def test_db_reconciliation_updates_position_pnl_from_fills():
    db = DBManager("sqlite:///:memory:")

    entry_order_id = db.record_simulated_order(
        {
            "ticker": "RELIANCE",
            "strategy": "DAILY",
            "side": "BUY",
            "order_type": "MARKET",
            "status": "FILLED",
            "quantity": 1,
            "filled_quantity": 1,
            "signal_price": 100,
            "avg_fill_price": 101,
            "fill_ratio": 1.0,
            "slippage_bps": 100,
            "time_to_fill_ms": 100,
        },
        [{"quantity": 1, "fill_price": 101, "latency_ms": 100}],
    )
    assert entry_order_id is not None

    assert db.open_position("RELIANCE", 100.0, strategy="DAILY", entry_order_id=entry_order_id)

    exit_order_id = db.record_simulated_order(
        {
            "ticker": "RELIANCE",
            "strategy": "DAILY",
            "side": "SELL",
            "order_type": "MARKET",
            "status": "FILLED",
            "quantity": 1,
            "filled_quantity": 1,
            "signal_price": 110,
            "avg_fill_price": 109,
            "fill_ratio": 1.0,
            "slippage_bps": 90,
            "time_to_fill_ms": 120,
        },
        [{"quantity": 1, "fill_price": 109, "latency_ms": 120}],
    )
    assert exit_order_id is not None

    closed = db.close_position("RELIANCE", 110.0, strategy="DAILY", exit_order_id=exit_order_id)
    assert closed is not None

    updated = db.reconcile_positions_from_fills(strategy="DAILY")
    assert updated >= 1

    closed_positions = db.get_closed_positions(strategy="DAILY")
    assert closed_positions[0]["entry_price"] == 101.0
    assert closed_positions[0]["exit_price"] == 109.0
    assert round(closed_positions[0]["pnl_pct"], 6) == round(((109 - 101) / 101) * 100, 6)


def test_performance_tracker_includes_execution_quality_metrics(monkeypatch):
    tracker = PerformanceTracker(strategy="DAILY")
    tracker.db = DBManager("sqlite:///:memory:")

    monkeypatch.setattr(tracker, "_batch_fetch_prices", lambda tickers: {})

    tracker.db.record_simulated_order(
        {
            "ticker": "A",
            "strategy": "DAILY",
            "side": "BUY",
            "order_type": "MARKET",
            "status": "FILLED",
            "quantity": 1,
            "filled_quantity": 1,
            "signal_price": 100,
            "avg_fill_price": 100.1,
            "fill_ratio": 1.0,
            "slippage_bps": 10,
            "time_to_fill_ms": 80,
        },
        [{"quantity": 1, "fill_price": 100.1, "latency_ms": 80}],
    )

    metrics = tracker.compute_fund_metrics()
    assert "avg_slippage_bps" in metrics
    assert metrics["avg_slippage_bps"] == 10.0
    assert metrics["avg_fill_ratio"] == 1.0
    assert metrics["avg_time_to_fill_ms"] == 80.0
