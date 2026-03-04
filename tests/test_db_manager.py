from src.database.db_manager import DBManager


def test_subscriber_crud_in_memory_sqlite():
    db = DBManager("sqlite:///:memory:")

    assert db.add_subscriber("alice@example.com", "Alice") is True
    assert db.add_subscriber("alice@example.com", "Alice") is False
    assert db.get_active_subscribers() == ["alice@example.com"]

    assert db.unsubscribe("alice@example.com") is True
    assert db.get_active_subscribers() == []

    # Re-subscribe existing user should reactivate
    assert db.add_subscriber("alice@example.com", "Alice") is True
    assert db.get_active_subscribers() == ["alice@example.com"]


def test_position_open_close_workflow_in_memory_sqlite():
    db = DBManager("sqlite:///:memory:")

    assert db.open_position("RELIANCE", 100.0, strategy="DAILY") is True
    assert db.open_position("RELIANCE", 100.0, strategy="DAILY") is False

    open_positions = db.get_open_positions(strategy="DAILY")
    assert len(open_positions) == 1
    assert open_positions[0]["ticker"] == "RELIANCE"

    closed = db.close_position("RELIANCE", 110.0, exit_reason="SELL_SIGNAL", strategy="DAILY")
    assert closed is not None
    assert closed["ticker"] == "RELIANCE"
    assert closed["exit_price"] == 110.0
    assert round(closed["pnl_pct"], 4) == 10.0

    assert db.get_open_positions(strategy="DAILY") == []

    closed_positions = db.get_closed_positions(strategy="DAILY")
    assert len(closed_positions) == 1
    assert closed_positions[0]["ticker"] == "RELIANCE"
    assert round(closed_positions[0]["pnl_pct"], 4) == 10.0
