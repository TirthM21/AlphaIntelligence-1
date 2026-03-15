from datetime import date, datetime

from src.api.service import APIService


class StubDBManager:
    def __init__(self):
        self.db_url = "sqlite:///:memory:"

    def get_recommendation_performance(self):
        return [
            {
                "ticker": "AAPL",
                "type": "BUY",
                "entry_price": 190.5,
                "benchmark_entry": 430.2,
                "date": datetime(2024, 1, 2, 14, 0, 0),
            },
            {
                "ticker": "MSFT",
                "type": "SELL",
                "entry_price": 390.1,
                "benchmark_entry": 430.2,
                "date": datetime(2024, 1, 2, 14, 0, 0),
            },
            {
                "ticker": "TSLA",
                "type": "BUY",
                "entry_price": 220.3,
                "benchmark_entry": 420.5,
                "date": datetime(2023, 12, 29, 12, 0, 0),
            },
        ]

    def get_open_positions(self, strategy="DAILY"):
        return [
            {
                "ticker": "AAPL",
                "entry_price": 180.0,
                "entry_date": datetime(2024, 1, 1, 9, 15, 0),
                "stop_loss": 170.0,
                "signal_score": 0.85,
                "strategy": strategy,
            }
        ]

    def get_performance_history(self, strategy="DAILY", limit=30):
        rows = [
            {
                "date": date(2024, 1, 2),
                "total_pnl_pct": 1.2,
                "open_positions": 2,
                "closed_positions": 8,
                "win_rate": 0.63,
                "avg_gain": 1.8,
                "avg_loss": -0.9,
                "sharpe_ratio": 1.1,
                "max_drawdown": -4.0,
                "alpha_vs_benchmark": 0.4,
                "benchmark_return": 0.8,
                "best_trade": "AAPL",
                "worst_trade": "NFLX",
            },
            {
                "date": date(2024, 1, 1),
                "total_pnl_pct": 0.7,
                "open_positions": 3,
                "closed_positions": 7,
                "win_rate": 0.57,
                "avg_gain": 1.3,
                "avg_loss": -0.8,
                "sharpe_ratio": 0.9,
                "max_drawdown": -4.2,
                "alpha_vs_benchmark": 0.2,
                "benchmark_return": 0.5,
                "best_trade": "MSFT",
                "worst_trade": "TSLA",
            },
        ]
        return rows[:limit]

    def get_execution_quality_metrics(self, strategy="DAILY", limit=500):
        return {
            "avg_slippage_bps": 3.4,
            "avg_fill_ratio": 0.98,
            "avg_time_to_fill_ms": 42.0,
        }


class EmptyDBManager(StubDBManager):
    def __init__(self):
        self.db_url = None

    def get_recommendation_performance(self):
        return []

    def get_open_positions(self, strategy="DAILY"):
        return []

    def get_performance_history(self, strategy="DAILY", limit=30):
        return []


def test_signals_contract_latest_and_history():
    service = APIService(db_manager=StubDBManager())

    status, latest_payload = service.handle_request("/signals/latest")
    assert status == 200
    assert latest_payload["count"] == 2
    assert latest_payload["as_of"] == "2024-01-02T14:00:00Z"
    assert set(latest_payload["signals"][0].keys()) == {
        "ticker",
        "signal_type",
        "entry_price",
        "benchmark_entry",
        "timestamp",
    }

    status, history_payload = service.handle_request("/signals/history", limit=2)
    assert status == 200
    assert history_payload["count"] == 2
    assert history_payload["limit"] == 2
    assert history_payload["signals"][0]["timestamp"] >= history_payload["signals"][1]["timestamp"]


def test_portfolio_contract_snapshot_and_performance():
    service = APIService(db_manager=StubDBManager())

    status, snapshot_payload = service.handle_request("/portfolio/current", strategy="daily")
    assert status == 200
    assert snapshot_payload["strategy"] == "DAILY"
    assert snapshot_payload["open_count"] == 1
    assert snapshot_payload["positions"][0]["entry_date"].endswith("Z")

    status, perf_payload = service.handle_request(
        "/portfolio/performance", strategy="QUARTERLY", limit=1
    )
    assert status == 200
    assert perf_payload["strategy"] == "QUARTERLY"
    assert perf_payload["count"] == 1
    assert perf_payload["history"][0]["date"] == "2024-01-02"
    assert "avg_slippage_bps" in perf_payload["execution_quality"]


def test_health_contracts_include_pipeline_and_provider_summary():
    service = APIService(db_manager=StubDBManager())

    status, providers_payload = service.handle_request("/health/providers")
    assert status == 200
    assert "providers" in providers_payload
    assert set(providers_payload["summary"].keys()) == {"total", "unavailable", "healthy"}

    status, pipeline_payload = service.handle_request("/health/pipeline")
    assert status == 200
    assert pipeline_payload["status"] in {"ok", "degraded"}
    assert set(pipeline_payload["stages"].keys()) == {"database", "signals", "performance"}


def test_crowwd_event_and_strategy_method_contracts():
    service = APIService(db_manager=StubDBManager())

    status, event_payload = service.handle_request("/events/crowwd/closing-bell", as_of="2026-03-30")
    assert status == 200
    assert event_payload["event"]["title"] == "Crowwd: The Closing Bell"
    assert event_payload["snapshot"]["phase"] == "fy-end-volatility"
    assert len(event_payload["timeline"]) == 4
    assert any("Pre-Placement Interview" in reward for reward in event_payload["rewards"])

    status, methods_payload = service.handle_request("/strategies/methods")
    assert status == 200
    assert set(methods_payload["tracks"].keys()) == {"value_investing", "algorithmic"}
    assert methods_payload["summary"]["track_count"] == 2
    assert methods_payload["summary"]["method_count"] >= 6

    status, playbook_payload = service.handle_request(
        "/events/crowwd/closing-bell/playbook",
        as_of="2026-03-30",
        risk_level="aggressive",
        style="momentum",
    )
    assert status == 200
    assert playbook_payload["playbook"]["phase"] == "fy-end-volatility"
    assert playbook_payload["playbook"]["positioning"]["max_positions"] == 16


def test_error_handling_contract_for_bad_inputs_and_unknown_route():
    service = APIService(db_manager=EmptyDBManager())

    status, not_found = service.handle_request("/unknown")
    assert status == 404
    assert not_found["error"]["code"] == "not_found"

    status, invalid_limit = service.handle_request("/signals/history", limit="abc")
    assert status == 400
    assert invalid_limit["error"]["code"] == "invalid_limit"

    status, invalid_strategy = service.handle_request(
        "/portfolio/current", strategy="intraday"
    )
    assert status == 400
    assert invalid_strategy["error"]["code"] == "invalid_strategy"

    status, empty_latest = service.handle_request("/signals/latest")
    assert status == 200
    assert empty_latest == {"signals": [], "as_of": None, "count": 0}

    status, pipeline = service.handle_request("/health/pipeline")
    assert status == 200
    assert pipeline["stages"]["database"]["status"] == "degraded"

    status, invalid_as_of = service.handle_request("/events/crowwd/closing-bell", as_of="03-30-2026")
    assert status == 400
    assert invalid_as_of["error"]["code"] == "invalid_as_of"

    status, invalid_as_of_playbook = service.handle_request(
        "/events/crowwd/closing-bell/playbook", as_of="03-30-2026"
    )
    assert status == 400
    assert invalid_as_of_playbook["error"]["code"] == "invalid_as_of"
