"""Service module exposing API-like endpoints for core platform telemetry.

The module intentionally keeps transport concerns out so it can be mounted in
any web framework (FastAPI/Flask/etc.) while providing stable response
contracts for tests and callers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from src.data.provider_health import provider_health
from src.database.db_manager import DBManager
from src.research.crowwd_closing_bell import (
    ClosingBellConfig,
    build_timeline,
    competitor_playbook,
    rewards_catalogue,
    simulation_snapshot,
)
from src.strategies.method_catalog import get_strategy_method_catalogue


class APIServiceError(Exception):
    """Error raised for API contract/validation failures."""

    def __init__(self, message: str, status_code: int = 400, code: str = "bad_request"):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code


class APIService:
    """Data service exposing endpoint-style methods backed by DBManager."""

    def __init__(self, db_manager: Optional[DBManager] = None):
        self.db = db_manager or DBManager()

    def handle_request(self, path: str, **params: Any) -> Tuple[int, Dict[str, Any]]:
        """Dispatch an endpoint path to a service method.

        Returns:
            tuple(status_code, payload)
        """
        routes = {
            "/signals/latest": self.get_signals_latest,
            "/signals/history": self.get_signals_history,
            "/portfolio/current": self.get_portfolio_current,
            "/portfolio/performance": self.get_portfolio_performance,
            "/health/providers": self.get_health_providers,
            "/health/pipeline": self.get_health_pipeline,
            "/events/crowwd/closing-bell": self.get_crowwd_closing_bell,
            "/strategies/methods": self.get_strategy_methods,
            "/events/crowwd/closing-bell/playbook": self.get_crowwd_playbook,
        }

        handler = routes.get(path)
        if handler is None:
            return self._error_response(
                APIServiceError(
                    f"Unknown endpoint: {path}", status_code=404, code="not_found"
                )
            )

        try:
            return 200, handler(**params)
        except APIServiceError as err:
            return self._error_response(err)

    def get_signals_latest(self) -> Dict[str, Any]:
        """Return the newest signal batch grouped by latest timestamp."""
        records = self.db.get_recommendation_performance()
        if not records:
            return {"signals": [], "as_of": None, "count": 0}

        latest_ts = max(r["date"] for r in records if r.get("date") is not None)
        latest_records = [r for r in records if r.get("date") == latest_ts]
        latest_records.sort(key=lambda row: row.get("ticker") or "")

        return {
            "signals": [self._serialize_signal(row) for row in latest_records],
            "as_of": self._serialize_datetime(latest_ts),
            "count": len(latest_records),
        }

    def get_signals_history(self, limit: int = 100) -> Dict[str, Any]:
        """Return historical signals newest-first with a bounded limit."""
        limit = self._validate_limit(limit)
        records = self.db.get_recommendation_performance()

        ordered = sorted(
            records,
            key=lambda row: row.get("date") or datetime.min,
            reverse=True,
        )[:limit]

        return {
            "signals": [self._serialize_signal(row) for row in ordered],
            "count": len(ordered),
            "limit": limit,
        }

    def get_portfolio_current(self, strategy: str = "DAILY") -> Dict[str, Any]:
        """Return a point-in-time open portfolio snapshot."""
        strategy = self._validate_strategy(strategy)
        open_positions = self.db.get_open_positions(strategy=strategy)

        return {
            "strategy": strategy,
            "positions": [self._serialize_position(p) for p in open_positions],
            "open_count": len(open_positions),
        }

    def get_portfolio_performance(
        self,
        strategy: str = "DAILY",
        limit: int = 30,
    ) -> Dict[str, Any]:
        """Return portfolio performance time series and execution metrics."""
        strategy = self._validate_strategy(strategy)
        limit = self._validate_limit(limit)
        history = self.db.get_performance_history(strategy=strategy, limit=limit)
        execution = self.db.get_execution_quality_metrics(strategy=strategy)

        return {
            "strategy": strategy,
            "history": [self._serialize_performance_row(row) for row in history],
            "execution_quality": execution,
            "count": len(history),
            "limit": limit,
        }

    def get_health_providers(self) -> Dict[str, Any]:
        """Return current provider/source health state from registry."""
        providers = provider_health.snapshot()
        unavailable = sum(1 for details in providers.values() if not details.get("available", True))

        return {
            "providers": providers,
            "summary": {
                "total": len(providers),
                "unavailable": unavailable,
                "healthy": len(providers) - unavailable,
            },
        }

    def get_health_pipeline(self) -> Dict[str, Any]:
        """Return health for major data pipeline stages."""
        recommendations = self.db.get_recommendation_performance()
        latest_signal_ts = max(
            (row.get("date") for row in recommendations if row.get("date") is not None),
            default=None,
        )
        perf_history = self.db.get_performance_history(limit=1)
        latest_perf = perf_history[0]["date"] if perf_history else None

        stages = {
            "database": {
                "status": "ok" if bool(getattr(self.db, "db_url", None)) else "degraded",
                "details": "connected" if bool(getattr(self.db, "db_url", None)) else "missing DATABASE_URL",
            },
            "signals": {
                "status": "ok" if latest_signal_ts else "degraded",
                "last_update": self._serialize_datetime(latest_signal_ts),
            },
            "performance": {
                "status": "ok" if latest_perf else "degraded",
                "last_update": self._serialize_date(latest_perf),
            },
        }
        overall_status = "ok" if all(s["status"] == "ok" for s in stages.values()) else "degraded"

        return {"status": overall_status, "stages": stages}

    def get_crowwd_closing_bell(self, as_of: Optional[str] = None) -> Dict[str, Any]:
        """Return Crowwd Closing Bell event metadata, timeline, and progress snapshot."""
        config = ClosingBellConfig()
        if as_of:
            try:
                parsed_date = datetime.fromisoformat(as_of).date()
            except ValueError:
                raise APIServiceError("as_of must be ISO date (YYYY-MM-DD)", code="invalid_as_of")
        else:
            parsed_date = datetime.utcnow().date()

        timeline = [
            {"name": milestone.name, "date": milestone.day.isoformat(), "description": milestone.description}
            for milestone in build_timeline(config)
        ]

        return {
            "event": {
                "title": config.title,
                "host": config.host,
                "tagline": config.tagline,
                "format": config.format,
                "virtual_capital_inr": config.virtual_capital_inr,
                "universe": config.universe,
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
            },
            "snapshot": simulation_snapshot(as_of=parsed_date, config=config),
            "timeline": timeline,
            "rewards": rewards_catalogue(),
        }


    def get_crowwd_playbook(
        self,
        as_of: Optional[str] = None,
        risk_level: str = "balanced",
        style: str = "hybrid",
    ) -> Dict[str, Any]:
        """Return participant-focused winning playbook for the Crowwd competition."""
        if as_of:
            try:
                parsed_date = datetime.fromisoformat(as_of).date()
            except ValueError:
                raise APIServiceError("as_of must be ISO date (YYYY-MM-DD)", code="invalid_as_of")
        else:
            parsed_date = datetime.utcnow().date()

        return {
            "as_of": parsed_date.isoformat(),
            "playbook": competitor_playbook(
                as_of=parsed_date,
                risk_level=risk_level,
                style=style,
                config=ClosingBellConfig(),
            ),
        }
    def get_strategy_methods(self) -> Dict[str, Any]:
        """Return strategy method catalogue (value-investing and algorithmic tracks)."""
        catalogue = get_strategy_method_catalogue()
        method_count = sum(len(methods) for methods in catalogue.values())
        return {
            "tracks": catalogue,
            "summary": {
                "track_count": len(catalogue),
                "method_count": method_count,
            },
        }

    def _error_response(self, error: APIServiceError) -> Tuple[int, Dict[str, Any]]:
        return error.status_code, {
            "error": {
                "code": error.code,
                "message": error.message,
            }
        }

    def _validate_strategy(self, strategy: str) -> str:
        normalized = (strategy or "").strip().upper()
        if normalized not in {"DAILY", "QUARTERLY"}:
            raise APIServiceError(
                "strategy must be DAILY or QUARTERLY", status_code=400, code="invalid_strategy"
            )
        return normalized

    def _validate_limit(self, limit: Any) -> int:
        try:
            limit_value = int(limit)
        except (TypeError, ValueError):
            raise APIServiceError("limit must be an integer", status_code=400, code="invalid_limit")

        if limit_value <= 0:
            raise APIServiceError("limit must be positive", status_code=400, code="invalid_limit")

        return min(limit_value, 1000)

    def _serialize_signal(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ticker": row.get("ticker"),
            "signal_type": row.get("type"),
            "entry_price": row.get("entry_price"),
            "benchmark_entry": row.get("benchmark_entry"),
            "timestamp": self._serialize_datetime(row.get("date")),
        }

    def _serialize_position(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ticker": row.get("ticker"),
            "entry_price": row.get("entry_price"),
            "entry_date": self._serialize_datetime(row.get("entry_date")),
            "stop_loss": row.get("stop_loss"),
            "signal_score": row.get("signal_score"),
            "strategy": row.get("strategy"),
        }

    def _serialize_performance_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        serialized = dict(row)
        serialized["date"] = self._serialize_date(row.get("date"))
        return serialized

    def _serialize_datetime(self, value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return value.replace(microsecond=0).isoformat() + "Z"

    def _serialize_date(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)
