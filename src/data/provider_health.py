"""Shared in-run provider health registry.

Tracks provider/endpoint availability signals so downstream planners can
short-circuit known-bad providers before issuing additional API calls.
"""

from datetime import datetime
from threading import Lock
from typing import Dict, Optional


class ProviderHealthRegistry:
    """Simple process-local health registry for provider clients."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._providers: Dict[str, Dict] = {}

    def mark_unavailable(self, provider: str, endpoint: Optional[str] = None, reason: str = "") -> None:
        """Mark provider/endpoint as unavailable for the current run."""
        provider_key = (provider or "").strip().lower()
        if not provider_key:
            return
        endpoint_key = (endpoint or "").strip().lower() or None
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        with self._lock:
            entry = self._providers.setdefault(provider_key, {"available": True, "reason": "", "updated_at": now, "endpoints": {}})
            entry["available"] = False
            entry["reason"] = reason or entry.get("reason") or "provider_unavailable"
            entry["updated_at"] = now
            if endpoint_key:
                endpoints = entry.setdefault("endpoints", {})
                endpoints[endpoint_key] = {"available": False, "reason": reason or "endpoint_unavailable", "updated_at": now}

    def is_provider_available(self, provider: str) -> bool:
        """Return provider-level availability."""
        provider_key = (provider or "").strip().lower()
        with self._lock:
            if provider_key not in self._providers:
                return True
            return bool(self._providers[provider_key].get("available", True))

    def is_endpoint_available(self, provider: str, endpoint: str) -> bool:
        """Return endpoint availability for provider (defaults to available)."""
        provider_key = (provider or "").strip().lower()
        endpoint_key = (endpoint or "").strip().lower()
        if not provider_key or not endpoint_key:
            return True

        with self._lock:
            provider_entry = self._providers.get(provider_key, {})
            if provider_entry and not provider_entry.get("available", True):
                endpoint_entry = (provider_entry.get("endpoints") or {}).get(endpoint_key)
                if endpoint_entry is None:
                    return False
            endpoint_entry = (provider_entry.get("endpoints") or {}).get(endpoint_key)
            if not endpoint_entry:
                return True
            return bool(endpoint_entry.get("available", True))

    def snapshot(self) -> Dict[str, Dict]:
        """Return a shallow-copy snapshot for diagnostics/consumers."""
        with self._lock:
            return {
                name: {
                    "available": details.get("available", True),
                    "reason": details.get("reason", ""),
                    "updated_at": details.get("updated_at", ""),
                    "endpoints": dict(details.get("endpoints", {})),
                }
                for name, details in self._providers.items()
            }


provider_health = ProviderHealthRegistry()

