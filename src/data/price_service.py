"""Price service for authoritative market prices via yfinance.

This module intentionally centralizes *all* price retrieval behind yfinance.
FMP/Finnhub data may still be used for news/fundamentals/events/sentiment, but
must not be treated as authoritative for price values.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


class PriceService:
    """Fetches current and historical prices from yfinance only."""

    _ALLOWED_PRICE_SOURCES = {"yfinance", "yf", "yahoo", "yahoo_finance"}
    _BLOCKED_PRICE_SOURCES = {"fmp", "financialmodelingprep", "finnhub"}

    def __init__(self, cache_ttl_seconds: int = 60):
        """Initialize short-lived cache and per-provider backoff state."""
        self.cache_ttl_seconds = max(30, min(int(cache_ttl_seconds or 60), 120))
        self._current_price_cache: Dict[str, Tuple[float, float]] = {}
        self._provider_backoff: Dict[str, Dict[str, float]] = {}

    def _normalize_ticker(self, ticker: str) -> str:
        return str(ticker or "").strip().upper()

    def _is_rate_limited(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        return "429" in msg or "too many requests" in msg or "rate limit" in msg

    def _is_provider_backoff_active(self, provider: str) -> bool:
        state = self._provider_backoff.get(provider) or {}
        return (state.get("until", 0.0) or 0.0) > time.time()

    def _mark_provider_success(self, provider: str) -> None:
        self._provider_backoff.pop(provider, None)

    def _mark_provider_backoff(self, provider: str) -> None:
        now = time.time()
        state = self._provider_backoff.get(provider, {"failures": 0, "until": 0.0})
        failures = int(state.get("failures", 0)) + 1
        backoff_seconds = min(120, 30 * (2 ** (failures - 1)))
        self._provider_backoff[provider] = {"failures": failures, "until": now + backoff_seconds}
        logger.info(
            "PriceService provider backoff activated: provider=%s failures=%s backoff=%ss",
            provider,
            failures,
            backoff_seconds,
        )

    def _get_cached_price(self, ticker: str, *, allow_stale: bool = False) -> Optional[float]:
        entry = self._current_price_cache.get(ticker)
        if not entry:
            return None
        price, fetched_at = entry
        age = time.time() - fetched_at
        if allow_stale or age <= self.cache_ttl_seconds:
            return float(price)
        return None

    def _set_cached_price(self, ticker: str, price: float) -> None:
        if price and price > 0:
            self._current_price_cache[ticker] = (float(price), time.time())

    def get_current_price(self, ticker: str) -> Optional[float]:
        """Return latest close/last price for ``ticker`` from yfinance."""
        normalized_ticker = self._normalize_ticker(ticker)
        if not normalized_ticker:
            return None

        cached = self._get_cached_price(normalized_ticker)
        if cached and cached > 0:
            return cached

        provider = "yfinance"
        if self._is_provider_backoff_active(provider):
            return self._get_cached_price(normalized_ticker, allow_stale=True)

        try:
            stock = yf.Ticker(normalized_ticker)
            fast_info = getattr(stock, "fast_info", None)
            if fast_info:
                last_price = fast_info.get("lastPrice") or fast_info.get("last_price")
                if last_price and float(last_price) > 0:
                    price = float(last_price)
                    self._set_cached_price(normalized_ticker, price)
                    self._mark_provider_success(provider)
                    return price

            hist = stock.history(period="2d")
            if hist is not None and not hist.empty:
                price = float(hist["Close"].dropna().iloc[-1])
                self._set_cached_price(normalized_ticker, price)
                self._mark_provider_success(provider)
                return price
        except Exception as exc:
            if self._is_rate_limited(exc):
                self._mark_provider_backoff(provider)
            else:
                logger.warning("PriceService failed to fetch current price for %s: %s", normalized_ticker, exc)

        return self._get_cached_price(normalized_ticker, allow_stale=True)

    def get_batch_current_prices(self, tickers: Iterable[str]) -> Dict[str, float]:
        """Return latest prices for tickers via yfinance batch call with fallback."""
        clean_tickers = [t for t in dict.fromkeys(tickers or []) if t]
        prices: Dict[str, float] = {}
        if not clean_tickers:
            return prices

        unresolved = []
        for ticker in clean_tickers:
            normalized = self._normalize_ticker(ticker)
            cached = self._get_cached_price(normalized)
            if cached and cached > 0:
                prices[normalized] = cached
            else:
                unresolved.append(normalized)

        if not unresolved:
            return prices

        provider = "yfinance"
        if self._is_provider_backoff_active(provider):
            for ticker in unresolved:
                cached = self._get_cached_price(ticker, allow_stale=True)
                if cached and cached > 0:
                    prices[ticker] = cached
            return prices

        try:
            data = yf.download(unresolved, period="2d", progress=False, threads=True)
            if data is not None and not data.empty and "Close" in data:
                if len(unresolved) == 1:
                    close = data["Close"]
                    if close is not None and not close.empty:
                        price = float(close.dropna().iloc[-1])
                        self._set_cached_price(unresolved[0], price)
                        prices[unresolved[0]] = price
                else:
                    for ticker in unresolved:
                        if ticker in data["Close"].columns:
                            close = data["Close"][ticker]
                            if close is not None and not close.empty:
                                price = float(close.dropna().iloc[-1])
                                self._set_cached_price(ticker, price)
                                prices[ticker] = price
                self._mark_provider_success(provider)
        except Exception as exc:
            if self._is_rate_limited(exc):
                self._mark_provider_backoff(provider)
            else:
                logger.warning("Batch yfinance download failed: %s", exc)

        # Fallback for misses
        for ticker in unresolved:
            if ticker not in prices:
                current = self.get_current_price(ticker)
                if current and current > 0:
                    prices[ticker] = current

        return prices

    def get_price_history(self, ticker: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
        """Return historical OHLCV prices for ``ticker`` from yfinance."""
        if not ticker:
            return pd.DataFrame()
        try:
            return yf.Ticker(ticker).history(period=period, interval=interval)
        except Exception as exc:
            logger.warning("PriceService failed historical fetch for %s: %s", ticker, exc)
            return pd.DataFrame()

    def validate_price_payload_source(self, payload: Optional[dict], *, context: str = "") -> Tuple[bool, Optional[str]]:
        """Validate that a payload's declared price source is yfinance-compatible.

        Returns:
            (is_valid, detected_source)
        """
        if not payload:
            return True, None

        candidate = self._extract_price_source(payload)
        if not candidate:
            return True, None

        normalized = candidate.strip().lower()
        if normalized in self._BLOCKED_PRICE_SOURCES:
            logger.error(
                "Rejected non-yfinance price payload%s: source=%s payload_keys=%s",
                f" ({context})" if context else "",
                normalized,
                sorted(payload.keys()),
            )
            return False, normalized

        if normalized in self._ALLOWED_PRICE_SOURCES:
            return True, normalized

        # Unknown source declaration is allowed; caller may still fetch from yfinance directly.
        return True, normalized

    def _extract_price_source(self, payload: dict) -> Optional[str]:
        for key in ("price_source", "current_price_source", "price_provider", "priceDataSource"):
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                return val

        for key in ("data_source", "source", "provider"):
            val = payload.get(key)
            if isinstance(val, str):
                lowered = val.strip().lower()
                if lowered in self._ALLOWED_PRICE_SOURCES or lowered in self._BLOCKED_PRICE_SOURCES:
                    return lowered

        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            return self._extract_price_source(metadata)

        return None
