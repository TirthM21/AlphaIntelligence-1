"""Enhanced fundamentals wrapper for Indian stocks using yfinance.

This module provides a unified interface for fetching quarterly fundamentals:
- Uses yfinance for all data fetching
- Caches results to minimize API calls
"""

import logging
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional
from .fundamentals_fetcher import (
    create_fundamental_snapshot, 
    analyze_fundamentals_for_signal,
    fetch_quarterly_financials
)

logger = logging.getLogger(__name__)

class EnhancedFundamentalsFetcher:
    """Fundamentals fetcher for Indian stocks using yfinance."""

    def __init__(self, cache_dir: str = "./data/fundamentals_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_expiry_days = 30 # Cache fundamentals for 30 days
        logger.info(f"EnhancedFundamentalsFetcher initialized (yfinance-only, cache_dir={cache_dir})")

    def _get_cache_path(self, ticker: str) -> Path:
        """Get cache file path."""
        return self.cache_dir / f"{ticker}_yfinance_fundamentals.json"

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache is valid."""
        if not cache_path.exists():
            return False
        
        file_modified = datetime.fromtimestamp(cache_path.stat().st_mtime)
        expiry_time = datetime.now() - timedelta(days=self.cache_expiry_days)
        return file_modified > expiry_time

    def is_fundamentals_cached(self, ticker: str) -> bool:
        """Check if fundamentals are cached."""
        return self._is_cache_valid(self._get_cache_path(ticker))

    def fetch_quarterly_data(
        self,
        ticker: str
    ) -> Dict[str, any]:
             
        cache_path = self._get_cache_path(ticker)
        if self._is_cache_valid(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    logger.debug(f"FUNDAMENTALS [{ticker}]: Loading from cache")
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load fundamental cache for {ticker}: {e}")

        logger.info(f"FUNDAMENTALS [{ticker}]: Fetching fresh from yfinance")
        data = fetch_quarterly_financials(ticker)
        
        if data:
            try:
                # Convert keys to strings (Timestamp objects cannot be JSON keys)
                serializable_data = self._make_json_serializable(data)
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump(serializable_data, f, indent=2, default=str)
            except Exception as e:
                logger.warning(f"Failed to save fundamental cache for {ticker}: {e}")
                
        return data

    def _make_json_serializable(self, data: any) -> any:
        """Recursively convert dictionary keys (like Timestamps) to strings and NaNs to None for JSON."""
        import math
        if isinstance(data, dict):
            return {str(k): self._make_json_serializable(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._make_json_serializable(item) for item in data]
        elif isinstance(data, float) and math.isnan(data):
            return None
        else:
            return data


    def create_snapshot(
        self,
        ticker: str,
        quarterly_data: Optional[Dict] = None
    ) -> str:
        """Create fundamental snapshot."""
        # Fetch data if not provided
        if not quarterly_data:
            quarterly_data = self.fetch_quarterly_data(ticker)

        return create_fundamental_snapshot(ticker, quarterly_data)

    def analyze_for_signal(
        self,
        ticker: str,
        quarterly_data: Optional[Dict] = None
    ) -> Dict[str, any]:
        """Analyze fundamentals for signal engine."""
        if not quarterly_data:
            quarterly_data = self.fetch_quarterly_data(ticker)

        return analyze_fundamentals_for_signal(quarterly_data)

    def get_api_usage(self) -> Dict[str, any]:
        """Get usage statistics."""
        return {
            'source': 'yfinance'
        }
