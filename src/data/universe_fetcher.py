"""Fetch and maintain the universe of Indian stocks listed on NSE.

This module fetches the complete list of NSE-listed stocks
and maintains a daily-updated universe for screening.
"""

import logging
import os
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd

from .nse_fetcher import NSEFetcher

logger = logging.getLogger(__name__)


class StockUniverseFetcher:
    """Fetches and maintains the universe of NSE-listed stocks."""

    def __init__(self, cache_dir: str = "./data/cache"):
        """Initialize the universe fetcher.

        Args:
            cache_dir: Directory for caching universe data
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / "nse_stock_universe.pkl"
        self.nse = NSEFetcher()
        logger.info("StockUniverseFetcher initialized (NSE India)")

    def fetch_universe(self, force_refresh: bool = False, include_etfs: bool = False) -> List[str]:
        """Fetch the complete universe of NSE-listed stocks.
        
        Args:
            force_refresh: Ignore cache and fetch fresh
            include_etfs: Whether to include ETFs in the universe
            
        Returns:
            List of stock ticker symbols (with .NS suffix for yfinance).
        """
        cache_key = "etf" if include_etfs else "equity"
        cache_file = self.cache_dir / f"nse_{cache_key}_universe.pkl"

        if not force_refresh and cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(
                cache_file.stat().st_mtime
            )

            if cache_age < timedelta(days=1):
                with open(cache_file, 'rb') as f:
                    cached_data = pickle.load(f)
                logger.info(f"Loaded {len(cached_data['symbols'])} NSE {cache_key} symbols from cache")
                return cached_data['symbols']

        if include_etfs:
            logger.info("Fetching NSE ETF list...")
            symbols = self.nse.get_etfs()
        else:
            logger.info("Fetching total equity market list from NSE CSV...")
            symbols = self.nse.get_all_equity_stocks()

        if not symbols:
            # Fallback to index if CSV/ETF fails
            logger.warning("Primary fetch failed, trying NIFTY 500 fallback...")
            symbols = self.nse.get_index_stocks('NIFTY 500')

        # Add .NS suffix for yfinance compatibility
        yf_symbols = [f"{s}.NS" for s in symbols]
        yf_symbols = sorted(list(set(yf_symbols)))

        # Cache the results
        cache_data = {
            'symbols': yf_symbols,
            'fetch_date': datetime.now().isoformat(),
            'count': len(yf_symbols)
        }

        with open(cache_file, 'wb') as f:
            pickle.dump(cache_data, f)

        logger.info(f"Cached {len(yf_symbols)} NSE symbols")
        return yf_symbols

    def get_universe_info(self) -> Dict:
        """Get information about the cached universe."""
        if not self.cache_file.exists():
            return {'cached': False, 'count': 0}

        with open(self.cache_file, 'rb') as f:
            cached_data = pickle.load(f)

        cache_age = datetime.now() - datetime.fromtimestamp(
            self.cache_file.stat().st_mtime
        )

        return {
            'cached': True,
            'count': cached_data['count'],
            'fetch_date': cached_data['fetch_date'],
            'cache_age_hours': cache_age.total_seconds() / 3600
        }
