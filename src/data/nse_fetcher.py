"""NSE (National Stock Exchange of India) data fetcher.

This module provides an interface to fetch Indian stock data using the nse library.
"""

import logging
from typing import Dict, List, Optional
import pandas as pd
from nse import NSE

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NSEFetcher:
    """Fetches data from the National Stock Exchange of India (NSE)."""

    def __init__(self, download_folder: str = "./data/nse_downloads"):
        """Initialize the NSE fetcher.
        
        Args:
            download_folder: Directory to store downloaded NSE reports.
        """
        self.nse = NSE(download_folder=download_folder, server=False)
        logger.info("NSEFetcher initialized")

    def get_all_equity_stocks(self) -> List[str]:
        """Fetch list of all equity stocks listed on NSE using the official CSV."""
        try:
            nse_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
            df = pd.read_csv(nse_url)
            if "SYMBOL" in df.columns:
                return df["SYMBOL"].tolist()
            return []
        except Exception as e:
            logger.error(f"Error fetching NSE equity CSV: {e}")
            return []

    def get_etfs(self) -> List[str]:
        """Fetch list of all ETFs listed on NSE."""
        try:
            response = self.nse.listEtf()
            if isinstance(response, dict) and 'data' in response:
                return [item['symbol'] for item in response['data'] if 'symbol' in item]
            return []
        except Exception as e:
            logger.error(f"Error fetching NSE ETFs: {e}")
            return []

    def get_quote(self, symbol: str) -> Dict:
        """Fetch real-time quote for a symbol.
        
        Args:
            symbol: NSE stock symbol (e.g., 'RELIANCE').
            
        Returns:
            Dictionary with quote data.
        """
        try:
            return self.nse.equityQuote(symbol)
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return {}

    def get_indices(self) -> List[str]:
        """Fetch list of available NSE indices."""
        try:
            return self.nse.listIndices()
        except Exception as e:
            logger.error(f"Error fetching indices: {e}")
            return []

    def get_index_stocks(self, index_name: str) -> List[str]:
        """Fetch stocks belonging to a specific index.
        
        Args:
            index_name: Name of the index (e.g., 'NIFTY 50').
        """
        try:
            response = self.nse.listEquityStocksByIndex(index_name)
            if isinstance(response, dict) and 'data' in response:
                stocks = response['data']
                return [stock['symbol'] for stock in stocks if isinstance(stock, dict) and 'symbol' in stock]
            return []
        except Exception as e:
            logger.error(f"Error fetching stocks for index {index_name}: {e}")
            return []
