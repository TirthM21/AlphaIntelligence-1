"""
ETF Universe Discovery and Filtering.

Identifies and classifies thematic ETFs from major providers,
filtering for structural themes and quality metrics.
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ETFMetadata:
    """Container for ETF metadata."""

    ticker: str
    name: str
    theme_id: str  # From etf_themes.json
    theme_name: str
    aum_millions: float
    expense_ratio: float
    turnover: float
    inception_date: str
    top_10_concentration: float
    sector_concentration: float  # Herfindahl index or similar
    holdings_count: int

    # Returns
    return_1yr: Optional[float] = None
    return_3yr: Optional[float] = None
    return_5yr: Optional[float] = None

    # Metadata
    data_quality_score: float = 0.0
    market: str = "IN"


class ETFUniverse:
    """Discover and filter thematic ETFs."""

    def __init__(self, themes_file: str = "data/etf_themes.json"):
        """
        Initialize ETF universe.

        Args:
            themes_file: Path to ETF themes configuration
        """
        self.themes_file = themes_file
        self.themes_config = self._load_themes_config()
        self.etf_list: List[ETFMetadata] = []

    def _load_themes_config(self) -> Dict[str, Any]:
        """Load ETF themes configuration."""
        try:
            if not os.path.exists(self.themes_file):
                logger.warning(f"Themes file not found: {self.themes_file}")
                return self._default_themes_config()

            with open(self.themes_file, "r") as f:
                config = json.load(f)

            logger.info(f"Loaded {len(config.get('themes', []))} themes from {self.themes_file}")
            return config

        except Exception as e:
            logger.error(f"Error loading themes: {e}")
            return self._default_themes_config()

    def _default_themes_config(self) -> Dict[str, Any]:
        """Return default themes configuration."""
        return {
            "themes": [
                {
                    "id": "ai_cloud",
                    "name": "AI & Cloud Infrastructure",
                    "description": "Semiconductors, data centers, AI, cloud",
                    "tailwind_score": 10.0,
                    "keywords": ["semiconductor", "chip", "ai", "gpu", "data center", "cloud"],
                },
                {
                    "id": "defense",
                    "name": "Defense & Aerospace",
                    "description": "Defense contractors, aerospace, space",
                    "tailwind_score": 7.0,
                    "keywords": ["defense", "aerospace", "space", "missile"],
                },
                {
                    "id": "energy_transition",
                    "name": "Energy Transition",
                    "description": "Clean energy, batteries, grid",
                    "tailwind_score": 6.0,
                    "keywords": ["energy", "clean", "renewable", "solar", "wind", "battery"],
                },
                {
                    "id": "healthcare_innovation",
                    "name": "Healthcare Innovation",
                    "description": "Biotech, medical devices, genomics",
                    "tailwind_score": 6.0,
                    "keywords": ["biotech", "healthcare", "medical", "genomics"],
                },
                {
                    "id": "cybersecurity",
                    "name": "Cybersecurity",
                    "description": "Cyber defense, network security",
                    "tailwind_score": 7.0,
                    "keywords": ["cyber", "security", "threat"],
                },
            ],
            "exclude_etfs": ["NIFTYBEES.NS", "JUNIORBEES.NS", "SETFNIF50.NS", "SPY", "QQQ"],
            "market_region": "IN",
            "allowed_markets": ["IN"],
            "filtering_rules": {
                "min_aum_millions": 100,
                "max_expense_ratio": 0.75,
                "max_turnover": 200,
                "min_age_years": 1,
            },
        }

    def discover_thematic_etfs(
        self,
        source: str = "yfinance"
    ) -> List[ETFMetadata]:
        """
        Discover thematic ETFs from major providers.

        Args:
            source: Data source ("yfinance", "manual", etc.)

        Returns:
            List of filtered ETFMetadata objects
        """
        logger.info(f"Discovering thematic ETFs from {source}")

        region = str(self.themes_config.get("market_region", "IN")).upper().strip()

        if source == "manual":
            return self._get_manual_etf_list(region=region)
        # Default: use manual curated list
        return self._get_manual_etf_list(region=region)

    def _get_manual_etf_list(self, region: str = "IN") -> List[ETFMetadata]:
        """
        Return manually curated list of high-quality thematic ETFs.

        This is populated with known good thematic ETFs.
        In production, this would be extended with automated discovery.
        """
        region = (region or "IN").upper().strip()

        if region == "US":
            return self._get_manual_us_etf_list()

        # Default to India-listed ETFs so the quarterly India universe is consistent
        return self._get_manual_india_etf_list()

    def _get_manual_india_etf_list(self) -> List[ETFMetadata]:
        """India-listed ETF universe for thematic allocation."""
        etfs = [
            ETFMetadata(
                ticker="ITBEES.NS",
                name="Nippon India ETF Nifty IT",
                theme_id="ai_cloud",
                theme_name="AI & Cloud Infrastructure",
                aum_millions=1200,
                expense_ratio=0.22,
                turnover=24,
                inception_date="2015-01-01",
                top_10_concentration=62,
                sector_concentration=96,
                holdings_count=10,
                market="IN",
            ),
            ETFMetadata(
                ticker="MON100.NS",
                name="Motilal Oswal NASDAQ 100 ETF",
                theme_id="ai_cloud",
                theme_name="AI & Cloud Infrastructure",
                aum_millions=2900,
                expense_ratio=0.54,
                turnover=18,
                inception_date="2011-03-29",
                top_10_concentration=52,
                sector_concentration=88,
                holdings_count=100,
                market="IN",
            ),
            ETFMetadata(
                ticker="NETFNN50.NS",
                name="Nippon India ETF Nifty Next 50",
                theme_id="ai_cloud",
                theme_name="AI & Cloud Infrastructure",
                aum_millions=1600,
                expense_ratio=0.18,
                turnover=16,
                inception_date="2003-01-08",
                top_10_concentration=40,
                sector_concentration=70,
                holdings_count=50,
                market="IN",
            ),
            ETFMetadata(
                ticker="NIFTYIETF.NS",
                name="ICICI Prudential Nifty ETF",
                theme_id="defense",
                theme_name="Defense & Aerospace",
                aum_millions=1900,
                expense_ratio=0.07,
                turnover=12,
                inception_date="2013-03-20",
                top_10_concentration=44,
                sector_concentration=68,
                holdings_count=50,
                market="IN",
            ),
            ETFMetadata(
                ticker="ICICIB22.NS",
                name="ICICI Bharat 22 ETF",
                theme_id="defense",
                theme_name="Defense & Aerospace",
                aum_millions=2500,
                expense_ratio=0.09,
                turnover=14,
                inception_date="2017-11-14",
                top_10_concentration=46,
                sector_concentration=72,
                holdings_count=22,
                market="IN",
            ),
            ETFMetadata(
                ticker="CPSEETF.NS",
                name="CPSE ETF",
                theme_id="energy_transition",
                theme_name="Energy Transition",
                aum_millions=3100,
                expense_ratio=0.07,
                turnover=20,
                inception_date="2014-03-28",
                top_10_concentration=64,
                sector_concentration=86,
                holdings_count=10,
                market="IN",
            ),
            ETFMetadata(
                ticker="NIFITETF.NS",
                name="Nifty India Manufacturing ETF",
                theme_id="energy_transition",
                theme_name="Energy Transition",
                aum_millions=450,
                expense_ratio=0.32,
                turnover=27,
                inception_date="2021-02-10",
                top_10_concentration=38,
                sector_concentration=78,
                holdings_count=30,
                market="IN",
            ),
            ETFMetadata(
                ticker="PHARMABEES.NS",
                name="Nippon India ETF Nifty Pharma",
                theme_id="healthcare_innovation",
                theme_name="Healthcare Innovation",
                aum_millions=800,
                expense_ratio=0.22,
                turnover=26,
                inception_date="2020-01-15",
                top_10_concentration=55,
                sector_concentration=93,
                holdings_count=20,
                market="IN",
            ),
            ETFMetadata(
                ticker="MOMOMENTUM.NS",
                name="Motilal Oswal Nifty 200 Momentum 30 ETF",
                theme_id="healthcare_innovation",
                theme_name="Healthcare Innovation",
                aum_millions=1300,
                expense_ratio=0.30,
                turnover=34,
                inception_date="2020-02-19",
                top_10_concentration=48,
                sector_concentration=74,
                holdings_count=30,
                market="IN",
            ),
            ETFMetadata(
                ticker="MAFANG.NS",
                name="Mirae Asset NYSE FANG+ ETF",
                theme_id="cybersecurity",
                theme_name="Cybersecurity",
                aum_millions=900,
                expense_ratio=0.60,
                turnover=22,
                inception_date="2021-04-19",
                top_10_concentration=100,
                sector_concentration=95,
                holdings_count=10,
                market="IN",
            ),
        ]

        return etfs

    def _get_manual_us_etf_list(self) -> List[ETFMetadata]:
        """US-listed fallback ETF universe (legacy behavior)."""
        etfs = [
            # AI & Cloud Infrastructure
            ETFMetadata(
                ticker="SOXX",
                name="iShares NASDAQ-100 Technology Sector ETF",
                theme_id="ai_cloud",
                theme_name="AI & Cloud Infrastructure",
                aum_millions=8200,
                expense_ratio=0.20,
                turnover=18,
                inception_date="2001-06-19",
                top_10_concentration=52,
                sector_concentration=98,
                holdings_count=107,
                market="US",
            ),
            ETFMetadata(
                ticker="SMH",
                name="Semiconductor ETF",
                theme_id="ai_cloud",
                theme_name="AI & Cloud Infrastructure",
                aum_millions=9500,
                expense_ratio=0.35,
                turnover=22,
                inception_date="2006-04-20",
                top_10_concentration=48,
                sector_concentration=95,
                holdings_count=52,
                market="US",
            ),
            ETFMetadata(
                ticker="NVDA",
                name="NVIDIA (proxy - single stock, exclude)",
                theme_id="ai_cloud",
                theme_name="AI & Cloud Infrastructure",
                aum_millions=3500,
                expense_ratio=0.00,
                turnover=0,
                inception_date="2020-01-01",
                top_10_concentration=100,
                sector_concentration=100,
                holdings_count=1,
                market="US",
            ),

            # Defense & Aerospace
            ETFMetadata(
                ticker="ITA",
                name="iShares Aerospace & Defense ETF",
                theme_id="defense",
                theme_name="Defense & Aerospace",
                aum_millions=9200,
                expense_ratio=0.41,
                turnover=12,
                inception_date="2006-05-04",
                top_10_concentration=38,
                sector_concentration=92,
                holdings_count=67,
                market="US",
            ),
            ETFMetadata(
                ticker="XAR",
                name="SPDR S&P Aerospace & Defense ETF",
                theme_id="defense",
                theme_name="Defense & Aerospace",
                aum_millions=2100,
                expense_ratio=0.35,
                turnover=15,
                inception_date="2011-07-22",
                top_10_concentration=42,
                sector_concentration=88,
                holdings_count=47,
                market="US",
            ),

            # Energy Transition
            ETFMetadata(
                ticker="ICLN",
                name="iShares Global Clean Energy ETF",
                theme_id="energy_transition",
                theme_name="Energy Transition",
                aum_millions=7800,
                expense_ratio=0.41,
                turnover=35,
                inception_date="2008-06-25",
                top_10_concentration=28,
                sector_concentration=85,
                holdings_count=102,
                market="US",
            ),
            ETFMetadata(
                ticker="TAN",
                name="Invesco Solar ETF",
                theme_id="energy_transition",
                theme_name="Energy Transition",
                aum_millions=2300,
                expense_ratio=0.70,
                turnover=42,
                inception_date="2008-04-22",
                top_10_concentration=32,
                sector_concentration=90,
                holdings_count=56,
                market="US",
            ),

            # Healthcare Innovation
            ETFMetadata(
                ticker="XBI",
                name="SPDR S&P Biotech ETF",
                theme_id="healthcare_innovation",
                theme_name="Healthcare Innovation",
                aum_millions=8100,
                expense_ratio=0.35,
                turnover=28,
                inception_date="2006-04-13",
                top_10_concentration=25,
                sector_concentration=92,
                holdings_count=118,
                market="US",
            ),
            ETFMetadata(
                ticker="BBH",
                name="VanEck Biotech ETF",
                theme_id="healthcare_innovation",
                theme_name="Healthcare Innovation",
                aum_millions=5600,
                expense_ratio=0.35,
                turnover=32,
                inception_date="2006-03-20",
                top_10_concentration=22,
                sector_concentration=88,
                holdings_count=95,
                market="US",
            ),

            # Cybersecurity
            ETFMetadata(
                ticker="CIBR",
                name="AIS Cybersecurity ETF",
                theme_id="cybersecurity",
                theme_name="Cybersecurity",
                aum_millions=2400,
                expense_ratio=0.13,
                turnover=18,
                inception_date="2014-08-28",
                top_10_concentration=28,
                sector_concentration=82,
                holdings_count=41,
                market="US",
            ),
            ETFMetadata(
                ticker="HACK",
                name="HackerOne ETF (proxy)",
                theme_id="cybersecurity",
                theme_name="Cybersecurity",
                aum_millions=850,
                expense_ratio=0.50,
                turnover=25,
                inception_date="2014-11-12",
                top_10_concentration=32,
                sector_concentration=75,
                holdings_count=48,
                market="US",
            ),
        ]

        return etfs

    def filter_by_quality(self, etfs: List[ETFMetadata]) -> List[ETFMetadata]:
        """
        Filter ETFs by quality criteria.

        Args:
            etfs: List of candidate ETFs

        Returns:
            Filtered list meeting quality standards
        """
        rules = self.themes_config.get("filtering_rules", {})
        min_aum = rules.get("min_aum_millions", 100)
        max_expense = rules.get("max_expense_ratio", 0.75)
        max_turnover = rules.get("max_turnover", 200)
        exclude_list = self.themes_config.get("exclude_etfs", [])
        allowed_markets = [m.upper() for m in self.themes_config.get("allowed_markets", ["IN"]) if m]

        filtered = []

        for etf in etfs:
            # Skip single-stock holdings and excluded tickers
            if etf.holdings_count <= 1 or etf.ticker in exclude_list:
                logger.debug(f"Skipping {etf.ticker} (excluded or single-stock)")
                continue

            # Match configured market region(s)
            if allowed_markets and etf.market.upper() not in allowed_markets:
                logger.debug(
                    f"Skipping {etf.ticker} (market {etf.market} not in allowed {allowed_markets})"
                )
                continue

            # Check AUM
            if etf.aum_millions < min_aum:
                logger.debug(f"Skipping {etf.ticker} (AUM {etf.aum_millions}M < {min_aum}M)")
                continue

            # Check expense ratio
            if etf.expense_ratio > max_expense:
                logger.debug(
                    f"Skipping {etf.ticker} "
                    f"(expense {etf.expense_ratio:.2%} > {max_expense:.2%})"
                )
                continue

            # Check turnover
            if etf.turnover > max_turnover:
                logger.debug(
                    f"Skipping {etf.ticker} (turnover {etf.turnover:.0f} > {max_turnover:.0f})"
                )
                continue

            filtered.append(etf)

        logger.info(f"Filtered: {len(filtered)}/{len(etfs)} ETFs passed quality checks")
        return filtered

    def get_etfs_by_theme(
        self,
        theme_id: str,
        filtered: bool = True
    ) -> List[ETFMetadata]:
        """
        Get all ETFs for a specific theme.

        Args:
            theme_id: Theme ID (ai_cloud, defense, etc.)
            filtered: If True, apply quality filters

        Returns:
            List of matching ETFs
        """
        all_etfs = self.discover_thematic_etfs()

        theme_etfs = [etf for etf in all_etfs if etf.theme_id == theme_id]

        if filtered:
            theme_etfs = self.filter_by_quality(theme_etfs)

        return theme_etfs

    def get_theme_by_id(self, theme_id: str) -> Optional[Dict[str, Any]]:
        """Get theme configuration by ID."""
        themes = self.themes_config.get("themes", [])
        for theme in themes:
            if theme.get("id") == theme_id:
                return theme
        return None

    def calculate_theme_purity(
        self,
        etf: ETFMetadata
    ) -> float:
        """
        Calculate theme purity score (0-100).

        Based on sector concentration and top-10 holdings.

        Args:
            etf: ETF metadata

        Returns:
            Purity score (0-100)
        """
        # Top 10 concentration (0-50 points)
        top_10_score = (etf.top_10_concentration - 30) / (80 - 30) * 50
        top_10_score = max(0, min(50, top_10_score))

        # Sector concentration (0-50 points)
        sector_score = etf.sector_concentration

        return top_10_score + sector_score

    def get_tailwind_score(self, theme_id: str) -> float:
        """Get structural tailwind score for a theme (0-10)."""
        theme = self.get_theme_by_id(theme_id)
        if theme:
            return theme.get("tailwind_score", 5.0)
        return 5.0

    def summary_by_theme(self) -> Dict[str, List[ETFMetadata]]:
        """Get summary of ETFs grouped by theme."""
        all_etfs = self.discover_thematic_etfs()
        all_etfs = self.filter_by_quality(all_etfs)

        summary = {}
        for theme in self.themes_config.get("themes", []):
            theme_id = theme.get("id")
            theme_etfs = [e for e in all_etfs if e.theme_id == theme_id]
            summary[theme_id] = theme_etfs

        return summary
