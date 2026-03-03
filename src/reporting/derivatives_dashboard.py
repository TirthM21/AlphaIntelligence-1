"""Derivatives (Futures & Options) tracking dashboard for NSE instruments."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from nse import NSE

logger = logging.getLogger(__name__)


@dataclass
class DerivativeSnapshot:
    symbol: str
    generated_at: str
    expiry: Optional[str]
    underlying_value: Optional[float]
    atm_strike: Optional[float]
    pcr: Optional[float]
    total_ce_oi: Optional[float]
    total_pe_oi: Optional[float]
    max_pain: Optional[float]
    max_ce_oi_strike: Optional[float]
    max_pe_oi_strike: Optional[float]
    top_calls: List[Dict]
    top_puts: List[Dict]
    recent_futures_rows: int


class NSEDerivativesDashboard:
    """Collect and render futures/options analytics from NSE APIs."""

    def __init__(self, download_folder: str = "./data/nse_downloads", server: bool = False, timeout: int = 15):
        self.download_folder = Path(download_folder)
        self.download_folder.mkdir(parents=True, exist_ok=True)
        self.nse = NSE(download_folder=str(self.download_folder), server=server, timeout=timeout)

    def close(self) -> None:
        """Close NSE session."""
        self.nse.exit()

    def _safe_float(self, value) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    def fetch_symbol_snapshot(self, symbol: str) -> DerivativeSnapshot:
        """Fetch option chain + recent futures info for one symbol/index."""
        option_chain = self.nse.optionChain(symbol)
        expiry = option_chain.get("records", {}).get("expiryDates", [None])[0]

        summary = self.nse.compileOptionChain(symbol, datetime.strptime(expiry, "%d-%b-%Y")) if expiry else {}

        ce_rows = summary.get("maxCalls", [])[:5] if isinstance(summary, dict) else []
        pe_rows = summary.get("maxPuts", [])[:5] if isinstance(summary, dict) else []

        to_date = date.today()
        from_date = to_date - timedelta(days=30)
        futures_rows = 0
        try:
            futures_hist = self.nse.fetch_historical_fno_data(
                symbol=symbol.upper(),
                instrument="FUTIDX" if symbol.lower() in {"nifty", "banknifty", "finnifty", "niftyit"} else "FUTSTK",
                from_date=from_date,
                to_date=to_date,
            )
            futures_rows = len(futures_hist)
        except Exception as exc:
            logger.warning("Unable to fetch futures history for %s: %s", symbol, exc)

        return DerivativeSnapshot(
            symbol=symbol.upper(),
            generated_at=datetime.now().isoformat(timespec="seconds"),
            expiry=expiry,
            underlying_value=self._safe_float(summary.get("underlyingValue")) if isinstance(summary, dict) else None,
            atm_strike=self._safe_float(summary.get("atmStrike")) if isinstance(summary, dict) else None,
            pcr=self._safe_float(summary.get("pcr")) if isinstance(summary, dict) else None,
            total_ce_oi=self._safe_float(summary.get("totalCE")) if isinstance(summary, dict) else None,
            total_pe_oi=self._safe_float(summary.get("totalPE")) if isinstance(summary, dict) else None,
            max_pain=self._safe_float(summary.get("maxPain")) if isinstance(summary, dict) else None,
            max_ce_oi_strike=self._safe_float(summary.get("maxCallOI")) if isinstance(summary, dict) else None,
            max_pe_oi_strike=self._safe_float(summary.get("maxPutOI")) if isinstance(summary, dict) else None,
            top_calls=ce_rows,
            top_puts=pe_rows,
            recent_futures_rows=futures_rows,
        )

    def build_dashboard(self, symbols: Optional[List[str]] = None, output_dir: str = "./data/derivatives") -> Dict[str, str]:
        """Build markdown + json dashboard artifacts."""
        symbols = symbols or ["nifty", "banknifty", "reliance"]
        snapshots = [self.fetch_symbol_snapshot(symbol) for symbol in symbols]

        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        json_path = out_dir / f"derivatives_dashboard_{stamp}.json"
        md_path = out_dir / f"derivatives_dashboard_{stamp}.md"

        payload = [asdict(s) for s in snapshots]
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        lines = [
            "# NSE Futures & Options Tracking Dashboard",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "This dashboard focuses on options positioning (PCR, max pain, OI concentration) and recent futures data availability.",
            "",
        ]

        for snap in snapshots:
            lines.extend(
                [
                    f"## {snap.symbol}",
                    f"- Expiry: `{snap.expiry}`",
                    f"- Underlying: `{snap.underlying_value}`",
                    f"- ATM Strike: `{snap.atm_strike}`",
                    f"- PCR: `{snap.pcr}`",
                    f"- Max Pain: `{snap.max_pain}`",
                    f"- Max CE OI Strike: `{snap.max_ce_oi_strike}`",
                    f"- Max PE OI Strike: `{snap.max_pe_oi_strike}`",
                    f"- Recent futures rows (30d fetch): `{snap.recent_futures_rows}`",
                    "",
                    "Top Calls (OI concentration):",
                ]
            )
            lines.extend([f"- `{row}`" for row in snap.top_calls] or ["- _No call rows available_"])
            lines.append("")
            lines.append("Top Puts (OI concentration):")
            lines.extend([f"- `{row}`" for row in snap.top_puts] or ["- _No put rows available_"])
            lines.append("")

        md_path.write_text("\n".join(lines), encoding="utf-8")

        return {"markdown": str(md_path), "json": str(json_path)}
