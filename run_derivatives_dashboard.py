#!/usr/bin/env python3
"""Run NSE derivatives endpoint checks and build dedicated F&O dashboard."""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

from src.reporting.derivatives_dashboard import NSEDerivativesDashboard

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def write_offline_artifact(output_dir: str, reason: str) -> None:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    md_path = out_dir / f"derivatives_dashboard_offline_{stamp}.md"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "offline",
        "reason": reason,
        "note": "NSE endpoint calls failed in this environment. Re-run in a network that can reach www.nseindia.com.",
    }
    md_path.write_text("# NSE F&O Dashboard (Offline Fallback)\n\n```json\n" + json.dumps(payload, indent=2) + "\n```\n", encoding="utf-8")
    logger.warning("Wrote offline fallback artifact: %s", md_path)


def run_endpoint_smoke_tests(dashboard: NSEDerivativesDashboard) -> None:
    """Smoke-test key NSE derivatives endpoints used by the dashboard."""
    index_names = dashboard.nse.fetch_index_names()
    logger.info("fetch_index_names: %s categories", len(index_names.keys()))

    underlyings = dashboard.nse.fetch_fno_underlying()
    logger.info("fetch_fno_underlying: IndexList=%s UnderlyingList=%s", len(underlyings.get("IndexList", [])), len(underlyings.get("UnderlyingList", [])))

    expiries = dashboard.nse.getFuturesExpiry("nifty")
    logger.info("getFuturesExpiry(nifty): %s", expiries[:3])

    option_chain = dashboard.nse.optionChain("nifty")
    expiry = option_chain.get("records", {}).get("expiryDates", [None])[0]
    logger.info("optionChain(nifty): nearest expiry=%s", expiry)


def main() -> None:
    parser = argparse.ArgumentParser(description="NSE derivatives dashboard generator")
    parser.add_argument("--symbols", type=str, default="nifty,banknifty,reliance", help="Comma-separated symbols")
    parser.add_argument("--output-dir", type=str, default="./data/derivatives", help="Output directory")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    try:
        dash = NSEDerivativesDashboard()
    except Exception as exc:
        logger.error("Unable to initialize NSE session: %s", exc)
        write_offline_artifact(args.output_dir, reason=str(exc))
        return

    try:
        run_endpoint_smoke_tests(dash)
        artifacts = dash.build_dashboard(symbols=symbols, output_dir=args.output_dir)
        logger.info("Generated dashboard markdown: %s", artifacts["markdown"])
        logger.info("Generated dashboard json: %s", artifacts["json"])
    except Exception as exc:
        logger.error("Derivatives dashboard run failed: %s", exc)
        write_offline_artifact(args.output_dir, reason=str(exc))
    finally:
        dash.close()


if __name__ == "__main__":
    main()
