#!/usr/bin/env python3
"""Run standalone technical-signal category scan (separate from daily/quarterly scans)."""

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import yfinance as yf

import pickle

from src.data.universe_fetcher import StockUniverseFetcher
from src.notifications.email_notifier import EmailNotifier
from src.screening.signal_engine import analyze_technical_signals

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CATEGORIES = {
    "buy_signals": "Buy Signals",
    "sell_signals": "Sell Signals",
    "momentum_factors": "Momentum",
    "chart_patterns": "Patterns",
}



def _configure_runtime_environment() -> None:
    """Reduce noisy logs and configure yfinance timezone cache location."""
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("src.screening.indicators").setLevel(logging.ERROR)

    tz_cache_dir = Path("./data/cache/py-yfinance-tz")
    tz_cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        yf.set_tz_cache_location(str(tz_cache_dir.resolve()))
    except Exception as exc:
        logger.debug("Unable to set yfinance timezone cache location: %s", exc)


def _load_tickers_with_fallback(include_etfs: bool) -> List[str]:
    """Load tickers from live universe, then cache, then static fallback."""
    try:
        return StockUniverseFetcher().fetch_universe(include_etfs=include_etfs)
    except Exception as exc:
        logger.warning("Live universe fetch failed: %s", exc)

    cache_key = "etf" if include_etfs else "equity"
    cache_file = Path("./data/cache") / f"nse_{cache_key}_universe.pkl"
    if cache_file.exists():
        try:
            payload = pickle.loads(cache_file.read_bytes())
            symbols = payload.get("symbols") if isinstance(payload, dict) else None
            if symbols:
                logger.info("Using cached %s universe with %d symbols", cache_key, len(symbols))
                return list(symbols)
        except Exception as exc:
            logger.warning("Failed reading cached universe: %s", exc)

    fallback = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS"]
    logger.warning("Using static fallback universe (%d symbols)", len(fallback))
    return fallback

def _scan_ticker(ticker: str) -> Dict:
    try:
        hist = yf.Ticker(ticker).history(period="1y", auto_adjust=False)
        if hist is None or hist.empty or len(hist) < 90:
            return {"ticker": ticker, "signals": {}, "error": "insufficient_data"}

        frame = pd.DataFrame(
            {
                "Open": hist["Open"],
                "High": hist["High"],
                "Low": hist["Low"],
                "Close": hist["Close"],
                "Volume": hist["Volume"],
            }
        ).dropna()
        if len(frame) < 90:
            return {"ticker": ticker, "signals": {}, "error": "insufficient_data"}

        signals = analyze_technical_signals(frame)
        return {"ticker": ticker, "signals": signals, "error": None}
    except Exception as exc:
        return {"ticker": ticker, "signals": {}, "error": type(exc).__name__}


def _render_report(results: List[Dict], output_dir: Path) -> Path:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"technical_signals_scan_{timestamp}.txt"

    grouped = {k: {} for k in CATEGORIES}
    analyzed = 0
    errored = 0

    for row in results:
        if row.get("error"):
            errored += 1
            continue
        analyzed += 1
        ticker = row["ticker"]
        signals = row.get("signals", {}) or {}
        for key in CATEGORIES:
            vals = signals.get(key, []) or []
            if vals:
                grouped[key][ticker] = vals

    lines = []
    lines.append("=" * 80)
    lines.append("STANDALONE TECHNICAL SIGNAL CATEGORY SCAN")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 80)
    lines.append("")
    lines.append(f"Universe Analyzed: {analyzed}")
    lines.append(f"Errors/Skipped: {errored}")
    lines.append("")

    lines.append("Buy Signals\tSell Signals\tMomentum\tPatterns")
    lines.append("-" * 80)
    lines.append(
        "Bullish crossovers\tBearish divergence\tHigh RSI/MFI/CCI\tHead & Shoulders"
    )
    lines.append(
        "Breakout detection\tDeath crossover\tVolume breakouts\tDouble Top/Bottom"
    )
    lines.append(
        "Support bounces\t52-week lows\tMomentum gainers\tCup & Handle"
    )
    lines.append(
        "Golden cross\tTrend reversal\tATR expansion\tInside Bar"
    )
    lines.append("")

    for key, label in CATEGORIES.items():
        lines.append("=" * 80)
        lines.append(f"{label.upper()} DETECTIONS")
        lines.append("=" * 80)
        section = grouped[key]
        if not section:
            lines.append("No detections.")
            lines.append("")
            continue

        ranked = sorted(section.items(), key=lambda item: len(item[1]), reverse=True)
        for ticker, sigs in ranked[:200]:
            lines.append(f"- {ticker}: {', '.join(sigs)}")
        lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    latest = output_dir / "latest_technical_scan.txt"
    latest.write_text(report_path.read_text(encoding="utf-8"), encoding="utf-8")
    return report_path


def _email_report(report_path: Path) -> bool:
    """Email technical scan report as markdown/plain newsletter payload."""
    notifier = EmailNotifier()
    if not notifier.enabled:
        logger.warning("Email notifier is not configured; skipping technical report email")
        return False
    return notifier.send_newsletter(
        newsletter_path=str(report_path),
        scan_report_path=str(report_path),
        subject=f"📊 Standalone Technical Signals Scan | {datetime.now().strftime('%Y-%m-%d')}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone technical signal category scan")
    parser.add_argument("--limit", type=int, default=0, help="Limit universe size (0 = full universe)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers")
    parser.add_argument("--include-etfs", action="store_true", help="Include ETFs universe")
    parser.add_argument("--send-email", action="store_true", help="Email the generated technical report")
    parser.add_argument("--output-dir", default="./data/technical_scans", help="Output directory")
    args = parser.parse_args()

    _configure_runtime_environment()

    universe = _load_tickers_with_fallback(include_etfs=args.include_etfs)
    tickers = universe[: args.limit] if args.limit and args.limit > 0 else universe
    logger.info("Scanning %d tickers for technical categories...", len(tickers))

    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_scan_ticker, t): t for t in tickers}
        for fut in as_completed(futures):
            results.append(fut.result())

    report = _render_report(results, Path(args.output_dir))
    logger.info("Technical signal report written: %s", report)

    if args.send_email:
        sent = _email_report(report)
        if sent:
            logger.info("Technical signal report emailed successfully")
        else:
            logger.error("Technical signal report email failed")


if __name__ == "__main__":
    main()
