#!/usr/bin/env python3
"""Generate a Kalman-filter market report and optionally email it."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from src.data.finnhub_fetcher import FinnhubFetcher
from src.data.marketaux_fetcher import MarketauxFetcher
from src.data.fmp_fetcher import FMPFetcher
from src.notifications.email_notifier import EmailNotifier
from src.database.db_manager import DBManager


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("kalman_report")


def scalar_kalman_filter(prices: np.ndarray, q: float = 0.01, r: float = 1.0) -> np.ndarray:
    """Simple 1D Kalman filter for price smoothing."""
    x = float(prices[0])
    p = 1.0
    estimates = [x]

    for z in prices[1:]:
        # Predict
        x_pred = x
        p_pred = p + q

        # Update
        k = p_pred / (p_pred + r)
        x = x_pred + k * (float(z) - x_pred)
        p = (1.0 - k) * p_pred
        estimates.append(x)

    return np.array(estimates)


def zscore(series: np.ndarray, window: int = 20) -> np.ndarray:
    s = pd.Series(series)
    mean = s.rolling(window).mean()
    std = s.rolling(window).std(ddof=0).replace(0, np.nan)
    z = (s - mean) / std
    return z.fillna(0).values


def evaluate_ticker(ticker: str, period: str = "1y") -> Dict:
    data = yf.download(ticker, period=period, interval="1d", auto_adjust=True, progress=False)
    if data is None or data.empty or "Close" not in data:
        raise ValueError(f"No price data for {ticker}")

    close = data["Close"].astype(float)
    kavg = scalar_kalman_filter(close.values)
    spread = close.values - kavg
    z = zscore(spread, window=20)

    current_price = float(close.iloc[-1])
    current_kavg = float(kavg[-1])
    current_z = float(z[-1])

    signal = "HOLD"
    if current_z <= -2.0:
        signal = "LONG_SETUP"
    elif current_z >= 2.0:
        signal = "SHORT_SETUP"

    return {
        "ticker": ticker,
        "current_price": current_price,
        "kalman_avg": current_kavg,
        "spread": float(spread[-1]),
        "zscore": current_z,
        "signal": signal,
        "return_3m": float(close.pct_change(63).iloc[-1]) if len(close) > 63 else 0.0,
    }


def fetch_multi_source_news() -> List[Dict]:
    all_news: List[Dict] = []

    # Finnhub
    try:
        finnhub = FinnhubFetcher()
        news = finnhub.fetch_top_market_news(limit=20)
        for item in news or []:
            all_news.append({"source": "finnhub", "title": item.get("headline") or item.get("title"), "url": item.get("url")})
    except Exception as exc:
        logger.warning("Finnhub news unavailable: %s", exc)

    # Marketaux
    try:
        marketaux = MarketauxFetcher()
        news = marketaux.fetch_trending_entities(minutes=1440)
        for item in news or []:
            all_news.append({"source": "marketaux", "title": item.get("title") or item.get("name"), "url": item.get("url")})
    except Exception as exc:
        logger.warning("Marketaux news unavailable: %s", exc)

    # FMP
    try:
        fmp = FMPFetcher()
        news = fmp.fetch_market_news(limit=20)
        for item in news or []:
            all_news.append({"source": "fmp", "title": item.get("title"), "url": item.get("url")})
    except Exception as exc:
        logger.warning("FMP news unavailable: %s", exc)

    dedup: Dict[str, Dict] = {}
    for item in all_news:
        title = (item.get("title") or "").strip()
        if not title:
            continue
        key = title.lower()
        if key not in dedup:
            dedup[key] = item
    return list(dedup.values())[:25]


def build_report(results: List[Dict], news: List[Dict]) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        "# Kalman Filter Intelligence Report",
        "",
        f"Generated: {ts}",
        "",
        "## Strategy Rules",
        "- Long setup when z-score <= -2.0",
        "- Short setup when z-score >= +2.0",
        "- Hold otherwise",
        "",
        "## Signal Table",
        "| Ticker | Price | Kalman Avg | Spread | Z-Score | Signal | 3M Return |",
        "|---|---:|---:|---:|---:|---|---:|",
    ]

    for r in results:
        lines.append(
            f"| {r['ticker']} | {r['current_price']:.2f} | {r['kalman_avg']:.2f} | {r['spread']:.2f} | {r['zscore']:.2f} | {r['signal']} | {r['return_3m']:.2%} |"
        )

    lines.extend(["", "## Multi-Source News Digest"])
    if not news:
        lines.append("- No fresh news items available from configured providers.")
    else:
        for item in news[:15]:
            title = item.get("title", "Untitled")
            src = item.get("source", "unknown")
            url = item.get("url") or ""
            if url:
                lines.append(f"- [{title}]({url}) _(source: {src})_")
            else:
                lines.append(f"- {title} _(source: {src})_")

    lines.extend(["", "---", "_AlphaIntelligence automated Kalman report._"])
    return "\n".join(lines) + "\n"


def resolve_recipients() -> Tuple[List[str], str]:
    recipients: List[str] = []
    try:
        db = DBManager()
        recipients = [s[0] for s in db.get_active_subscribers()]
    except Exception as exc:
        logger.warning("Unable to load DB subscribers: %s", exc)

    notifier = EmailNotifier()
    fallback = notifier.recipient_email
    if not recipients and fallback:
        recipients = [fallback]
    return recipients, fallback


def main() -> None:
    parser = argparse.ArgumentParser(description="Kalman filter report generator")
    parser.add_argument("--tickers", default="AAPL,MSFT,NVDA,SPY,QQQ", help="Comma-separated symbols")
    parser.add_argument("--period", default="1y", help="yfinance period")
    parser.add_argument("--send-email", action="store_true", help="Email generated report")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    results: List[Dict] = []

    for ticker in tickers:
        try:
            results.append(evaluate_ticker(ticker, period=args.period))
        except Exception as exc:
            logger.warning("Skipping %s: %s", ticker, exc)

    if not results:
        logger.warning("No ticker results generated from live feeds; creating placeholder report.")
        for ticker in tickers:
            results.append(
                {
                    "ticker": ticker,
                    "current_price": 0.0,
                    "kalman_avg": 0.0,
                    "spread": 0.0,
                    "zscore": 0.0,
                    "signal": "DATA_UNAVAILABLE",
                    "return_3m": 0.0,
                }
            )

    news = fetch_multi_source_news()
    report_md = build_report(results, news)

    out_dir = Path("data/reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"kalman_filter_report_{stamp}.md"
    latest_path = out_dir / "kalman_filter_report_latest.md"
    out_path.write_text(report_md, encoding="utf-8")
    latest_path.write_text(report_md, encoding="utf-8")
    logger.info("Kalman report saved: %s", out_path)

    if args.send_email:
        notifier = EmailNotifier()
        if not notifier.enabled:
            raise SystemExit("Email disabled: configure EMAIL_SENDER and EMAIL_PASSWORD")

        recipients, _ = resolve_recipients()
        if not recipients:
            raise SystemExit("No email recipients found")

        sent = 0
        for recipient in recipients:
            try:
                notifier.recipient_email = recipient
                if notifier.send_newsletter(str(out_path), scan_report_path=None):
                    sent += 1
            except Exception as exc:
                logger.error("Email failed for %s: %s", recipient, exc)

        logger.info("Kalman email delivery: %s/%s", sent, len(recipients))
        if sent == 0:
            raise SystemExit("Kalman report email delivery failed")


if __name__ == "__main__":
    main()
