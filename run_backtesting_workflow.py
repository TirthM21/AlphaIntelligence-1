"""Run backtesting workflow and optionally email summary."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

from src.backtesting.dashboard_data import run_backtests, compute_piotroski_proxy
from src.notifications.email_notifier import EmailNotifier
from src.utils.logging_config import configure_logging


logger = logging.getLogger(__name__)

def main() -> int:
    parser = argparse.ArgumentParser(description="Run backtesting workflow")
    parser.add_argument("--symbols", default="RELIANCE,HDFCBANK,TCS,INFY", help="Comma-separated NSE symbols")
    parser.add_argument("--send-email", action="store_true", help="Email summary using configured EMAIL_* env vars")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    parser.add_argument("--json-logs", action="store_true", help="Emit structured JSON logs")
    args = parser.parse_args()

    configure_logging(level=args.log_level, json_logs=args.json_logs)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    data = run_backtests(symbols)
    pio = compute_piotroski_proxy(symbols)

    summary_lines = [
        f"# Backtesting Workflow Summary ({datetime.now():%Y-%m-%d %H:%M})",
        "",
        "## Top metrics",
        data["metrics"].head(10).to_markdown(index=False),
        "",
        "## Piotroski proxy",
        pio.to_markdown(index=False),
        "",
        f"Metrics CSV: {data['csv']}",
        f"Payload JSON: {data['json']}",
    ]

    out = Path("data/backtests/results")
    out.mkdir(parents=True, exist_ok=True)
    summary_path = out / "backtesting_workflow_latest.md"
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")
    logger.info("Wrote summary: %s", summary_path)

    if args.send_email:
        notifier = EmailNotifier()
        notifier.send_newsletter(str(summary_path), subject="📈 Backtesting Workflow Summary")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
