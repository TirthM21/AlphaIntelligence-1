from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.strategies.method_catalog import get_strategy_method_catalogue
from src.strategies.research_model_registry import get_research_model_registry, summarize_registry_status

BUY_RE = re.compile(r"BUY\s*#\d+:\s*([A-Z0-9_\.\-]+)\s*\|\s*Score:\s*([0-9.]+)/")
SELL_RE = re.compile(r"SELL\s*#\d+:\s*([A-Z0-9_\.\-]+)\s*\|\s*Score:\s*([0-9.]+)/")


@dataclass
class SignalRow:
    ticker: str
    score: float
    side: str


def run_scan(strategy: str, limit: int, root_dir: Path, out_dir: Path) -> Path:
    cmd = [
        "python",
        "run_optimized_scan.py",
        "--conservative",
        "--git-storage",
        "--prefetch-storage",
        "--no-email",
        "--universe-source",
        "exchange",
        "--strategy",
        strategy,
        "--limit",
        str(limit),
    ]
    subprocess.run(cmd, cwd=root_dir, check=True)

    latest_report = root_dir / "data" / "daily_scans" / "latest_optimized_scan.txt"
    if not latest_report.exists():
        raise FileNotFoundError(f"Expected scan report missing: {latest_report}")

    strategy_report = out_dir / f"{strategy}_latest_scan.txt"
    shutil.copy2(latest_report, strategy_report)
    return strategy_report


def parse_report(report_path: Path) -> tuple[list[SignalRow], list[SignalRow]]:
    text = report_path.read_text(encoding="utf-8", errors="ignore")
    buys = [SignalRow(ticker=t, score=float(s), side="BUY") for t, s in BUY_RE.findall(text)]
    sells = [SignalRow(ticker=t, score=float(s), side="SELL") for t, s in SELL_RE.findall(text)]
    return buys, sells


def aggregate(signals_by_strategy: dict[str, tuple[list[SignalRow], list[SignalRow]]], top_n: int) -> dict:
    buy_votes: dict[str, dict] = defaultdict(lambda: {"votes": 0, "score_sum": 0.0, "sources": []})
    sell_votes: dict[str, dict] = defaultdict(lambda: {"votes": 0, "score_sum": 0.0, "sources": []})

    for strategy, (buys, sells) in signals_by_strategy.items():
        for row in buys:
            bucket = buy_votes[row.ticker]
            bucket["votes"] += 1
            bucket["score_sum"] += row.score
            bucket["sources"].append({"strategy": strategy, "score": row.score})
        for row in sells:
            bucket = sell_votes[row.ticker]
            bucket["votes"] += 1
            bucket["score_sum"] += row.score
            bucket["sources"].append({"strategy": strategy, "score": row.score})

    def to_ranked(votes: dict[str, dict]) -> list[dict]:
        ranked = []
        for ticker, row in votes.items():
            avg_score = row["score_sum"] / row["votes"] if row["votes"] else 0.0
            confidence = round((row["votes"] / max(len(signals_by_strategy), 1)) * 100, 2)
            ranked.append(
                {
                    "ticker": ticker,
                    "votes": row["votes"],
                    "avg_score": round(avg_score, 2),
                    "confidence_pct": confidence,
                    "sources": row["sources"],
                }
            )
        return sorted(ranked, key=lambda x: (x["votes"], x["avg_score"]), reverse=True)[:top_n]

    return {"buys": to_ranked(buy_votes), "sells": to_ranked(sell_votes)}


def render_markdown(payload: dict) -> str:
    lines: list[str] = []
    lines.append("# Crowwd Competition Daily Condensed List")
    lines.append("")
    lines.append(f"Generated at: {payload['generated_at_utc']}")
    lines.append("")

    lines.append("## Ensemble Coverage")
    lines.append(f"- Strategies run: {', '.join(payload['strategies_run'])}")
    status = payload["research_registry"]["summary"]
    lines.append(
        f"- Research models tracked: implemented={status.get('implemented', 0)}, "
        f"partial={status.get('partial', 0)}, planned={status.get('planned', 0)}"
    )
    lines.append("")

    lines.append("## Top BUY Ideas")
    if payload["condensed"]["buys"]:
        for i, row in enumerate(payload["condensed"]["buys"], 1):
            lines.append(
                f"{i}. **{row['ticker']}** | votes={row['votes']} | avg_score={row['avg_score']} "
                f"| confidence={row['confidence_pct']}%"
            )
    else:
        lines.append("- No consensus BUY ideas found.")
    lines.append("")

    lines.append("## Top SELL / AVOID Ideas")
    if payload["condensed"]["sells"]:
        for i, row in enumerate(payload["condensed"]["sells"], 1):
            lines.append(
                f"{i}. **{row['ticker']}** | votes={row['votes']} | avg_score={row['avg_score']} "
                f"| confidence={row['confidence_pct']}%"
            )
    else:
        lines.append("- No consensus SELL ideas found.")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily condensed buy/sell list for Crowwd competition")
    parser.add_argument("--limit", type=int, default=180, help="Universe limit for each strategy scan")
    parser.add_argument("--top-n", type=int, default=15, help="Top ideas to keep on each side")
    parser.add_argument("--skip-scan", action="store_true", help="Skip fresh scans and parse existing copied reports")
    args = parser.parse_args()

    root_dir = ROOT_DIR
    out_dir = root_dir / "data" / "competition_signals"
    out_dir.mkdir(parents=True, exist_ok=True)

    strategies = ["daily_momentum", "long_term"]
    reports: dict[str, Path] = {}

    if args.skip_scan:
        for strategy in strategies:
            path = out_dir / f"{strategy}_latest_scan.txt"
            if not path.exists():
                raise FileNotFoundError(f"Missing report for --skip-scan mode: {path}")
            reports[strategy] = path
    else:
        for strategy in strategies:
            reports[strategy] = run_scan(strategy=strategy, limit=args.limit, root_dir=root_dir, out_dir=out_dir)

    parsed = {strategy: parse_report(path) for strategy, path in reports.items()}
    condensed = aggregate(parsed, top_n=args.top_n)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "strategies_run": strategies,
        "method_catalog": get_strategy_method_catalogue(),
        "research_registry": {
            "summary": summarize_registry_status(),
            "models": get_research_model_registry(),
        },
        "condensed": condensed,
    }

    json_path = out_dir / "daily_competition_signals.json"
    md_path = out_dir / "daily_competition_signals.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(payload), encoding="utf-8")

    stable_json = out_dir / "latest_competition_signals.json"
    stable_md = out_dir / "latest_competition_signals.md"
    stable_json.write_text(json_path.read_text(encoding="utf-8"), encoding="utf-8")
    stable_md.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
