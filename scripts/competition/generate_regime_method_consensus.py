from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.strategies.competition_method_framework import (
    compute_method_votes,
    detect_regime,
    library_payload,
    regime_weighted_score,
)

DEFAULT_SYMBOLS = [
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
    "SBIN.NS",
    "LT.NS",
    "ITC.NS",
    "HINDUNILVR.NS",
    "BHARTIARTL.NS",
]


def _fetch_history(symbol: str, period: str = "2y"):
    df = yf.download(symbol, period=period, interval="1d", progress=False, auto_adjust=False)
    if df is None or df.empty:
        return None
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)
    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(set(df.columns)):
        return None
    return df


def _rank(results: list[dict], side: str, top_n: int) -> list[dict]:
    if side == "BUY":
        ordered = sorted(results, key=lambda x: (x["score"], x["buy_votes"], -x["sell_votes"]), reverse=True)
        return [r for r in ordered if r["score"] > 0][:top_n]
    ordered = sorted(results, key=lambda x: (x["score"], -x["sell_votes"], x["buy_votes"]))
    return [r for r in ordered if r["score"] < 0][:top_n]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate regime-aware daily BUY/SELL list across 65-method framework")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="Comma-separated symbols")
    parser.add_argument("--top-n", type=int, default=12)
    parser.add_argument("--period", default="2y")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out_dir = ROOT_DIR / "data" / "competition_signals"
    out_dir.mkdir(parents=True, exist_ok=True)

    six_month_returns: dict[str, float] = {}
    raw_frames = {}

    for symbol in symbols:
        df = _fetch_history(symbol, period=args.period)
        if df is None or len(df) < 260:
            continue
        raw_frames[symbol] = df
        six_month_returns[symbol] = df["Close"].iloc[-1] / df["Close"].iloc[-126] - 1

    if not raw_frames:
        raise RuntimeError("No usable symbol history fetched for consensus generation")

    benchmark_return_6m = sum(six_month_returns.values()) / max(len(six_month_returns), 1)

    rows = []
    for symbol, df in raw_frames.items():
        votes = compute_method_votes(df, benchmark_return_6m=benchmark_return_6m)
        regime = detect_regime(df)
        score = regime_weighted_score(votes, regime)
        buy_votes = sum(1 for v in votes.values() if v > 0)
        sell_votes = sum(1 for v in votes.values() if v < 0)

        rows.append(
            {
                "symbol": symbol,
                "regime": regime,
                "score": score,
                "buy_votes": buy_votes,
                "sell_votes": sell_votes,
                "votes": votes,
            }
        )

    top_buys = _rank(rows, "BUY", args.top_n)
    top_sells = _rank(rows, "SELL", args.top_n)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "symbols_considered": sorted(raw_frames.keys()),
        "benchmark_return_6m": round(benchmark_return_6m, 6),
        "method_library": library_payload(),
        "top_buys": top_buys,
        "top_sells": top_sells,
    }

    json_path = out_dir / "daily_regime_method_consensus.json"
    md_path = out_dir / "daily_regime_method_consensus.md"

    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Daily Regime-Switching Method Consensus",
        "",
        f"Generated at: {payload['generated_at_utc']}",
        f"Symbols considered: {len(payload['symbols_considered'])}",
        "",
        "## Top BUY",
    ]
    if top_buys:
        for i, row in enumerate(top_buys, 1):
            lines.append(
                f"{i}. **{row['symbol']}** | score={row['score']} | regime={row['regime']} "
                f"| buy_votes={row['buy_votes']} | sell_votes={row['sell_votes']}"
            )
    else:
        lines.append("- No BUY consensus.")

    lines.append("")
    lines.append("## Top SELL / AVOID")
    if top_sells:
        for i, row in enumerate(top_sells, 1):
            lines.append(
                f"{i}. **{row['symbol']}** | score={row['score']} | regime={row['regime']} "
                f"| buy_votes={row['buy_votes']} | sell_votes={row['sell_votes']}"
            )
    else:
        lines.append("- No SELL consensus.")

    summary = payload["method_library"]["summary"]
    lines.extend(
        [
            "",
            "## Method Library Coverage",
            f"- Implemented: {summary.get('implemented', 0)}",
            f"- Partial: {summary.get('partial', 0)}",
            f"- Planned: {summary.get('planned', 0)}",
        ]
    )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    (out_dir / "latest_regime_method_consensus.json").write_text(json_path.read_text(encoding="utf-8"), encoding="utf-8")
    (out_dir / "latest_regime_method_consensus.md").write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
