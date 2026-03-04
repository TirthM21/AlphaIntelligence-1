"""Run walk-forward experiments and persist outputs."""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

from src.backtesting.dashboard_data import fetch_price_history
from src.experiments import (
    generate_experiment_comparison_report,
    persist_experiment_run,
    run_walk_forward_backtest,
)


def _default_param_sets() -> list[dict]:
    return [
        {
            "id": "baseline",
            "short_ma": 50,
            "long_ma": 200,
            "breakout_window": 20,
            "vol_short": 20,
            "vol_long": 60,
            "volume_multiplier": 1.2,
        },
        {
            "id": "fast_breakout",
            "short_ma": 30,
            "long_ma": 150,
            "breakout_window": 10,
            "vol_short": 15,
            "vol_long": 40,
            "volume_multiplier": 1.1,
        },
        {
            "id": "conservative",
            "short_ma": 60,
            "long_ma": 200,
            "breakout_window": 30,
            "vol_short": 20,
            "vol_long": 80,
            "volume_multiplier": 1.4,
        },
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run walk-forward backtest experiments")
    parser.add_argument("--symbol", default="RELIANCE", help="Single NSE symbol")
    parser.add_argument("--years", type=int, default=5, help="History years to fetch")
    parser.add_argument("--train-window", type=int, default=252)
    parser.add_argument("--test-window", type=int, default=63)
    parser.add_argument("--step", type=int, default=63)
    parser.add_argument("--params-json", default="", help="Path to parameter sets JSON")
    parser.add_argument("--db-path", default="", help="Optional SQLite file to store experiment_run_metrics")
    args = parser.parse_args()

    to_date = date.today()
    from_date = to_date - timedelta(days=365 * args.years)
    prices = fetch_price_history(args.symbol, from_date, to_date)

    if args.params_json:
        with open(args.params_json, "r", encoding="utf-8") as fp:
            parameter_sets = json.load(fp)
    else:
        parameter_sets = _default_param_sets()

    results = run_walk_forward_backtest(
        prices=prices,
        parameter_sets=parameter_sets,
        train_window=args.train_window,
        test_window=args.test_window,
        step=args.step,
    )

    paths = persist_experiment_run(
        symbol=args.symbol,
        results=results,
        db_path=args.db_path or None,
    )

    run_dir = paths["run_dir"]
    report = generate_experiment_comparison_report(
        results["aggregate_metrics"],
        baseline_parameter_id="baseline" if "baseline" in {p.get("id") for p in parameter_sets} else None,
        output_path=f"{run_dir}/metrics/comparison_report.md",
    )

    print("Walk-forward experiment completed")
    print(f"Run directory: {run_dir}")
    print(f"Aggregate metrics CSV: {paths['aggregate_csv']}")
    print(f"Window metrics CSV: {paths['window_csv']}")
    print(f"Comparison report: {report['report_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
