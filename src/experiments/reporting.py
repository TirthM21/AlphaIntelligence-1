"""Comparison reporting for experiment runs."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


def _normal_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def generate_experiment_comparison_report(
    aggregate_metrics: pd.DataFrame,
    baseline_parameter_id: Optional[str] = None,
    output_path: Optional[str] = None,
) -> Dict[str, object]:
    """Rank parameter sets and flag statistically robust upgrades."""
    if aggregate_metrics.empty:
        raise ValueError("aggregate_metrics is empty")

    ranked = aggregate_metrics.copy()
    ranked["robust_score"] = (
        ranked["sharpe"]
        - ranked["sharpe_std"].fillna(0)
        + ranked["total_return"]
        + ranked["max_drawdown"]
    )
    ranked = ranked.sort_values("robust_score", ascending=False).reset_index(drop=True)

    if baseline_parameter_id is None:
        baseline_parameter_id = str(ranked.iloc[0]["parameter_id"])

    base_row = ranked[ranked["parameter_id"] == baseline_parameter_id]
    if base_row.empty:
        raise ValueError(f"baseline parameter_id not found: {baseline_parameter_id}")

    baseline = base_row.iloc[0]
    baseline_sharpe = float(baseline["sharpe"])
    baseline_var = float(baseline.get("sharpe_std", 0.0)) ** 2

    flags = []
    for _, row in ranked.iterrows():
        diff = float(row["sharpe"] - baseline_sharpe)
        pooled_se = math.sqrt(max(float(row.get("sharpe_std", 0.0)) ** 2 + baseline_var, 1e-9))
        z_score = diff / pooled_se
        p_value = float(2 * (1 - _normal_cdf(abs(z_score))))
        robust_upgrade = bool(
            (row["parameter_id"] != baseline_parameter_id)
            and (float(row["sharpe"]) > baseline_sharpe)
            and (float(row["max_drawdown"]) >= float(baseline["max_drawdown"]))
            and (p_value < 0.1)
        )
        flags.append(
            {
                "parameter_id": row["parameter_id"],
                "robust_score": float(row["robust_score"]),
                "vs_baseline_sharpe_diff": diff,
                "z_score": float(z_score),
                "p_value": p_value,
                "robust_upgrade": robust_upgrade,
            }
        )

    flags_df = pd.DataFrame(flags)
    merged = ranked.merge(flags_df, on=["parameter_id", "robust_score"], how="left")

    ranking_view = merged[[
        "parameter_id",
        "robust_score",
        "sharpe",
        "max_drawdown",
        "hit_rate",
        "turnover",
        "total_return",
        "p_value",
        "robust_upgrade",
    ]].copy()

    markdown = [
        "# Experiment Comparison Report",
        "",
        f"Baseline parameter set: `{baseline_parameter_id}`",
        "",
        "## Ranking",
        "```",
        ranking_view.to_string(index=False),
        "```",
    ]

    report_path = None
    if output_path:
        report_path = Path(output_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("\n".join(markdown), encoding="utf-8")

    return {
        "ranked": merged,
        "baseline_parameter_id": baseline_parameter_id,
        "report_markdown": "\n".join(markdown),
        "report_path": str(report_path) if report_path else None,
    }
