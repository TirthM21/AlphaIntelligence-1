"""Drift monitoring utilities for screening input features."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


NUMERIC_FEATURES = ["volume_ratio", "rs_slope"]
CATEGORICAL_FEATURES = ["phase"]
BINARY_FEATURES = ["has_fundamentals"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES + BINARY_FEATURES

DEFAULT_THRESHOLDS = {
    "psi": 0.25,
    "ks": 0.20,
    "zscore": 3.0,
}


@dataclass
class DriftBaseline:
    """Serialized baseline profile for all monitored features."""

    created_at: str
    n_samples: int
    features: Dict[str, Dict[str, Any]]


class DriftMonitor:
    """Compute distribution drift against a precomputed baseline."""

    def __init__(
        self,
        snapshot_path: str | Path = "data/monitoring/drift_snapshots.jsonl",
        thresholds: Optional[Dict[str, float]] = None,
    ) -> None:
        self.snapshot_path = Path(snapshot_path)
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}

    def define_baseline(self, baseline_df: pd.DataFrame, bins: int = 10) -> DriftBaseline:
        """Define baseline distributions for monitored inputs."""
        baseline_features: Dict[str, Dict[str, Any]] = {}

        for feature in NUMERIC_FEATURES:
            series = pd.to_numeric(baseline_df.get(feature, pd.Series(dtype=float)), errors="coerce").dropna()
            if series.empty:
                baseline_features[feature] = {"type": "numeric", "missing": True}
                continue

            edges = np.quantile(series, np.linspace(0, 1, bins + 1))
            edges = np.unique(edges)
            if len(edges) < 2:
                edges = np.array([series.min() - 1e-9, series.max() + 1e-9])

            counts, final_edges = np.histogram(series, bins=edges)
            baseline_features[feature] = {
                "type": "numeric",
                "edges": final_edges.tolist(),
                "dist": self._normalize(counts).tolist(),
                "mean": float(series.mean()),
                "std": float(series.std(ddof=0) or 1e-9),
                "values": series.to_numpy().tolist(),
            }

        phase_series = baseline_df.get("phase", pd.Series(dtype=str)).fillna("unknown").astype(str)
        phase_counts = phase_series.value_counts(dropna=False)
        baseline_features["phase"] = {
            "type": "categorical",
            "categories": phase_counts.index.tolist(),
            "dist": self._normalize(phase_counts.values).tolist(),
        }

        fundamentals_series = baseline_df.get("has_fundamentals", pd.Series(dtype=float)).fillna(0).astype(int).clip(0, 1)
        fundamentals_counts = fundamentals_series.value_counts().reindex([0, 1], fill_value=0)
        baseline_features["has_fundamentals"] = {
            "type": "categorical",
            "categories": [0, 1],
            "dist": self._normalize(fundamentals_counts.values).tolist(),
            "availability_rate": float(fundamentals_series.mean()) if len(fundamentals_series) else 0.0,
        }

        return DriftBaseline(
            created_at=datetime.utcnow().isoformat(),
            n_samples=len(baseline_df),
            features=baseline_features,
        )

    def compute_daily_drift(
        self,
        current_df: pd.DataFrame,
        baseline: DriftBaseline,
        snapshot_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Compute feature-level drift metrics and alert state."""
        snapshot_date = snapshot_date or datetime.utcnow().date()
        metrics: Dict[str, Dict[str, Any]] = {}

        for feature in ALL_FEATURES:
            feature_profile = baseline.features.get(feature, {})
            if feature_profile.get("missing"):
                metrics[feature] = {"status": "missing_baseline"}
                continue

            if feature in NUMERIC_FEATURES:
                metrics[feature] = self._compute_numeric_metrics(current_df, feature, feature_profile)
            else:
                metrics[feature] = self._compute_categorical_metrics(current_df, feature, feature_profile)

        alerts = self._collect_alerts(metrics)
        snapshot = {
            "date": snapshot_date.isoformat(),
            "created_at": datetime.utcnow().isoformat(),
            "n_samples": len(current_df),
            "thresholds": self.thresholds,
            "metrics": metrics,
            "alerts": alerts,
            "alert_triggered": len(alerts) > 0,
        }
        return snapshot

    def store_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Append a drift snapshot to history storage."""
        with self.snapshot_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(snapshot) + "\n")

    def load_snapshots(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Read historical snapshots from jsonl storage."""
        if not self.snapshot_path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        with self.snapshot_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        if limit:
            return rows[-limit:]
        return rows

    def run_daily_monitoring(
        self,
        current_df: pd.DataFrame,
        baseline: DriftBaseline,
        slack_notifier: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Compute, store, and optionally notify on drift."""
        snapshot = self.compute_daily_drift(current_df=current_df, baseline=baseline)
        self.store_snapshot(snapshot)

        if snapshot["alert_triggered"] and slack_notifier is not None:
            try:
                slack_notifier.send_drift_alert(snapshot)
            except Exception as exc:
                logger.warning("Failed to send drift alert: %s", exc)

        return snapshot

    def to_dashboard_payload(self, snapshots: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Transform drift history into dashboard-ready tabular payload."""
        records: List[Dict[str, Any]] = []
        for snap in snapshots:
            for feature, metric in snap.get("metrics", {}).items():
                records.append(
                    {
                        "date": snap.get("date"),
                        "feature": feature,
                        "psi": metric.get("psi"),
                        "ks": metric.get("ks"),
                        "zscore": metric.get("zscore"),
                        "alert": bool(metric.get("alert", False)),
                    }
                )
        frame = pd.DataFrame(records)
        return {
            "summary": {
                "latest_date": snapshots[-1]["date"] if snapshots else None,
                "num_snapshots": len(snapshots),
                "active_alerts": len(snapshots[-1].get("alerts", [])) if snapshots else 0,
            },
            "timeseries": frame,
            "latest_snapshot": snapshots[-1] if snapshots else None,
        }

    def _compute_numeric_metrics(self, current_df: pd.DataFrame, feature: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        series = pd.to_numeric(current_df.get(feature, pd.Series(dtype=float)), errors="coerce").dropna()
        if series.empty:
            return {"status": "missing_current"}

        edges = np.array(profile["edges"])
        base_dist = np.array(profile["dist"])
        cur_counts, _ = np.histogram(series, bins=edges)
        cur_dist = self._normalize(cur_counts)

        psi = self._compute_psi(base_dist, cur_dist)
        ks = self._compute_ks(np.array(profile["values"]), series.to_numpy())
        zscore = (float(series.mean()) - float(profile["mean"])) / max(float(profile["std"]), 1e-9)

        alert = (
            psi >= self.thresholds["psi"]
            or ks >= self.thresholds["ks"]
            or abs(zscore) >= self.thresholds["zscore"]
        )
        return {
            "psi": float(psi),
            "ks": float(ks),
            "zscore": float(zscore),
            "mean": float(series.mean()),
            "alert": bool(alert),
        }

    def _compute_categorical_metrics(self, current_df: pd.DataFrame, feature: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        series = current_df.get(feature)
        if series is None:
            return {"status": "missing_current"}

        categories = profile["categories"]
        normalized = series.fillna("unknown") if feature == "phase" else series.fillna(0).astype(int).clip(0, 1)
        counts = normalized.value_counts().reindex(categories, fill_value=0)

        base_dist = np.array(profile["dist"])
        cur_dist = self._normalize(counts.values)
        psi = self._compute_psi(base_dist, cur_dist)

        zscore = None
        if feature == "has_fundamentals":
            baseline_rate = float(profile.get("availability_rate", 0.0))
            cur_rate = float(normalized.mean())
            baseline_var = max(baseline_rate * (1 - baseline_rate), 1e-9)
            zscore = (cur_rate - baseline_rate) / np.sqrt(baseline_var / max(len(normalized), 1))

        alert = psi >= self.thresholds["psi"] or (zscore is not None and abs(zscore) >= self.thresholds["zscore"])
        payload = {
            "psi": float(psi),
            "ks": None,
            "zscore": float(zscore) if zscore is not None else None,
            "alert": bool(alert),
        }
        if feature == "has_fundamentals":
            payload["availability_rate"] = float(normalized.mean())
        return payload

    def _collect_alerts(self, metrics: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        alerts = []
        for feature, metric in metrics.items():
            if metric.get("alert"):
                alerts.append(
                    {
                        "feature": feature,
                        "psi": metric.get("psi"),
                        "ks": metric.get("ks"),
                        "zscore": metric.get("zscore"),
                    }
                )
        return alerts

    @staticmethod
    def _compute_psi(base_dist: np.ndarray, cur_dist: np.ndarray) -> float:
        epsilon = 1e-6
        base = np.clip(base_dist, epsilon, 1.0)
        cur = np.clip(cur_dist, epsilon, 1.0)
        return float(np.sum((cur - base) * np.log(cur / base)))

    @staticmethod
    def _compute_ks(base_values: np.ndarray, current_values: np.ndarray) -> float:
        if len(base_values) == 0 or len(current_values) == 0:
            return 0.0
        base_sorted = np.sort(base_values)
        cur_sorted = np.sort(current_values)
        all_values = np.sort(np.concatenate([base_sorted, cur_sorted]))
        base_cdf = np.searchsorted(base_sorted, all_values, side="right") / len(base_sorted)
        cur_cdf = np.searchsorted(cur_sorted, all_values, side="right") / len(cur_sorted)
        return float(np.max(np.abs(base_cdf - cur_cdf)))

    @staticmethod
    def _normalize(values: np.ndarray) -> np.ndarray:
        total = np.sum(values)
        if total <= 0:
            return np.zeros_like(values, dtype=float)
        return values / total


def load_drift_dashboard_payload(snapshot_path: str | Path, limit: int = 30) -> Dict[str, Any]:
    """Convenience loader for Streamlit dashboard consumption."""
    monitor = DriftMonitor(snapshot_path=snapshot_path)
    snapshots = monitor.load_snapshots(limit=limit)
    return monitor.to_dashboard_payload(snapshots)
