"""Tests for drift monitoring and Slack alert hooks."""

from pathlib import Path

import pandas as pd

from src.monitoring.drift import DriftMonitor
from src.notifications.slack_notifier import SlackNotifier


def _baseline_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "volume_ratio": [0.9, 1.0, 1.1, 1.2, 0.95, 1.05, 1.15, 1.0],
            "rs_slope": [0.01, 0.015, 0.02, 0.013, 0.018, 0.017, 0.014, 0.016],
            "phase": ["accumulation", "accumulation", "markup", "markup", "accumulation", "markup", "distribution", "accumulation"],
            "has_fundamentals": [1, 1, 1, 1, 1, 1, 0, 1],
        }
    )


def test_drift_alerts_trigger_on_shifted_distribution(tmp_path: Path):
    monitor = DriftMonitor(snapshot_path=tmp_path / "drift.jsonl", thresholds={"psi": 0.1, "ks": 0.1, "zscore": 1.5})
    baseline = monitor.define_baseline(_baseline_df())

    shifted = pd.DataFrame(
        {
            "volume_ratio": [3.0, 3.2, 2.9, 3.1, 3.3, 3.0, 3.4, 3.2],
            "rs_slope": [0.09, 0.1, 0.11, 0.095, 0.105, 0.11, 0.092, 0.108],
            "phase": ["markdown"] * 8,
            "has_fundamentals": [0] * 8,
        }
    )

    snapshot = monitor.compute_daily_drift(shifted, baseline)
    assert snapshot["alert_triggered"] is True
    assert len(snapshot["alerts"]) >= 2


def test_snapshot_storage_and_dashboard_payload(tmp_path: Path):
    monitor = DriftMonitor(snapshot_path=tmp_path / "drift.jsonl")
    baseline = monitor.define_baseline(_baseline_df())
    snapshot = monitor.compute_daily_drift(_baseline_df(), baseline)
    monitor.store_snapshot(snapshot)

    snapshots = monitor.load_snapshots()
    assert len(snapshots) == 1

    payload = monitor.to_dashboard_payload(snapshots)
    assert payload["summary"]["num_snapshots"] == 1
    assert set(payload["timeseries"]["feature"]) == {"volume_ratio", "rs_slope", "phase", "has_fundamentals"}


def test_run_daily_monitoring_calls_slack_when_alerted(tmp_path: Path):
    monitor = DriftMonitor(snapshot_path=tmp_path / "drift.jsonl", thresholds={"psi": 0.1, "ks": 0.1, "zscore": 1.0})
    baseline = monitor.define_baseline(_baseline_df())

    shifted = pd.DataFrame(
        {
            "volume_ratio": [3.0, 3.2, 3.1, 3.3],
            "rs_slope": [0.08, 0.09, 0.1, 0.11],
            "phase": ["markdown"] * 4,
            "has_fundamentals": [0, 0, 0, 0],
        }
    )

    class DummySlack:
        def __init__(self):
            self.calls = 0

        def send_drift_alert(self, snapshot):
            self.calls += 1
            return True

    slack = DummySlack()
    snapshot = monitor.run_daily_monitoring(shifted, baseline, slack_notifier=slack)
    assert snapshot["alert_triggered"] is True
    assert slack.calls == 1


def test_slack_notifier_formats_drift_blocks():
    notifier = SlackNotifier(webhook_url="http://example.com")
    snapshot = {
        "date": "2026-01-10",
        "alerts": [{"feature": "volume_ratio", "psi": 0.31, "ks": 0.22, "zscore": 3.1}],
    }
    blocks = notifier._format_drift_blocks(snapshot)
    assert blocks[0]["type"] == "header"
    assert "Data Drift Alert" in blocks[0]["text"]["text"]
    assert any("volume_ratio" in b.get("text", {}).get("text", "") for b in blocks if b.get("type") == "section")
