from datetime import date

from src.research.crowwd_closing_bell import (
    ClosingBellConfig,
    build_timeline,
    rewards_catalogue,
    simulation_snapshot,
)


def test_build_timeline_has_expected_order_and_count():
    timeline = build_timeline()

    assert len(timeline) == 4
    assert timeline[0].name == "Simulation Opens"
    assert timeline[-1].name == "The Bell Rings"
    assert timeline[0].day < timeline[-1].day


def test_snapshot_in_fy_end_window_reports_correct_phase_and_days():
    cfg = ClosingBellConfig()

    snapshot = simulation_snapshot(as_of=date(2026, 3, 30), config=cfg)

    assert snapshot["phase"] == "fy-end-volatility"
    assert snapshot["days_to_close"] == 16
    assert snapshot["elapsed_days"] == 16
    assert snapshot["next_milestone"]["name"] == "FY26 Begins"


def test_rewards_catalogue_contains_ppi_internship_reward():
    rewards = rewards_catalogue()

    assert len(rewards) >= 6
    assert any("Pre-Placement Interview" in reward for reward in rewards)
