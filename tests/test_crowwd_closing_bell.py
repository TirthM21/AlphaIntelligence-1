from datetime import date

from src.research.crowwd_closing_bell import (
    ClosingBellConfig,
    build_timeline,
    competitor_playbook,
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


def test_competitor_playbook_is_participant_focused_and_phase_aware():
    playbook = competitor_playbook(as_of=date(2026, 3, 30), risk_level="aggressive", style="momentum")

    assert playbook["phase"] == "fy-end-volatility"
    assert playbook["risk_level"] == "aggressive"
    assert playbook["style"] == "momentum"
    assert playbook["positioning"]["risk_per_trade_pct"] == 2.0
    assert any("volatility" in item.lower() for item in playbook["focus"])
