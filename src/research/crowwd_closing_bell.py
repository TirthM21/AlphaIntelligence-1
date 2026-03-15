from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class Milestone:
    """Key date in the simulation calendar."""

    name: str
    day: date
    description: str


@dataclass(frozen=True)
class ClosingBellConfig:
    """Static config for Crowwd's Closing Bell simulation."""

    title: str = "Crowwd: The Closing Bell"
    host: str = "Crowwd"
    tagline: str = "India's only social platform for investors"
    start_date: date = date(2026, 3, 15)
    fy_end_window_start: date = date(2026, 3, 28)
    fy_end_window_end: date = date(2026, 3, 31)
    fy26_start: date = date(2026, 4, 1)
    end_date: date = date(2026, 4, 15)
    virtual_capital_inr: int = 1_000_000
    format: str = "Individual"
    universe: str = "Curated Indian equities"


def _phase_label(config: ClosingBellConfig, as_of: date) -> str:
    if as_of < config.start_date:
        return "pre-launch"
    if config.start_date <= as_of < config.fy_end_window_start:
        return "opening-leg"
    if config.fy_end_window_start <= as_of <= config.fy_end_window_end:
        return "fy-end-volatility"
    if config.fy26_start <= as_of <= config.end_date:
        return "fy26-positioning"
    return "closed"


def build_timeline(config: ClosingBellConfig | None = None) -> list[Milestone]:
    cfg = config or ClosingBellConfig()
    return [
        Milestone("Simulation Opens", cfg.start_date, "Trading begins with ₹10,00,000 virtual capital."),
        Milestone(
            "FY-End Window",
            cfg.fy_end_window_start,
            "Final trading stretch of FY25 where volatility usually peaks.",
        ),
        Milestone("FY26 Begins", cfg.fy26_start, "New financial year allocations and positioning begin."),
        Milestone("The Bell Rings", cfg.end_date, "Leaderboard closes and winners are announced."),
    ]


def simulation_snapshot(as_of: date, config: ClosingBellConfig | None = None) -> dict[str, Any]:
    cfg = config or ClosingBellConfig()
    phase = _phase_label(cfg, as_of)

    total_days = (cfg.end_date - cfg.start_date).days + 1
    elapsed_days = 0
    if as_of >= cfg.start_date:
        elapsed_days = min((as_of - cfg.start_date).days + 1, total_days)

    days_to_start = (cfg.start_date - as_of).days
    days_to_close = (cfg.end_date - as_of).days

    next_milestone = None
    for milestone in build_timeline(cfg):
        if as_of <= milestone.day:
            next_milestone = milestone
            break

    return {
        "phase": phase,
        "as_of": as_of.isoformat(),
        "days_to_start": max(days_to_start, 0),
        "days_to_close": max(days_to_close, 0),
        "elapsed_days": elapsed_days,
        "total_days": total_days,
        "progress_pct": round((elapsed_days / total_days) * 100, 2) if total_days else 0.0,
        "next_milestone": {
            "name": next_milestone.name,
            "date": next_milestone.day.isoformat(),
            "description": next_milestone.description,
        }
        if next_milestone
        else None,
    }


def rewards_catalogue() -> list[str]:
    return [
        "Pre-Placement Interview: Top 10 participants receive a PPI for a Financial Analyst Internship at Crowwd.",
        "Letter of Appreciation issued by Crowwd's Co-Founder & CEO.",
        "Network Exposure through featured trades and insights on Crowwd's LinkedIn and Instagram.",
        "Newsroom Access to publish equity and macro analysis on Crowwd.",
        "Event Invitations for exclusive networking events in Delhi and Mumbai.",
        "Early Access and Merch for upcoming student programs and Crowwd merchandise.",
    ]


def competitor_playbook(
    as_of: date,
    risk_level: str = "balanced",
    style: str = "hybrid",
    config: ClosingBellConfig | None = None,
) -> dict[str, Any]:
    """Build a participant-focused playbook to maximize leaderboard performance."""
    cfg = config or ClosingBellConfig()
    snapshot = simulation_snapshot(as_of=as_of, config=cfg)
    phase = snapshot["phase"]

    normalized_risk = risk_level.strip().lower()
    if normalized_risk not in {"conservative", "balanced", "aggressive"}:
        normalized_risk = "balanced"

    normalized_style = style.strip().lower()
    if normalized_style not in {"value", "momentum", "hybrid"}:
        normalized_style = "hybrid"

    cash_floor_by_risk = {"conservative": 0.4, "balanced": 0.25, "aggressive": 0.1}
    risk_per_trade_by_risk = {"conservative": 0.0075, "balanced": 0.0125, "aggressive": 0.02}
    max_positions_by_risk = {"conservative": 8, "balanced": 12, "aggressive": 16}

    phase_focus = {
        "pre-launch": "Prepare watchlists and position templates before day 1.",
        "opening-leg": "Build initial book quality-first; avoid overtrading early noise.",
        "fy-end-volatility": "Exploit volatility with tighter stops and faster review loops.",
        "fy26-positioning": "Rotate to leaders with fresh FY momentum and avoid laggards.",
        "closed": "Review trades, document edge, and prepare interview-ready notes.",
    }

    style_focus = {
        "value": "Prioritize quality value + earnings durability setups.",
        "momentum": "Prioritize breakout strength, volume confirmation, and trend follow-through.",
        "hybrid": "Blend value filters with momentum timing for higher hit-rate entries.",
    }

    checklist = [
        "Run pre-market scan and shortlist only A+ setups.",
        "Define invalidation and stop-loss before entry.",
        "Update leaderboard delta and open risk after market close.",
        "Journal top 3 decisions (good and bad) daily.",
    ]

    return {
        "phase": phase,
        "risk_level": normalized_risk,
        "style": normalized_style,
        "positioning": {
            "cash_floor_pct": round(cash_floor_by_risk[normalized_risk] * 100, 1),
            "risk_per_trade_pct": round(risk_per_trade_by_risk[normalized_risk] * 100, 2),
            "max_positions": max_positions_by_risk[normalized_risk],
        },
        "focus": [phase_focus[phase], style_focus[normalized_style]],
        "daily_checklist": checklist,
        "win_condition": "Protect downside first, then compound into confirmed leaders.",
    }
