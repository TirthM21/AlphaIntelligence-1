from datetime import date

import pandas as pd
import streamlit as st

from src.research.crowwd_closing_bell import (
    ClosingBellConfig,
    build_timeline,
    competitor_playbook,
    rewards_catalogue,
    simulation_snapshot,
)
from src.strategies.method_catalog import get_strategy_method_catalogue


st.set_page_config(page_title="Crowwd: The Closing Bell", layout="wide")

cfg = ClosingBellConfig()
st.title("🔔 Crowwd: The Closing Bell — Participant Mode")
st.caption("Your personal winning cockpit for the 31-day simulation")

st.markdown(
    """
The financial year doesn't just end — it settles accounts.

Trade through the most consequential month on India's market calendar.
**Close FY25. Open FY26.**
"""
)

st.subheader("Your Simulation Frame")
d1, d2, d3, d4 = st.columns(4)
d1.metric("Format", cfg.format)
d2.metric("Your Capital", "₹10,00,000")
d3.metric("Universe", cfg.universe)
d4.metric("Window", "Mar 15 – Apr 15")

st.subheader("Your Live Progress")
as_of = st.date_input("Planning date", value=date.today())
snapshot = simulation_snapshot(as_of=as_of, config=cfg)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Phase", snapshot["phase"].replace("-", " ").title())
c2.metric("Elapsed Days", f"{snapshot['elapsed_days']} / {snapshot['total_days']}")
c3.metric("Progress", f"{snapshot['progress_pct']}%")
c4.metric("Days Left", snapshot["days_to_close"])

if snapshot["next_milestone"]:
    nxt = snapshot["next_milestone"]
    st.info(f"Next milestone: **{nxt['name']}** ({nxt['date']}) — {nxt['description']}")

st.subheader("Your Winning Playbook")
p1, p2 = st.columns(2)
with p1:
    risk_level = st.selectbox("Risk profile", ["conservative", "balanced", "aggressive"], index=1)
with p2:
    style = st.selectbox("Style bias", ["value", "momentum", "hybrid"], index=2)

playbook = competitor_playbook(as_of=as_of, risk_level=risk_level, style=style, config=cfg)

p3, p4, p5 = st.columns(3)
p3.metric("Cash Floor", f"{playbook['positioning']['cash_floor_pct']}%")
p4.metric("Risk / Trade", f"{playbook['positioning']['risk_per_trade_pct']}%")
p5.metric("Max Positions", playbook["positioning"]["max_positions"])

st.markdown("**Focus for today**")
for item in playbook["focus"]:
    st.markdown(f"- {item}")

st.markdown("**Daily execution checklist**")
for item in playbook["daily_checklist"]:
    st.markdown(f"- {item}")

st.success(playbook["win_condition"])

st.subheader("Timeline")
timeline_df = pd.DataFrame(
    [
        {"Milestone": m.name, "Date": m.day.isoformat(), "Description": m.description}
        for m in build_timeline(cfg)
    ]
)
st.dataframe(timeline_df, use_container_width=True, hide_index=True)

st.subheader("Strategy Tracks You Can Use")
catalogue = get_strategy_method_catalogue()
for track, methods in catalogue.items():
    st.markdown(f"#### {track.replace('_', ' ').title()}")
    methods_df = pd.DataFrame(
        [
            {
                "Method": method["name"],
                "Objective": method["objective"],
                "Signals": ", ".join(method["signals"]),
            }
            for method in methods
        ]
    )
    st.dataframe(methods_df, use_container_width=True, hide_index=True)

st.subheader("Rewards")
for reward in rewards_catalogue():
    st.markdown(f"- {reward}")
