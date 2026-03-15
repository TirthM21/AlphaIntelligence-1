from datetime import date

import pandas as pd
import streamlit as st

from src.research.crowwd_closing_bell import (
    ClosingBellConfig,
    build_timeline,
    rewards_catalogue,
    simulation_snapshot,
)


st.set_page_config(page_title="Crowwd: The Closing Bell", layout="wide")

cfg = ClosingBellConfig()
st.title("🔔 Crowwd: The Closing Bell")
st.caption("Hosted on Crowwd — India's only social platform for investors")

st.markdown(
    """
The financial year doesn't just end — it settles accounts.

Trade through the most consequential month on India's market calendar.
**Close FY25. Open FY26.**
"""
)

st.subheader("Simulation Details")
d1, d2, d3, d4 = st.columns(4)
d1.metric("Format", cfg.format)
d2.metric("Virtual Capital", "₹10,00,000")
d3.metric("Universe", cfg.universe)
d4.metric("Window", "Mar 15 – Apr 15")

st.subheader("Live Progress Snapshot")
as_of = st.date_input("View date", value=date.today())
snapshot = simulation_snapshot(as_of=as_of, config=cfg)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Phase", snapshot["phase"].replace("-", " ").title())
c2.metric("Elapsed Days", f"{snapshot['elapsed_days']} / {snapshot['total_days']}")
c3.metric("Progress", f"{snapshot['progress_pct']}%")
c4.metric("Days to Close", snapshot["days_to_close"])

if snapshot["next_milestone"]:
    nxt = snapshot["next_milestone"]
    st.info(f"Next milestone: **{nxt['name']}** ({nxt['date']}) — {nxt['description']}")

st.subheader("Timeline")
timeline_df = pd.DataFrame(
    [
        {"Milestone": m.name, "Date": m.day.isoformat(), "Description": m.description}
        for m in build_timeline(cfg)
    ]
)
st.dataframe(timeline_df, use_container_width=True, hide_index=True)

st.subheader("Why the Year-End Window Matters")
st.write(
    "Institutional money repositions, advance-tax deadlines hit, and new FY allocations begin. "
    "The leaderboard reflects how quickly and accurately you read those shifts in real time."
)

st.subheader("Rewards")
for reward in rewards_catalogue():
    st.markdown(f"- {reward}")

st.success(
    "31 days. One financial year transition. If you believe you belong where real financial decisions are made, prove it here first."
)
