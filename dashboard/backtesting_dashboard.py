"""Streamlit dashboard for SEPA/VCP, Kalman hedge ratio, and Piotroski proxy screens."""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from src.monitoring.drift import load_drift_dashboard_payload
from src.utils.logging_config import configure_logging

from src.backtesting.dashboard_data import (
    compute_piotroski_proxy,
    fetch_nse_equity_universe,
    run_backtests,
)

configure_logging(level=os.getenv("LOG_LEVEL", "INFO"), json_logs=os.getenv("LOG_FORMAT", "text").lower() == "json")

st.set_page_config(page_title="AlphaIntelligence Backtesting Dashboard", layout="wide")

st.title("📈 Backtesting Dashboard — SEPA/VCP + Kalman + Piotroski")
st.caption("Uses cache-first NSE historical data. If cache is missing, data is downloaded then cached.")

with st.sidebar:
    st.header("Run Controls")
    use_full_universe = st.checkbox("Run on full NSE equity universe", value=False)
    symbols_raw = st.text_input(
        "Symbols (comma-separated NSE symbols)",
        value="RELIANCE,HDFCBANK,TCS,INFY",
        disabled=use_full_universe,
    )
    max_symbols = st.number_input("Max symbols to process", min_value=10, max_value=2500, value=300, step=10)
    symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]
    run_btn = st.button("Run backtests")

if run_btn:
    if use_full_universe:
        with st.spinner("Fetching NSE equity universe..."):
            universe_symbols = fetch_nse_equity_universe()
        if not universe_symbols:
            st.error("Could not fetch NSE universe symbols. Check network/connectivity and retry.")
            st.stop()
        symbols = universe_symbols[: int(max_symbols)]
        st.info(f"Loaded {len(universe_symbols)} NSE symbols. Processing first {len(symbols)} symbols.")

    if len(symbols) < 2:
        st.error("Please provide at least 2 symbols.")
    else:
        with st.spinner("Running strategies..."):
            data = run_backtests(symbols=symbols)
            piotroski_df = compute_piotroski_proxy(symbols)

        st.success("Backtests complete.")

        metrics = data["metrics"]
        st.subheader("Strategy Metrics")
        if metrics.empty:
            st.warning("No strategy metrics were generated. Data may be unavailable for selected symbols/date range.")
        else:
            st.dataframe(metrics, use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                st.metric("Top Sharpe", f"{metrics.iloc[0]['sharpe']:.2f}")
            with c2:
                st.metric("Best Total Return", f"{metrics.iloc[0]['total_return']:.2%}")

        st.subheader("Equity Curves")
        if data["equities"]:
            eq_df = pd.concat(data["equities"], axis=1)
            st.line_chart(eq_df)
        else:
            st.warning("No equity curves to display.")

        st.subheader("Piotroski Proxy F-Score Screen")
        if piotroski_df.empty:
            st.warning("No Piotroski proxy rows were computed.")
        else:
            st.dataframe(piotroski_df, use_container_width=True)

        if data.get("errors"):
            st.subheader("Symbol Processing Errors")
            st.caption("These symbols were skipped due to missing/insufficient data or API failures.")
            st.dataframe(pd.DataFrame({"error": data["errors"]}), use_container_width=True)



        st.subheader("Data Drift Monitoring")
        drift_path = "data/monitoring/drift_snapshots.jsonl"
        drift_payload = load_drift_dashboard_payload(drift_path, limit=60)
        drift_summary = drift_payload["summary"]
        if drift_summary["num_snapshots"] == 0:
            st.info("No drift snapshots available yet.")
        else:
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Drift Snapshots", drift_summary["num_snapshots"])
            with c2:
                st.metric("Latest Drift Date", drift_summary["latest_date"])
            with c3:
                st.metric("Active Drift Alerts", drift_summary["active_alerts"])

            drift_timeseries = drift_payload["timeseries"]
            st.dataframe(drift_timeseries, use_container_width=True)

        st.info(
            f"Saved metrics CSV: {data['csv']}\n\n"
            f"Saved JSON payload: {data['json']}\n\n"
            f"Processed symbols: {len(data.get('processed_symbols', []))}/{len(symbols)}"
        )

st.markdown("---")
st.markdown("### Email workflow\nUse existing pipeline to email outputs (newsletter + attachments) after generating these artifacts.")
