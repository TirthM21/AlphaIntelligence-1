"""Streamlit dashboard for SEPA/VCP, Kalman hedge ratio, and Piotroski proxy screens."""

from __future__ import annotations

import streamlit as st
import pandas as pd

from src.backtesting.dashboard_data import compute_piotroski_proxy, run_backtests

st.set_page_config(page_title="AlphaIntelligence Backtesting Dashboard", layout="wide")

st.title("📈 Backtesting Dashboard — SEPA/VCP + Kalman + Piotroski")
st.caption("Uses cache-first NSE historical data. If cache is missing, data is downloaded then cached.")

with st.sidebar:
    st.header("Run Controls")
    symbols_raw = st.text_input("Symbols (comma-separated NSE symbols)", value="RELIANCE,HDFCBANK,TCS,INFY")
    symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]
    run_btn = st.button("Run backtests")

if run_btn:
    if len(symbols) < 2:
        st.error("Please provide at least 2 symbols.")
    else:
        with st.spinner("Running strategies..."):
            data = run_backtests(symbols=symbols)
            piotroski_df = compute_piotroski_proxy(symbols)

        st.success("Backtests complete.")

        metrics = data["metrics"]
        st.subheader("Strategy Metrics")
        st.dataframe(metrics, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            st.metric("Top Sharpe", f"{metrics.iloc[0]['sharpe']:.2f}")
        with c2:
            st.metric("Best Total Return", f"{metrics.iloc[0]['total_return']:.2%}")

        st.subheader("Equity Curves")
        eq_df = pd.concat(data["equities"], axis=1)
        st.line_chart(eq_df)

        st.subheader("Piotroski Proxy F-Score Screen")
        st.dataframe(piotroski_df, use_container_width=True)

        st.info(f"Saved metrics CSV: {data['csv']}\n\nSaved JSON payload: {data['json']}")

st.markdown("---")
st.markdown("### Email workflow\nUse existing pipeline to email outputs (newsletter + attachments) after generating these artifacts.")
