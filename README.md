# 🏦 AlphaIntelligence Capital | Systematic Alpha Engine

> **A high-fidelity quantitative hedge fund framework for identifying high-probability Stage 2 momentum breakouts and elite multi-year fundamental compounders.**

---

## 🚀 Vision
AlphaIntelligence Capital is a **systematic hedge fund engine** — not a simple stock screener. It eliminates emotional trading through rigorous institutional-grade quantitative analysis:
1.  **Technical Precision**: Minervini Trend Template & 4-Stage Market Regime Classification.
2.  **Fundamental Dominance**: Deep-dive analytics into revenue quality, margin expansion, and inventory dynamics.
3.  **Risk Management**: Automated position sizing, logical stop-loss placement, and R/R optimization.
4.  **AI Integration**: Narrative generation and thesis synthesis via High-Performance LLMs.
5.  **Automated Delivery**: Institutional-quality newsletter delivery to fund subscribers via encrypted SMTP.

---

## 🛠️ The Dual-System Architecture
Our framework bridges two worlds: the **Daily Momentum Alpha** and the **Quarterly Compounder Strategy**.

### ⚡ System 1: Short-Term Alpha Generation (Daily)
*   **Target**: 2-8 week holding periods.
*   **Strategy**: Specific Entry Point Analysis (SEPA).
*   **Entry**: Phase 2 momentum breakouts with high Relative Strength (RS).
*   **Exit**: 50-SMA violations or Phase 3 distribution signals.

### 🏛️ System 2: Long-Term Wealth Compounding (Quarterly)
*   **Target**: 5-10 year wealth building.
*   **Strategy**: Growth Quality & Capital Efficiency (60/25/15 Formula).
*   **Focus**: Widening moats, pricing power, and institutional leadership.
*   **Portfolio**: Thematic concentration with strict diversification rules.

---

## 📂 Core Documentation
1.  **`README.md`**: Fund overview and landing page.
2.  **`SYSTEM_OVERVIEW.md`**: Technical architecture, module breakdown, and developer reference.
3.  **`SIMPLE_TRADE_TRACKER.md`**: The essential Google Sheets template for tracking alpha generation.

---

## 🏁 Quick Start

### 1. Installation
```powershell
# Install core dependencies
pip install -r requirements.txt
```

### 2. Configure Environment
Create a `.env` file with your keys:
```env
FREE_LLM_API_KEY=your_key_here
EMAIL_SENDER=your_gmail@gmail.com
EMAIL_PASSWORD=your_gmail_app_password
EMAIL_RECIPIENT=recipient@email.com
```

### 3. Execution
| Goal | Command |
| :--- | :--- |
| **Daily Market Scan** | `python run_optimized_scan.py --limit 50` |
| **Scan + Email Delivery** | `python run_optimized_scan.py --limit 50 --send-email` |
| **System Diagnostics** | `python run_optimized_scan.py --diagnostics` |
| **Test Email Pipeline** | `python test_email_full.py` |
| **Standalone Technical Signals Scan** | `python run_technical_signals_scan.py --workers 8 --send-email` |
| **F&O Tracking Dashboard** | `python run_derivatives_dashboard.py --symbols nifty,banknifty,reliance` |
| **Backtesting Streamlit Dashboard** | `./run_backtesting_dashboard.sh` (uses `dashboard/backtesting_dashboard.py`) |
| **Backtesting + Email Workflow** | `python run_backtesting_workflow.py --symbols RELIANCE,HDFCBANK,TCS,INFY --send-email` |
| **Walk-forward Experiment Runner** | `python run_walk_forward_experiments.py --symbol RELIANCE --years 5 --db-path experiments/metrics.db` |




## 📰 Newsletter Data Sources & Priority
The newsletter pipeline now reads provider priority from `config.yaml` under `newsletter.providers` and enforces this order at runtime.

```yaml
newsletter:
  providers:
    macro: [yfinance]
    headlines: [yfinance]
    sector_performance: [yfinance]
    prices: [yfinance]
```

### Runtime behavior
- Providers are tried in configured order per section (`macro`, `headlines`, `sector_performance`, `prices`).
- Disabled or misconfigured providers (for example, missing API keys) are skipped automatically.
- On startup, `NewsletterGenerator` logs explicit diagnostics that include:
  - section fallback order,
  - active providers detected for each section,
  - providers missing required API keys.

### API key requirements
- `yfinance` does not require an API key

If a section has no active providers at runtime, newsletter generation continues with safe fallback content where possible.

---
# AlphaIntelligence Capital
