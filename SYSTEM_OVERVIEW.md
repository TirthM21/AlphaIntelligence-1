# 🏦 AlphaIntelligence Capital — System Overview

This document provides a comprehensive breakdown of the AlphaIntelligence Capital systematic hedge fund engine architecture, module responsibilities, and operational metrics.

---


## 🌍 Market Target & Multi-Market Roadmap
- **Production target (current): NSE (India)**.
  - Universe, benchmark regime analysis, and default examples are NSE-aligned.
- **US market status:** available only as legacy references in a few docs/config snippets; not the production path.
- **Multi-market support:** **planned** via provider abstraction (details below).

## 🚀 Core Entry Points
| File | Description |
| :--- | :--- |
| `run_optimized_scan.py` | **Main Engine**. Coordinates the full market scan, signal detection, report generation, and email delivery. |
| `comprehensive_system_test.py` | **Diagnostic Tool**. Tests major modules (DB, Scanner, Notifications) to ensure system health. |

---

## 📂 System Architecture (`src/`)

### 📊 Data Acquisition (`src/data/`)
*   **`universe_fetcher.py`**: Fetches the full list of NSE-listed equities (with ETF toggle support) and maintains cached universe snapshots.
*   **`enhanced_fundamentals.py`**: Fundamental aggregation engine with robust fallback handling.
*   **`fundamentals_fetcher.py`**: Core logic for calculating YoY/QoQ growth, inventory signals, and margin expansion.
*   **`git_storage_fetcher.py`**: Specialized storage layer for persistent tracking of fundamental changes.

### 🔍 Screening & Strategy (`src/screening/`)
*   **`optimized_batch_processor.py`**: The parallel execution core. Manages thread pools and rate limits to achieve 10-25 TPS safely.
*   **`signal_engine.py`**: **The Brain**. Implements Minervini Trend Templates and quantitative scoring (0-100) for Buy/Sell signals.
*   **`phase_indicators.py`**: Technical analysis layer that classifies stocks into Phase 1 (Base), Phase 2 (Uptrend), Phase 3 (Top), or Phase 4 (Downtrend).
*   **`benchmark.py`**: Analyzes market regime (Risk-On/Off) via NIFTY trend and market breadth (Advance/Decline).

### 📈 Reporting & Notifications (`src/reporting/` & `src/notifications/`)
*   **`newsletter_generator.py`**: Compiles technicals, fundamentals, and market context into a professional Markdown newsletter.
*   **`portfolio_manager.py`**: Generates institutional reports: Allocation Plans, Ownership Tracking, and Rebalance Actions.
*   **`email_notifier.py`**: Institutional email delivery system for sending fund research and alerts to subscribers via encrypted SMTP.

---


### 🧩 Planned Multi-Market Architecture (Pluggable)
To support NSE + US (and additional exchanges) without strategy drift, the architecture roadmap introduces pluggable market modules:

1. **Universe Provider Interface**
   - Contract: `fetch_universe()`, `get_benchmark_symbol()`, `normalize_symbol()`, and metadata for exchange/asset class.
   - Implementations (planned): `NSEUniverseProvider`, `USUniverseProvider`.
   - Runtime selection via config/CLI (`market: nse|us`).

2. **Market Context Provider**
   - Encapsulates benchmark, breadth inputs, holidays, and session assumptions per market.
   - Keeps risk-on/off and phase logic reusable while injecting market-specific inputs.

3. **Indicator Profile Registry**
   - Shared core indicators (trend template, phase model, momentum).
   - Market-specific overlays (e.g., symbol suffixing, benchmark symbol, derivatives-specific metrics).
   - Objective: avoid hard-coding `^NSEI`/`SPY` logic in strategy orchestration.

4. **Command/Workflow Consistency Rules**
   - All user-facing command examples should either:
     - be explicitly NSE production defaults, or
     - pass `--market` when multi-market mode becomes GA.

## 📏 Operational Metrics
*   **Market Universe (production)**: NSE equity universe via exchange fetch + cache.
*   **Legacy reference**: older US-universe docs/examples are being phased out in favor of explicit market-target labeling.
*   **Processing Speed**: 
    *   *Aggressive*: ~15-20 TPS (5-8 minutes full scan).
    *   *Optimized/PC-Safe*: ~2-5 TPS (15-25 minutes full scan).
*   **Signal Thresholds**:
    *   **Buy**: Requires Phase 2 + Minervini Template + Fundamental Score > 70.
    *   **Sell**: Triggered by Phase 3 transitions or >20% drawdown from peak.

---

## 🛠️ Usage Quick Reference
### Running a Safe Scan (Recommended for Home PCs)
```powershell
python run_optimized_scan.py --limit 50 --workers 1 --delay 2.0  # NSE production default
```

### Running System Diagnostics
```powershell
python comprehensive_system_test.py
```

### Logging Configuration
Use centralized logging bootstrap (`src/utils/logging_config.py`) from executable entrypoints.

- Environment variables:
  - `LOG_LEVEL` → `DEBUG|INFO|WARNING|ERROR|CRITICAL` (default: `INFO`)
  - `LOG_FORMAT` → `text|json` (default: `text`)
- CLI flags (supported by primary entry scripts):
  - `--log-level INFO`
  - `--json-logs`

Examples:
```powershell
python run_optimized_scan.py --log-level DEBUG --json-logs
python run_derivatives_dashboard.py --log-level INFO
python run_backtesting_workflow.py --log-level WARNING
```

---
*Last Updated: March 4, 2026*
