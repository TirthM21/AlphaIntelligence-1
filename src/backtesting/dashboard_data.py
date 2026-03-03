"""Backtesting data + strategy helpers for Streamlit dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json

import numpy as np
import pandas as pd

try:
    from nse import NSE
except Exception:  # pragma: no cover - optional runtime dependency
    NSE = None

CACHE_DIR = Path("data/backtests/cache")
RESULTS_DIR = Path("data/backtests/results")


@dataclass
class BacktestMetrics:
    strategy: str
    symbol: str
    total_return: float
    cagr: float
    sharpe: float
    max_drawdown: float
    trades: int


def _to_iso(d: date) -> str:
    return d.isoformat()


def _load_price_from_cache(symbol: str, from_date: date, to_date: date) -> Optional[pd.DataFrame]:
    cache_file = CACHE_DIR / f"{symbol}_{_to_iso(from_date)}_{_to_iso(to_date)}.csv"
    if cache_file.exists():
        df = pd.read_csv(cache_file, parse_dates=["date"])
        return df.sort_values("date")
    return None


def _save_price_to_cache(df: pd.DataFrame, symbol: str, from_date: date, to_date: date) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{symbol}_{_to_iso(from_date)}_{_to_iso(to_date)}.csv"
    df.to_csv(cache_file, index=False)


def fetch_price_history(symbol: str, from_date: date, to_date: date, download_folder: str = "data/nse_downloads") -> pd.DataFrame:
    """Load OHLCV from cache, fallback to NSE API, then cache results."""
    cached = _load_price_from_cache(symbol, from_date, to_date)
    if cached is not None and not cached.empty:
        return cached

    if NSE is None:
        raise ImportError("nse package not installed. Install `nse[local]`.")

    with NSE(download_folder=download_folder, server=False, timeout=30) as nse:
        rows = nse.fetch_equity_historical_data(symbol=symbol, from_date=from_date, to_date=to_date, series="EQ")

    if not rows:
        raise RuntimeError(f"No NSE history returned for {symbol}")

    df = pd.DataFrame(rows)
    rename_map = {
        "CH_TIMESTAMP": "date",
        "mTIMESTAMP": "date",
        "CH_CLOSING_PRICE": "close",
        "CH_OPENING_PRICE": "open",
        "CH_TRADE_HIGH_PRICE": "high",
        "CH_TRADE_LOW_PRICE": "low",
        "CH_TOT_TRADED_QTY": "volume",
    }
    df = df.rename(columns=rename_map)
    needed = ["date", "open", "high", "low", "close", "volume"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise RuntimeError(f"NSE payload missing columns for {symbol}: {missing}")

    df = df[needed].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna().sort_values("date")
    _save_price_to_cache(df, symbol, from_date, to_date)
    return df


def fetch_nse_equity_universe() -> List[str]:
    """Fetch all NSE equity symbols via official CSV with safe fallbacks."""
    urls = [
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    ]
    for url in urls:
        try:
            df = pd.read_csv(url)
            if "SYMBOL" in df.columns:
                symbols = [str(s).strip().upper() for s in df["SYMBOL"].dropna().tolist()]
                symbols = [s for s in symbols if s and s not in {"NIFTY", "NA"}]
                if symbols:
                    return sorted(set(symbols))
        except Exception:
            continue


    cache_candidates = [
        Path("data/cache/nse_equity_universe.pkl"),
        Path("data/cache/nse_stock_universe.pkl"),
    ]
    for cache_file in cache_candidates:
        if cache_file.exists():
            try:
                import pickle

                payload = pickle.loads(cache_file.read_bytes())
                symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
                cleaned = []
                for sym in symbols:
                    s = str(sym).replace(".NS", "").strip().upper()
                    if s:
                        cleaned.append(s)
                if cleaned:
                    return sorted(set(cleaned))
            except Exception:
                continue

    if NSE is not None:
        index_fallbacks = ["NIFTY 500", "NIFTY MIDCAP 150", "NIFTY SMALLCAP 250"]
        merged: List[str] = []
        try:
            with NSE(download_folder="data/nse_downloads", server=False, timeout=30) as nse:
                for index_name in index_fallbacks:
                    try:
                        response = nse.listEquityStocksByIndex(index_name)
                        if isinstance(response, dict) and "data" in response:
                            merged.extend(
                                str(item.get("symbol", "")).strip().upper()
                                for item in response["data"]
                                if isinstance(item, dict)
                            )
                    except Exception:
                        continue
        except Exception:
            pass

        merged = [s for s in merged if s]
        if merged:
            return sorted(set(merged))

    return []


def scalar_kalman_filter(values: np.ndarray, q: float = 1e-4, r: float = 1e-2) -> np.ndarray:
    est = np.zeros(len(values))
    p = np.zeros(len(values))
    est[0] = values[0]
    p[0] = 1.0
    for i in range(1, len(values)):
        est[i] = est[i - 1]
        p[i] = p[i - 1] + q
        k = p[i] / (p[i] + r)
        est[i] = est[i] + k * (values[i] - est[i])
        p[i] = (1 - k) * p[i]
    return est


def sepa_vcp_signal(prices: pd.DataFrame) -> pd.Series:
    close = prices["close"]
    vol = prices["volume"]
    sma50 = close.rolling(50).mean()
    sma150 = close.rolling(150).mean()
    sma200 = close.rolling(200).mean()
    high_52w = close.rolling(252).max()
    low_52w = close.rolling(252).min()

    trend_ok = (close > sma50) & (sma50 > sma150) & (sma150 > sma200)
    range_ok = (close >= 0.75 * high_52w) & (close >= 1.3 * low_52w)

    ret = close.pct_change()
    vol20 = ret.rolling(20).std()
    vol60 = ret.rolling(60).std()
    contraction_ok = vol20 < vol60
    breakout = close >= close.rolling(20).max().shift(1)
    volume_ok = vol > 1.2 * vol.rolling(20).mean()

    return (trend_ok & range_ok & contraction_ok & breakout & volume_ok).fillna(False)


def backtest_long_only(prices: pd.DataFrame, signal: pd.Series) -> Tuple[pd.Series, int]:
    daily_ret = prices["close"].pct_change().fillna(0)
    position = signal.shift(1).astype("boolean").fillna(False).astype(float)
    strat_ret = daily_ret * position
    trades = int((position.diff() > 0).sum())
    equity = (1 + strat_ret).cumprod()
    return equity, trades


def kalman_dynamic_hedge_backtest(y: pd.Series, x: pd.Series, entry_z: float = 2.0, exit_z: float = 0.5) -> Tuple[pd.Series, int]:
    ratio = (y / x.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).ffill().bfill()
    beta = pd.Series(scalar_kalman_filter(ratio.to_numpy()), index=y.index)
    spread = y - beta * x
    z = (spread - spread.rolling(60).mean()) / spread.rolling(60).std()

    position = pd.Series(0.0, index=y.index)
    position[z > entry_z] = -1.0
    position[z < -entry_z] = 1.0
    position[(z.abs() < exit_z)] = 0.0
    position = position.where(position != 0).ffill().fillna(0)

    yret = y.pct_change().fillna(0)
    xret = x.pct_change().fillna(0)
    pnl = position.shift(1).fillna(0) * (yret - xret)
    equity = (1 + pnl).cumprod()
    trades = int((position.diff().abs() > 0).sum())
    return equity, trades


def _metric_from_equity(equity: pd.Series, trades: int, strategy: str, symbol: str) -> BacktestMetrics:
    rets = equity.pct_change().dropna()
    total_return = float(equity.iloc[-1] - 1)
    years = max(len(equity) / 252, 1 / 252)
    cagr = float((equity.iloc[-1] ** (1 / years)) - 1)
    sharpe = float(np.sqrt(252) * rets.mean() / rets.std()) if rets.std() and not np.isnan(rets.std()) else 0.0
    dd = equity / equity.cummax() - 1
    max_dd = float(dd.min()) if not dd.empty else 0.0
    return BacktestMetrics(strategy, symbol, total_return, cagr, sharpe, max_dd, trades)


def run_backtests(symbols: List[str], benchmark_symbol: str = "NIFTY 50") -> Dict[str, object]:
    to_date = date.today()
    from_date = to_date - timedelta(days=365 * 3)

    equities: Dict[str, pd.Series] = {}
    rows: List[Dict[str, object]] = []
    errors: List[str] = []
    usable_symbols: List[str] = []

    for symbol in symbols:
        try:
            px = fetch_price_history(symbol, from_date, to_date)
            if px.empty or len(px) < 252:
                errors.append(f"{symbol}: insufficient history for backtest")
                continue
            signal = sepa_vcp_signal(px)
            eq, trades = backtest_long_only(px, signal)
            equities[f"SEPA_VCP_{symbol}"] = pd.Series(eq.values, index=px["date"])
            m = _metric_from_equity(eq, trades, "SEPA_VCP", symbol)
            rows.append(m.__dict__)
            usable_symbols.append(symbol)
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")

    if len(usable_symbols) >= 2:
        try:
            p1 = fetch_price_history(usable_symbols[0], from_date, to_date)
            p2 = fetch_price_history(usable_symbols[1], from_date, to_date)
            merged = p1[["date", "close"]].merge(p2[["date", "close"]], on="date", suffixes=("_a", "_b")).dropna()
            if len(merged) >= 120:
                eq_k, trades_k = kalman_dynamic_hedge_backtest(merged["close_a"], merged["close_b"])
                equities[f"KALMAN_PAIR_{usable_symbols[0]}_{usable_symbols[1]}"] = pd.Series(eq_k.values, index=merged["date"])
                m = _metric_from_equity(eq_k, trades_k, "KALMAN_PAIR", f"{usable_symbols[0]}/{usable_symbols[1]}")
                rows.append(m.__dict__)
        except Exception as exc:
            errors.append(f"Kalman pair failed: {exc}")

    df_metrics = pd.DataFrame(rows)
    if not df_metrics.empty:
        df_metrics = df_metrics.sort_values("sharpe", ascending=False)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = RESULTS_DIR / f"backtest_results_{stamp}.json"
    out_csv = RESULTS_DIR / f"backtest_metrics_{stamp}.csv"

    payload = {
        "generated_at": datetime.now().isoformat(),
        "symbols": symbols,
        "processed_symbols": usable_symbols,
        "errors": errors,
        "metrics": df_metrics.to_dict(orient="records"),
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    df_metrics.to_csv(out_csv, index=False)

    return {
        "metrics": df_metrics,
        "equities": equities,
        "json": str(out_json),
        "csv": str(out_csv),
        "errors": errors,
        "processed_symbols": usable_symbols,
    }


def compute_piotroski_proxy(symbols: List[str], window: int = 252) -> pd.DataFrame:
    """Proxy F-score using available price/volume quality signals (0-9 scale)."""
    to_date = date.today()
    from_date = to_date - timedelta(days=365 * 2)
    records = []

    for symbol in symbols:
        try:
            px = fetch_price_history(symbol, from_date, to_date)
            if px.empty or len(px) < 200:
                continue
            close = px["close"]
            ret = close.pct_change()
            vol = px["volume"]

            score = 0
            score += int(ret.tail(window).mean() > 0)
            score += int(ret.tail(63).mean() > ret.tail(126).mean())
            score += int(close.iloc[-1] > close.rolling(200).mean().iloc[-1])
            score += int(close.rolling(50).mean().iloc[-1] > close.rolling(200).mean().iloc[-1])
            score += int(vol.tail(20).mean() > vol.tail(120).mean())
            score += int(ret.tail(20).std() < ret.tail(120).std())
            score += int(close.iloc[-1] > close.rolling(252).max().iloc[-2] * 0.9 if len(close) > 253 else 0)
            score += int((ret.tail(20) > 0).sum() > 11)
            score += int((ret.tail(5) > 0).sum() >= 3)

            records.append({"symbol": symbol, "piotroski_proxy_f_score": int(score)})
        except Exception:
            continue

    if not records:
        return pd.DataFrame(columns=["symbol", "piotroski_proxy_f_score"])
    return pd.DataFrame(records).sort_values("piotroski_proxy_f_score", ascending=False)
