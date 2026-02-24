"""Chart generation utilities for AlphaIntelligence newsletters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf

from .chart_style import LIGHT_CHART_STYLE


@dataclass
class ChartArtifact:
    """Metadata used when embedding chart references in markdown/email."""

    key: str
    title: str
    caption: str
    path: str


class MarketVisualizer:
    """Generate professional financial charts for newsletters."""

    def __init__(self, output_dir: str = "./data/reports/charts"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.style = LIGHT_CHART_STYLE
        self._apply_theme()

    def _apply_theme(self) -> None:
        plt.style.use("default")
        plt.rcParams.update(
            {
                "font.family": self.style.font_family,
                "axes.facecolor": self.style.palette["axes"],
                "figure.facecolor": self.style.palette["background"],
                "axes.edgecolor": self.style.palette["grid"],
                "axes.labelcolor": self.style.palette["text"],
                "axes.titlesize": self.style.title_size,
                "axes.labelsize": self.style.label_size,
                "xtick.color": self.style.palette["muted_text"],
                "ytick.color": self.style.palette["muted_text"],
                "xtick.labelsize": self.style.tick_size,
                "ytick.labelsize": self.style.tick_size,
                "text.color": self.style.palette["text"],
            }
        )

    def _build_chart_path(self, chart_name: str) -> Path:
        stamp = datetime.now().strftime("%Y%m%d")
        return self.output_dir / f"{stamp}_{chart_name}.png"

    def _polish_axes(self, ax, *, grid_axis: str = "y") -> None:
        """Apply consistent readability improvements across charts."""
        if grid_axis in {"x", "both"}:
            ax.grid(axis="x", color=self.style.palette["grid"], **self.style.grid_style)
        if grid_axis in {"y", "both"}:
            ax.grid(axis="y", color=self.style.palette["grid"], **self.style.grid_style)
        ax.tick_params(axis="both", which="major", pad=6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    @staticmethod
    def _to_float(value, default: float = 0.0) -> float:
        """Best-effort scalar conversion tolerant to pandas/numpy containers."""
        try:
            if hasattr(value, "item"):
                value = value.item()
        except Exception:
            pass

        if isinstance(value, pd.Series):
            if value.empty:
                return default
            value = value.iloc[-1]
        elif isinstance(value, pd.DataFrame):
            if value.empty:
                return default
            value = value.iloc[-1, -1]

        try:
            return float(value)
        except Exception:
            return default


    def available_chart_keys(self) -> List[str]:
        """Return supported daily chart keys for AI-driven selection."""
        return [
            "sp500_decline_vs_return",
            "market_breadth_snapshot",
            "sector_leadership",
            "seasonality_context",
            "market_cap_leadership",
            "notable_movers",
        ]

    def generate_default_charts(
        self,
        index_perf: Dict[str, float],
        sector_perf: List[Dict],
        market_status: Dict,
        cap_perf: Dict[str, float] | None = None,
        notable_movers: List[Dict] | None = None,
        selected_keys: List[str] | None = None,
    ) -> List[ChartArtifact]:
        """Build default chart suite, optionally constrained to selected keys."""
        artifacts: List[ChartArtifact] = []
        keys = selected_keys or self.available_chart_keys()

        chart_specs = [
            (
                "sp500_decline_vs_return",
                lambda: self.generate_sp500_decline_vs_return_chart(),
                "S&P 500 Intra-Year Declines vs Calendar-Year Returns",
                "Annual returns (bars) versus maximum intra-year drawdowns (red dots) since 1980.",
            ),
            (
                "market_breadth_snapshot",
                lambda: self.generate_market_breadth_snapshot(index_perf=index_perf, market_status=market_status),
                "Market Breadth & Index Performance Snapshot",
                "Breadth metrics and benchmark index moves for the latest session.",
            ),
            (
                "sector_leadership",
                lambda: self.generate_sector_leadership_chart(sector_data=sector_perf),
                "Sector Leadership",
                "Ranked sector leadership based on daily percentage performance.",
            ),
            (
                "seasonality_context",
                lambda: self.generate_seasonality_context_chart(ticker="SPY"),
                "Seasonality Context (Current Month vs Other Months)",
                "Average monthly return context for SPY over the last 10 years.",
            ),
            (
                "market_cap_leadership",
                lambda: self.generate_market_cap_leadership_chart(cap_perf=cap_perf or {}),
                "Market-Cap Leadership",
                "Large/Mid/Small-cap leadership for the current session.",
            ),
            (
                "notable_movers",
                lambda: self.generate_notable_movers_chart(movers=notable_movers or []),
                "Best & Worst Movers",
                "Top absolute movers from the daily flow monitor.",
            ),
        ]

        figure_num = 1
        for key, fn, title, caption in chart_specs:
            if key not in keys:
                continue
            path = fn()
            if path:
                artifacts.append(
                    ChartArtifact(
                        key=key,
                        title=title,
                        caption=f"Figure {figure_num}. {caption}",
                        path=path,
                    )
                )
                figure_num += 1

        return artifacts

    def generate_sp500_decline_vs_return_chart(self) -> str:
        """Create a dense annual bar/drawdown chart inspired by institutional market notes."""
        try:
            hist = yf.download("^GSPC", start="1980-01-01", auto_adjust=True, progress=False)
        except Exception:
            return ""
        if hist.empty or "Close" not in hist.columns:
            return ""

        close = hist["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.squeeze(axis=1) if close.shape[1] == 1 else close.iloc[:, 0]
        close = close.dropna()
        yearly_rows = []
        for year, series in close.groupby(close.index.year):
            if len(series) < 5:
                continue
            annual_return = (series.iloc[-1] / series.iloc[0] - 1.0) * 100.0
            rolling_peak = series.cummax()
            drawdown = ((series / rolling_peak) - 1.0) * 100.0
            max_decline = float(drawdown.min())
            yearly_rows.append((year, annual_return, max_decline))

        if not yearly_rows:
            return ""

        df = pd.DataFrame(yearly_rows, columns=["year", "annual_return", "max_decline"])
        years = df["year"].astype(int).tolist()
        x = list(range(len(df)))
        annual = df["annual_return"].tolist()
        declines = df["max_decline"].tolist()

        fig, ax = plt.subplots(figsize=(14.5, 7.4))
        fig.patch.set_facecolor("#e6e6e6")
        ax.set_facecolor("#e6e6e6")

        bars = ax.bar(x, annual, color="#595f63", width=0.72, alpha=0.95, zorder=2)
        ax.scatter(x, declines, color="#c81e1e", s=28, zorder=3, edgecolors="white", linewidth=0.5)

        ax.axhline(0, color="#242424", linewidth=1.1, zorder=1)
        ax.set_ylim(-60, max(42, max(annual) + 5))
        self._polish_axes(ax, grid_axis="y")

        for idx, value in enumerate(annual):
            ax.text(
                idx,
                value + (1.2 if value >= 0 else -2.0),
                f"{value:.0f}%",
                ha="center",
                va="bottom" if value >= 0 else "top",
                fontsize=8,
                color="#3f3f3f",
                fontweight="bold",
            )
        for idx, value in enumerate(declines):
            ax.text(idx, value - 1.3, f"{value:.0f}%", ha="center", va="top", fontsize=8, color="#c81e1e", fontweight="bold")

        tick_idx = [i for i, y in enumerate(years) if y % 5 == 0 or i == len(years) - 1]
        ax.set_xticks(tick_idx)
        ax.set_xticklabels([f"'{str(years[i])[-2:]}" for i in tick_idx])
        ax.set_yticks([-60, -40, -20, 0, 20, 40])
        ax.set_yticklabels([f"{t:.0f}%" for t in [-60, -40, -20, 0, 20, 40]])

        avg_decline = abs(df["max_decline"].mean())
        positive_years = int((df["annual_return"] > 0).sum())
        total_years = len(df)
        ax.set_title("S&P 500 intra-year declines vs. calendar year returns", loc="left", fontsize=16, pad=12, fontweight="bold")
        ax.text(
            0.0,
            1.02,
            f"Despite average intra-year drops of {avg_decline:.1f}%, annual returns were positive in {positive_years} of {total_years} years",
            transform=ax.transAxes,
            fontsize=11,
            color="#1f2937",
        )

        fig.tight_layout()
        output_path = self._build_chart_path("sp500_decline_vs_return")
        fig.savefig(output_path, dpi=self.style.dpi, bbox_inches="tight")
        plt.close(fig)
        return str(output_path)

    def generate_market_cap_leadership_chart(self, cap_perf: Dict[str, float]) -> str:
        if not cap_perf:
            return ""

        labels = list(cap_perf.keys())
        values = [self._to_float(cap_perf.get(k), 0.0) for k in labels]
        colors = [self.style.palette["positive"] if v >= 0 else self.style.palette["negative"] for v in values]

        fig, ax = plt.subplots(figsize=(8, 4.6))
        bars = ax.bar(labels, values, color=colors, alpha=0.9)
        ax.axhline(0, color=self.style.palette["grid"], linewidth=1)
        self._polish_axes(ax, grid_axis="y")
        ax.set_title("Market-Cap Leadership")
        ax.set_ylabel("Return %")

        for idx, bar in enumerate(bars):
            value = values[idx]
            ax.text(bar.get_x() + bar.get_width() / 2, value + (0.12 if value >= 0 else -0.18), f"{value:+.2f}%", ha="center", fontsize=9)

        fig.tight_layout()
        output_path = self._build_chart_path("market_cap_leadership")
        fig.savefig(output_path, dpi=self.style.dpi, bbox_inches="tight")
        plt.close(fig)
        return str(output_path)

    def generate_notable_movers_chart(self, movers: List[Dict]) -> str:
        if not movers:
            return ""

        normalized = []
        for item in movers:
            symbol = str(item.get("symbol") or "").strip()
            if not symbol:
                continue
            try:
                change_pct = self._to_float(item.get("change_pct"), 0.0)
            except (TypeError, ValueError):
                change_pct = 0.0
            normalized.append((symbol, change_pct))

        if not normalized:
            return ""

        ranked = sorted(normalized, key=lambda x: abs(x[1]), reverse=True)[:8]
        labels = [x[0] for x in ranked]
        values = [x[1] for x in ranked]
        colors = [self.style.palette["positive"] if v >= 0 else self.style.palette["negative"] for v in values]

        fig, ax = plt.subplots(figsize=(11, 5.4))
        bars = ax.bar(labels, values, color=colors, alpha=0.9)
        ax.axhline(0, color=self.style.palette["grid"], linewidth=1)
        self._polish_axes(ax, grid_axis="y")
        ax.set_title("Best & Worst Movers (Absolute % Move)")
        ax.set_ylabel("Change %")
        if len(labels) > 6:
            plt.setp(ax.get_xticklabels(), rotation=20, ha="right")

        for idx, bar in enumerate(bars):
            value = values[idx]
            ax.text(bar.get_x() + bar.get_width() / 2, value + (0.1 if value >= 0 else -0.16), f"{value:+.2f}%", ha="center", fontsize=8)

        fig.tight_layout()
        output_path = self._build_chart_path("best_worst_movers")
        fig.savefig(output_path, dpi=self.style.dpi, bbox_inches="tight")
        plt.close(fig)
        return str(output_path)

    def generate_market_breadth_snapshot(self, index_perf: Dict[str, float], market_status: Dict) -> str:
        """Create a combined chart for breadth indicators and index moves."""
        if not index_perf:
            return ""

        breadth = market_status.get("breadth", {}) if isinstance(market_status, dict) else {}
        ad_ratio = self._to_float(breadth.get("advance_decline_ratio"), 0.0)
        pct_above_200 = self._to_float(breadth.get("percent_above_200sma"), 0.0)

        fig, axes = plt.subplots(1, 2, figsize=(13, 5.2))
        fig.patch.set_facecolor(self.style.palette["background"])

        index_names = list(index_perf.keys())
        index_values = list(index_perf.values())
        bar_colors = [self.style.palette["positive"] if v >= 0 else self.style.palette["negative"] for v in index_values]
        axes[0].bar(index_names, index_values, color=bar_colors, alpha=0.9)
        axes[0].axhline(0, color=self.style.palette["grid"], linewidth=1)
        axes[0].set_title("Index Performance (%)")
        axes[0].set_ylabel("Daily Return %")
        self._polish_axes(axes[0], grid_axis="y")
        for idx, value in enumerate(index_values):
            axes[0].text(idx, value + (0.08 if value >= 0 else -0.18), f"{value:+.2f}%", ha="center", fontsize=9)

        metric_names = ["A/D Ratio", "% Above 200SMA"]
        metric_values = [ad_ratio, pct_above_200]
        metric_colors = [self.style.palette["accent"], self.style.palette["highlight"]]
        axes[1].bar(metric_names, metric_values, color=metric_colors, alpha=0.85)
        axes[1].set_title("Breadth Readings")
        self._polish_axes(axes[1], grid_axis="y")
        for idx, value in enumerate(metric_values):
            suffix = "" if idx == 0 else "%"
            axes[1].text(idx, value + (0.5 if value >= 0 else -0.5), f"{value:.1f}{suffix}", ha="center", fontsize=9)

        fig.suptitle("Market Breadth / Index Snapshot", fontsize=self.style.title_size + 1, y=1.02)
        fig.tight_layout()
        output_path = self._build_chart_path("market_breadth_snapshot")
        fig.savefig(output_path, dpi=self.style.dpi, bbox_inches="tight")
        plt.close(fig)
        return str(output_path)

    def generate_sector_leadership_chart(self, sector_data: List[Dict]) -> str:
        """Create ranked bar chart of sector performance."""
        if not sector_data:
            return ""

        df = pd.DataFrame(sector_data)
        if "changesPercentage" in df.columns:
            df["change"] = (
                pd.to_numeric(df["changesPercentage"].astype(str).str.replace("%", "", regex=False), errors="coerce")
            )
        elif "change" in df.columns:
            df["change"] = pd.to_numeric(df["change"], errors="coerce")
        else:
            return ""

        df = df.dropna(subset=["change", "sector"]).sort_values("change", ascending=True)
        if df.empty:
            return ""

        fig, ax = plt.subplots(figsize=(11.5, 6.8))
        colors = [self.style.palette["positive"] if v >= 0 else self.style.palette["negative"] for v in df["change"]]
        bars = ax.barh(df["sector"], df["change"], color=colors, alpha=0.9)
        ax.axvline(0, color=self.style.palette["grid"], linewidth=1)
        self._polish_axes(ax, grid_axis="x")
        ax.set_title("Sector Leadership (Ranked Daily Move)")
        ax.set_xlabel("Return %")

        for bar in bars:
            width = bar.get_width()
            x_pos = width + (0.08 if width >= 0 else -0.35)
            ax.text(x_pos, bar.get_y() + bar.get_height() / 2, f"{width:+.2f}%", va="center", fontsize=9)

        fig.tight_layout()
        output_path = self._build_chart_path("sector_leadership")
        fig.savefig(output_path, dpi=self.style.dpi, bbox_inches="tight")
        plt.close(fig)
        return str(output_path)

    def generate_seasonality_context_chart(self, ticker: str = "SPY") -> str:
        """Create a context chart for current month average vs overall monthly averages."""
        try:
            hist = yf.Ticker(ticker).history(period="10y", interval="1mo")
        except Exception:
            return ""

        if hist.empty:
            return ""

        monthly = hist["Close"].pct_change().dropna().to_frame("return")
        monthly["month"] = monthly.index.month
        month_means = monthly.groupby("month")["return"].mean() * 100
        if month_means.empty:
            return ""

        current_month = datetime.now().month
        labels = [datetime(2000, m, 1).strftime("%b") for m in month_means.index]
        values = month_means.values
        colors = [self.style.palette["neutral"] for _ in values]
        current_idx = list(month_means.index).index(current_month) if current_month in month_means.index else None
        if current_idx is not None:
            colors[current_idx] = self.style.palette["highlight"]

        fig, ax = plt.subplots(figsize=(12, 5.4))
        bars = ax.bar(labels, values, color=colors, alpha=0.9)
        ax.axhline(0, color=self.style.palette["grid"], linewidth=1)
        self._polish_axes(ax, grid_axis="y")
        ax.set_title(f"{ticker} Monthly Seasonality (10Y Avg Return)")
        ax.set_ylabel("Avg Monthly Return %")

        for idx, bar in enumerate(bars):
            val = values[idx]
            ax.text(bar.get_x() + bar.get_width() / 2, val + (0.1 if val >= 0 else -0.18), f"{val:+.1f}%", ha="center", fontsize=8)

        if current_idx is not None:
            current_val = values[current_idx]
            ax.annotate(
                f"Current month: {labels[current_idx]} ({current_val:+.1f}%)",
                xy=(current_idx, current_val),
                xytext=(max(current_idx - 2, 0), current_val + 0.8),
                arrowprops={"arrowstyle": "->", "color": self.style.palette["highlight"]},
                **self.style.annotation_style,
            )

        fig.tight_layout()
        output_path = self._build_chart_path("seasonality_context")
        fig.savefig(output_path, dpi=self.style.dpi, bbox_inches="tight")
        plt.close(fig)
        return str(output_path)

    def generate_price_history(self, ticker: str, price_data: pd.DataFrame, signals: List[Dict] = None) -> str:
        """Generate a price history chart with signal annotations."""
        if price_data.empty:
            return ""

        fig, ax = plt.subplots(figsize=(12, 5.8))
        ax.plot(price_data.index, price_data["Close"], color=self.style.palette["accent"], linewidth=1.6, label="Price")

        if "SMA_50" in price_data.columns:
            ax.plot(price_data.index, price_data["SMA_50"], color=self.style.palette["neutral"], alpha=0.7, label="50 SMA")
        if "SMA_200" in price_data.columns:
            ax.plot(price_data.index, price_data["SMA_200"], color=self.style.palette["highlight"], alpha=0.7, label="200 SMA")

        ax.set_title(f"{ticker} Technical Analysis")
        self._polish_axes(ax, grid_axis="both")
        ax.legend(frameon=False)

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        fig.autofmt_xdate(rotation=35)

        fig.tight_layout()
        output_path = self._build_chart_path(f"{ticker.lower()}_history")
        fig.savefig(output_path, dpi=self.style.dpi, bbox_inches="tight")
        plt.close(fig)
        return str(output_path)
