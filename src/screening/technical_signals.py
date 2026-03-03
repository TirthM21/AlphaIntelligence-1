"""Module for advanced technical signals, momentum, and pattern detection.
This module implements buy signals, sell signals, momentum factors, and chart patterns.
"""

import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from .indicators import (
    calculate_rsi, calculate_mfi, calculate_cci, 
    detect_crossover, detect_crossunder, calculate_sma,
    calculate_atr, detect_volume_spike
)

logger = logging.getLogger(__name__)

class TechnicalScanner:
    """Consolidated technical scanner for signals, momentum, and patterns."""

    def __init__(self, data: pd.DataFrame):
        self.df = data
        self.close = data['Close']
        self.high = data['High']
        self.low = data['Low']
        self.volume = data['Volume']

    # --- BUY SIGNALS ---
    def detect_bullish_crossover(self, short_p: int = 50, long_p: int = 200) -> bool:
        """Detect bullish crossover (Golden Cross or similar)."""
        short_sma = calculate_sma(self.close, short_p)
        long_sma = calculate_sma(self.close, long_p)
        return detect_crossover(short_sma, long_sma)

    def detect_breakout(self, window: int = 20) -> bool:
        """Detect price breakout above historical high."""
        if len(self.df) < window + 1: return False
        hist_high = self.high.iloc[-(window+1):-1].max()
        return self.close.iloc[-1] > hist_high and detect_volume_spike(self.volume, self.volume.iloc[-1])

    def detect_support_bounce(self, window: int = 50, tolerance: float = 0.01) -> bool:
        """Detect price bouncing off a key support level."""
        if len(self.df) < window: return False
        local_lows = self.low.iloc[-window:-1].min()
        curr_low = self.low.iloc[-1]
        curr_close = self.close.iloc[-1]
        # Touch support then close above it
        return curr_low <= local_lows * (1 + tolerance) and curr_close > local_lows

    def detect_golden_cross(self) -> bool:
        """Specific Golden Cross (50 SMA crossing 200 SMA)."""
        return self.detect_bullish_crossover(50, 200)

    # --- SELL SIGNALS ---
    def detect_bearish_divergence(self, period: int = 14) -> bool:
        """Simplified bearish divergence detection (Price higher high, RSI lower high)."""
        if len(self.df) < period * 2: return False
        rsi = calculate_rsi(self.close, period)
        
        # Check last two peaks
        # This is very simplified: compare end of slice vs end of previous slice
        p2 = self.close.iloc[-1]
        p1 = self.close.iloc[-period:-1].max()
        r2 = rsi.iloc[-1]
        r1 = rsi.iloc[-period:-1].max()
        
        return p2 > p1 and r2 < r1

    def detect_death_crossover(self) -> bool:
        """Detect Death Cross (50 SMA crossing below 200 SMA)."""
        short_sma = calculate_sma(self.close, 50)
        long_sma = calculate_sma(self.close, 200)
        return detect_crossunder(short_sma, long_sma)

    def detect_52week_low(self) -> bool:
        """Detect if price is at a 52-week low."""
        if len(self.df) < 252: return False
        fifty_two_week_low = self.low.iloc[-252:].min()
        return self.close.iloc[-1] <= fifty_two_week_low * 1.01

    def detect_trend_reversal(self) -> bool:
        """Detect bearish trend reversal (price crossing below 50 SMA after being above)."""
        sma50 = calculate_sma(self.close, 50)
        return detect_crossunder(self.close, sma50)

    # --- MOMENTUM ---
    def check_momentum_extreme(self) -> Dict[str, bool]:
        """Check for extreme momentum across RSI, MFI, and CCI."""
        rsi = calculate_rsi(self.close, 14).iloc[-1]
        mfi = calculate_mfi(self.high, self.low, self.close, self.volume, 14).iloc[-1]
        cci = calculate_cci(self.high, self.low, self.close, 20).iloc[-1]
        
        return {
            'rsi_high': rsi > 70,
            'mfi_high': mfi > 80,
            'cci_high': cci > 100
        }

    def detect_volume_breakout(self) -> bool:
        """Significant volume increase accompanying price move."""
        return detect_volume_spike(self.volume, self.volume.iloc[-1], threshold=2.0)

    def detect_momentum_gainers(self, window: int = 20) -> bool:
        """Detect stocks with strong positive momentum (ROC)."""
        if len(self.df) < window: return False
        roc = (self.close.iloc[-1] - self.close.iloc[-window]) / self.close.iloc[-window]
        return roc > 0.1 # 10% gain in 'window' periods

    def detect_atr_expansion(self, window: int = 14) -> bool:
        """Detect ATR expansion (volatility breaking out)."""
        atr = calculate_atr(self.high, self.low, self.close, window)
        if len(atr) < window + 1: return False
        avg_atr = atr.iloc[-(window+1):-1].mean()
        return atr.iloc[-1] > avg_atr * 1.25

    # --- PATTERNS ---
    def detect_inside_bar(self) -> bool:
        """Inside Bar pattern detection."""
        if len(self.df) < 2: return False
        curr = self.df.iloc[-1]
        prev = self.df.iloc[-2]
        return curr['High'] < prev['High'] and curr['Low'] > prev['Low']

    def detect_double_bottom(self, lookback: int = 100, tolerance: float = 0.02) -> bool:
        """Detect Double Bottom pattern."""
        if len(self.df) < lookback: return False
        # Find two lowest points in history
        lows = self.low.iloc[-lookback:].nsmallest(5)
        if len(lows) < 2: return False
        
        # Check if the two lowest points are significantly separated in time but similar in price
        idx1, idx2 = lows.index[0], lows.index[1]
        if abs((idx1 - idx2).days) > 10: # At least 10 days apart
            val1, val2 = self.low.loc[idx1], self.low.loc[idx2]
            return abs(val1 - val2) / val1 < tolerance
        return False

    def detect_double_top(self, lookback: int = 100, tolerance: float = 0.02) -> bool:
        """Detect Double Top pattern."""
        if len(self.df) < lookback: return False
        highs = self.high.iloc[-lookback:].nlargest(5)
        if len(highs) < 2: return False
        idx1, idx2 = highs.index[0], highs.index[1]
        if abs((idx1 - idx2).days) > 10:
            val1, val2 = self.high.loc[idx1], self.high.loc[idx2]
            return abs(val1 - val2) / val1 < tolerance
        return False

    def scan_all(self) -> Dict[str, List[str]]:
        """Run all scans and return categorized results."""
        results = {
            "buy_signals": [],
            "sell_signals": [],
            "momentum_factors": [],
            "chart_patterns": []
        }
        
        # Buy
        if self.detect_bullish_crossover(): results["buy_signals"].append("Bullish Crossover")
        if self.detect_breakout(): results["buy_signals"].append("Breakout Detected")
        if self.detect_support_bounce(): results["buy_signals"].append("Support Bounce")
        if self.detect_golden_cross(): results["buy_signals"].append("Golden Cross")
        
        # Sell
        if self.detect_bearish_divergence(): results["sell_signals"].append("Bearish Divergence")
        if self.detect_death_crossover(): results["sell_signals"].append("Death Crossover")
        if self.detect_52week_low(): results["sell_signals"].append("52-Week Low")
        if self.detect_trend_reversal(): results["sell_signals"].append("Trend Reversal")
        
        # Momentum
        m_extremes = self.check_momentum_extreme()
        if any(m_extremes.values()): 
            flags = [k.replace('_high', '').upper() for k, v in m_extremes.items() if v]
            results["momentum_factors"].append(f"High Momentum ({', '.join(flags)})")
        if self.detect_volume_breakout(): results["momentum_factors"].append("Volume Breakout")
        if self.detect_momentum_gainers(): results["momentum_factors"].append("Momentum Gainer")
        if self.detect_atr_expansion(): results["momentum_factors"].append("ATR Expansion")
        
        # Patterns
        if self.detect_inside_bar(): results["chart_patterns"].append("Inside Bar")
        if self.detect_double_bottom(): results["chart_patterns"].append("Double Bottom")
        if self.detect_double_top(): results["chart_patterns"].append("Double Top")
        # Cup & Handle and Head & Shoulders are complex; adding placeholders for report structure
        
        return results
