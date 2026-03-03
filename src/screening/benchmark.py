"""Benchmark and market breadth analysis module.

This module analyzes Nifty 50 (market benchmark) and calculates market breadth metrics.
"""

import logging
from typing import Dict, List

import pandas as pd

from .phase_indicators import classify_phase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def analyze_benchmark_trend(nifty_price_data: pd.DataFrame, current_nifty_price: float) -> Dict[str, any]:
    """Analyze Nifty 50 trend using Phase classification."""
    if nifty_price_data.empty:
        logger.warning("Empty benchmark price data")
        return {
            'phase': 0,
            'phase_name': 'Unknown',
            'trend': 'unknown',
            'error': 'No data'
        }

    # Classify Nifty phase
    phase_info = classify_phase(nifty_price_data, current_nifty_price)

    # Determine overall trend
    phase = phase_info['phase']
    if phase == 1:
        trend = 'Consolidating'
    elif phase == 2:
        trend = 'Bullish'
    elif phase == 3:
        trend = 'Topping'
    elif phase == 4:
        trend = 'Bearish'
    else:
        trend = 'Unknown'

    return {
        'ticker': '^NSEI',
        'phase': phase,
        'phase_name': phase_info['phase_name'],
        'trend': trend,
        'confidence': phase_info['confidence'],
        'sma_50': phase_info.get('sma_50'),
        'sma_200': phase_info.get('sma_200'),
        'slope_50': phase_info.get('slope_50'),
        'slope_200': phase_info.get('slope_200'),
        'current_price': current_nifty_price,
        'reasons': phase_info.get('reasons', [])
    }


def calculate_market_breadth(phase_results: List[Dict]) -> Dict[str, any]:
    """Calculate market breadth metrics.

    Args:
        phase_results: List of phase classification results for all stocks

    Returns:
        Dict with breadth metrics
    """
    if not phase_results:
        return {
            'total_stocks': 0,
            'phase_1_count': 0,
            'phase_2_count': 0,
            'phase_3_count': 0,
            'phase_4_count': 0,
            'phase_1_pct': 0,
            'phase_2_pct': 0,
            'phase_3_pct': 0,
            'phase_4_pct': 0,
            'breadth_quality': 'unknown'
        }

    total = len(phase_results)

    # Count stocks in each phase
    phase_counts = {1: 0, 2: 0, 3: 0, 4: 0, 0: 0}

    for result in phase_results:
        phase = result.get('phase', 0)
        phase_counts[phase] = phase_counts.get(phase, 0) + 1

    # Calculate percentages
    phase_1_pct = (phase_counts[1] / total * 100) if total > 0 else 0
    phase_2_pct = (phase_counts[2] / total * 100) if total > 0 else 0
    phase_3_pct = (phase_counts[3] / total * 100) if total > 0 else 0
    phase_4_pct = (phase_counts[4] / total * 100) if total > 0 else 0

    # Determine breadth quality
    if phase_2_pct > 50:
        breadth_quality = 'Excellent'
    elif phase_2_pct > 30:
        breadth_quality = 'Good'
    elif phase_2_pct > 15:
        breadth_quality = 'Fair'
    else:
        breadth_quality = 'Weak'

    return {
        'total_stocks': total,
        'phase_1_count': phase_counts[1],
        'phase_2_count': phase_counts[2],
        'phase_3_count': phase_counts[3],
        'phase_4_count': phase_counts[4],
        'phase_1_pct': round(phase_1_pct, 1),
        'phase_2_pct': round(phase_2_pct, 1),
        'phase_3_pct': round(phase_3_pct, 1),
        'phase_4_pct': round(phase_4_pct, 1),
        'breadth_quality': breadth_quality
    }


def classify_market_regime(benchmark_analysis: Dict, breadth: Dict) -> str:
    """Classify overall market regime."""
    bench_phase = benchmark_analysis.get('phase', 0)
    phase_2_pct = breadth.get('phase_2_pct', 0)

    # Strong Risk-On conditions
    if bench_phase == 2 and phase_2_pct > 40:
        return 'RISK-ON (Strong)'

    # Moderate Risk-On
    elif bench_phase == 2 and phase_2_pct > 25:
        return 'RISK-ON (Moderate)'

    # Weak Risk-On / Mixed
    elif bench_phase == 2 or (bench_phase == 1 and phase_2_pct > 30):
        return 'RISK-ON (Weak) / Mixed'

    # Risk-Off conditions
    elif bench_phase == 4 or phase_2_pct < 15:
        return 'RISK-OFF'

    # Transitional / Uncertain
    else:
        return 'TRANSITIONAL / Uncertain'


def format_benchmark_summary(bench_analysis: Dict, breadth: Dict) -> str:
    """Format benchmark summary (Nifty 50)."""
    regime = classify_market_regime(bench_analysis, breadth)

    summary = f"\n{'='*60}\n"
    summary += "BENCHMARK SUMMARY (NIFTY 50)\n"
    summary += f"{'='*60}\n\n"

    # Nifty 50 Analysis with emoji
    phase = bench_analysis['phase']
    phase_emoji = "🟢" if phase == 2 else "🟡" if phase in [1, 3] else "🔴"

    summary += f"{phase_emoji} Nifty 50 trend classification:\n"
    summary += f"  Phase: {bench_analysis['phase']} - {bench_analysis['phase_name']}\n"
    summary += f"  Trend: {bench_analysis['trend']}\n"
    summary += f"  Current Price: {bench_analysis.get('current_price', 0):.2f}\n"

    slope_50 = bench_analysis.get('slope_50', 0)
    slope_50_emoji = "🟢" if slope_50 > 0 else "🔴"
    summary += f"  {slope_50_emoji} 50 SMA: {bench_analysis.get('sma_50', 0):.2f} (slope: {slope_50:.4f})\n"

    slope_200 = bench_analysis.get('slope_200', 0)
    slope_200_emoji = "🟢" if slope_200 > 0 else "🔴"
    summary += f"  {slope_200_emoji} 200 SMA: {bench_analysis.get('sma_200', 0):.2f} (slope: {slope_200:.4f})\n"

    confidence = bench_analysis.get('confidence', 0)
    if confidence >= 80:
        conf_emoji = "🟢"
    elif confidence >= 60:
        conf_emoji = "🟡"
    else:
        conf_emoji = "🔴"
    summary += f"  {conf_emoji} Confidence: {confidence:.0f}%\n"

    # Market Breadth with emoji
    summary += f"\nMarket Breadth (n={breadth['total_stocks']}):\n"
    summary += f"  🟡 Phase 1 (Base Building): {breadth['phase_1_count']} stocks ({breadth['phase_1_pct']:.1f}%)\n"
    summary += f"  🟢 Phase 2 (Uptrend): {breadth['phase_2_count']} stocks ({breadth['phase_2_pct']:.1f}%)\n"
    summary += f"  🟡 Phase 3 (Distribution): {breadth['phase_3_count']} stocks ({breadth['phase_3_pct']:.1f}%)\n"
    summary += f"  🔴 Phase 4 (Downtrend): {breadth['phase_4_count']} stocks ({breadth['phase_4_pct']:.1f}%)\n"

    # Breadth quality emoji
    breadth_quality = breadth['breadth_quality']
    if breadth_quality == 'excellent':
        breadth_emoji = "⭐"  # Star for excellent
    elif breadth_quality == 'good':
        breadth_emoji = "🟢"
    elif breadth_quality == 'moderate':
        breadth_emoji = "🟡"
    else:
        breadth_emoji = "🔴"
    summary += f"  {breadth_emoji} Breadth Quality: {breadth_quality}\n"

    # Market Regime with emoji
    if 'RISK-ON' in regime:
        regime_emoji = "🟢"
    elif 'RISK-OFF' in regime:
        regime_emoji = "🔴"
    else:
        regime_emoji = "🟡"
    summary += f"\n{regime_emoji} Market Regime: {regime}\n"

    # Interpretation
    summary += "\nInterpretation:\n"
    if 'RISK-ON' in regime:
        summary += "  🟢 Favorable environment for breakout trades\n"
        summary += "  → Focus on Phase 2 breakouts with strong RS\n"
    elif 'RISK-OFF' in regime:
        summary += "  🔴 Defensive environment - raise cash, tighten stops\n"
        summary += "  → Avoid new breakouts, focus on preservation\n"
    else:
        summary += "  🟡 Mixed/transitional market - be selective\n"
        summary += "  → Focus on highest quality setups only\n"

    summary += f"{'='*60}\n"

    return summary


def should_generate_signals(benchmark_analysis: Dict, breadth: Dict,
                             min_phase2_pct: float = 15.0) -> Dict[str, any]:
    """Determine if market conditions warrant generating buy signals.

    Args:
        benchmark_analysis: Nifty 50 analysis
        breadth: Market breadth
        min_phase2_pct: Minimum Phase 2 percentage for signal generation

    Returns:
        Dict with recommendation
    """
    bench_phase = benchmark_analysis.get('phase', 0)
    phase_2_pct = breadth.get('phase_2_pct', 0)
    regime = classify_market_regime(benchmark_analysis, breadth)

    # Determine if we should generate buy signals
    # NOTE: Buy signal scoring should always run. Market regime/breadth is used as context,
    # not as a hard gate, otherwise strong setups can be hidden in weak tapes.
    should_buy = True
    reasons = []

    if bench_phase in [2, 1]:
        if phase_2_pct >= min_phase2_pct:
            reasons.append(f"Market breadth adequate ({phase_2_pct:.1f}% in Phase 2)")
        else:
            reasons.append(
                f"Market breadth weak ({phase_2_pct:.1f}% in Phase 2, need {min_phase2_pct}%) - buys still evaluated"
            )
    else:
        reasons.append(f"Nifty 50 in unfavorable phase ({bench_phase}) - buys still evaluated")

    # Sell signals - always generate if applicable
    should_sell = True

    return {
        'should_generate_buys': should_buy,
        'should_generate_sells': should_sell,
        'regime': regime,
        'reasons': reasons,
        'phase_2_pct': phase_2_pct,
        'bench_phase': bench_phase
    }
