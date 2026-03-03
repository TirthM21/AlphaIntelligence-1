---
description: Identify exit points and trend reversals for risk management.
---

1. Monitor holdings for signs of topping or distribution.
2. Check for the following exit signals:
   - **Bearish Divergence**: Price making higher highs while RSI/MFI makes lower highs (momentum exhaustion).
   - **Death Crossover**: 50 SMA crossing below 200 SMA (long-term bearish shift).
   - **52-Week Lows**: Price breaking below annual floors.
   - **Trend Reversal**: Price crossing below 50 SMA or trendline breakdown.
3. Check for **Phase 3 (Top)** or **Phase 4 (Downtrend)** transitions.
// turbo
4. Run the rebalance check to see list of sell candidates:
```powershell
python run_optimized_scan.py --limit 20
```
