---
description: Identify high-probability buy entries using trend, breakouts, and support.
---

1. Ensure the stock is in a healthy **Phase 2 (Uptrend)**.
2. Check for the following specific signals:
   - **Bullish Crossovers**: Fast EMA/SMA crossing above Slow EMA/SMA (e.g., 50 over 200).
   - **Breakout Detection**: Price closing above 20-day high on elevated volume (>150% average).
   - **Support Bounces**: Price touching recent local lows and bouncing significantly.
   - **Golden Cross**: Long-term directional confirmation (50 SMA > 200 SMA).
3. Validate with **Minervini Trend Template** (RS > 0, Price > 50 SMA > 150 SMA > 200 SMA).
// turbo
4. Run the scan to find candidates:
```powershell
python run_optimized_scan.py --limit 50
```
