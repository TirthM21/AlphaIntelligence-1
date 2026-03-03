---
description: Identify stocks with strong kinetic energy and volatility expansion.
---

1. Filter for high relative strength stocks.
2. Monitor momentum indicators:
   - **High RSI/MFI/CCI**: Overbought levels (>70/80/100) often indicate strong impulsive moves.
   - **Volume Breakouts**: Volume > 2x average confirming price conviction.
   - **Momentum Gainers**: High ROC (Rate of Change) over 20 periods.
   - **ATR Expansion**: Volatility breakout indicating a new directional move is starting.
3. Prioritize stocks in a "Volatility Contraction" (VCP) state before the momentum burst.
// turbo
4. Run momentum-focused scan:
```powershell
python run_optimized_scan.py --aggressive --limit 30
```
