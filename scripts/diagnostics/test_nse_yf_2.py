import yfinance as yf
import pandas as pd

def test_nse_yfinance(ticker):
    print(f"Testing yfinance for {ticker}...")
    stock = yf.Ticker(ticker)
    
    qf = stock.quarterly_financials
    print(f"\nQuarterly Financials Size: {len(qf)}")
    if not qf.empty:
         print(f"Index: {qf.index.tolist()[:10]}")
    
    qbs = stock.quarterly_balance_sheet
    print(f"Quarterly Balance Sheet Size: {len(qbs)}")
    if not qbs.empty:
         print(f"Index: {qbs.index.tolist()[:10]}")

    hist = stock.history(period="1d")
    print(f"History Price: {hist['Close'].iloc[-1] if not hist.empty else 'N/A'}")

if __name__ == "__main__":
    test_nse_yfinance("RELIANCE.NS")
    test_nse_yfinance("TCS.NS")
