import yfinance as yf
import pandas as pd

def test_nse_yfinance():
    ticker = "RELIANCE.NS"
    print(f"Testing yfinance for {ticker}...")
    stock = yf.Ticker(ticker)
    
    print("\nQuarterly Financials Index:")
    print(stock.quarterly_financials.index.tolist())
    
    print("\nQuarterly Balance Sheet Index:")
    print(stock.quarterly_balance_sheet.index.tolist())
    
    print("\nHistory (last 5 days):")
    print(stock.history(period="5d"))

if __name__ == "__main__":
    test_nse_yfinance()
