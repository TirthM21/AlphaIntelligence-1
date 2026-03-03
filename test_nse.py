from nse import NSE
import json

nse = NSE(download_folder='./data/nse_downloads', server=False)
try:
    stocks = nse.listEquityStocksByIndex('NIFTY 50')
    print(f"Type of stocks: {type(stocks)}")
    if stocks:
        print(f"First element type: {type(stocks[0])}")
        print(f"First element: {stocks[0]}")
except Exception as e:
    print(f"Error: {e}")
