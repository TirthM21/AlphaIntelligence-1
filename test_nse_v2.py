from nse import NSE
import json

nse = NSE(download_folder='./data/nse_downloads', server=False)
try:
    stocks = nse.listEquityStocksByIndex('NIFTY 50')
    print(f"Type of stocks: {type(stocks)}")
    if isinstance(stocks, dict):
        print(f"Keys: {list(stocks.keys())}")
        # Let's see if there's a 'data' key or something.
        if 'data' in stocks:
             print(f"Data type: {type(stocks['data'])}")
             if isinstance(stocks['data'], list) and len(stocks['data']) > 0:
                 print(f"First data element: {stocks['data'][0]}")
    elif isinstance(stocks, list):
        print(f"First element: {stocks[0]}")
except Exception as e:
    import traceback
    traceback.print_exc()
