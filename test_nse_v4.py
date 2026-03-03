from nse import NSE

nse = NSE(download_folder='./data/nse_downloads', server=False)
try:
    stocks = nse.listEquityStocksByIndex('NIFTY 50')
    if isinstance(stocks, dict) and 'data' in stocks:
        data = stocks['data']
        if len(data) > 0:
             print(f"Keys of first item: {data[0].keys()}")
             # Find which key is the symbol. Often it is 'symbol' or 'symbolName' or 'shrtName'.
             # Actually, if it's Nifty 50, it might be the list of stocks.
             print("First 3 items:")
             import pprint
             pprint.pprint(data[:3])
except Exception as e:
    import traceback
    traceback.print_exc()
