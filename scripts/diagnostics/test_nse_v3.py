from nse import NSE
import json

nse = NSE(download_folder='./data/nse_downloads', server=False)
try:
    stocks = nse.listEquityStocksByIndex('NIFTY 50')
    if isinstance(stocks, dict) and 'data' in stocks:
        data = stocks['data']
        if isinstance(data, list) and len(data) > 0:
            import pprint
            pprint.pprint(data[0])
except Exception as e:
    import traceback
    traceback.print_exc()
