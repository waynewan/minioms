# --
# --
# --
import pandas as pd
from jackutil.microfunc import retry

__mktprc_loader__ = None
def set_mktprc_loader(loader):
	global __mktprc_loader__
	__mktprc_loader__ = loader

def mktprc_loader():
	if(__mktprc_loader__ is None):
		raise ValueError("ERR: __mktprc_loader__ not set")
	return __mktprc_loader__

def load_market_price_impl(req_symbols,cached_data={}):
	missing_symbols = req_symbols - cached_data.keys()
	if(len(missing_symbols)>0):
		# -- debug -- print(f"missing_symbols:{missing_symbols}")
		price_data = retry(
			lambda : mktprc_loader().get_simple_quote(missing_symbols),
			retry=10, pause=5, rtnEx=False, silent = False,
		)
		cached_data.update({ ii['symbol'] : ii for ii in price_data })
	result = [ cached_data[sym] for sym in set(req_symbols) ]
	return result

# !!
# !! somepos will get modified
# !!
def load_market_price(somepos,cache={},clear_cache=False):
	if(clear_cache):
		cache.clear()
		return None
	symbols = somepos['symbol'].to_list()
	if(len(symbols)==0):
		symbols = ["QQQ"]
	price_data = load_market_price_impl(symbols,cache)
	price_data = pd.DataFrame(price_data).set_index('symbol',drop=True)
	somepos = somepos.join(other=price_data['price'], on="symbol", how="left")
	return somepos

