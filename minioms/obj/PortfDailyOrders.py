from ..oms_db.classes_io import PortfDailyOrders_IO
from jackutil.microfunc import types_validate
import pandas as pd

# --
# -- obj_spec: ( [ "strategy", "portfolio" ], None, DEF_IDX0, "PortfDailyOrders", "daily_orders.csv", "book,portfolio,date,symbol,action,unit,price,linked_buy_pkey,pkey" ), # portfolio
# --
class io_utility:
	def load(*,db_dir,strategy,portfolio):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(strategy,msg="strategy",types=[ type("") ],allow_none=False)
		types_validate(portfolio,msg="portfolio",types=[ type("") ],allow_none=False)
		return PortfDailyOrders_IO(load=True, **locals() )

	def load_bulk(db_dir,strat_portf_pairs):
		result = {}
		for strat,portf in strat_portf_pairs:
			result[(strat,portf)] = io_utility.load(db_dir=db_dir,strategy=strat,portfolio=portf)
		return result

	def save(db_dir, strategy, portfolio, df0):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(strategy,msg="strategy",types=[ type("") ],allow_none=False)
		types_validate(portfolio,msg="portfolio",types=[ type("") ],allow_none=False)
		obj = PortfDailyOrders_IO(db_dir=db_dir, strategy=strategy, portfolio=portfolio, df0=df0)
		obj.write()

	def create(base,df0):
		types_validate(base,msg="base",types=[ PortfDailyOrders_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	# --
	pass

