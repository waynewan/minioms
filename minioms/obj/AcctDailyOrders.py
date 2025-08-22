from ..oms_db.classes_io import AcctDailyOrders_IO
from jackutil.microfunc import types_validate
import pandas as pd

# --
# -- obj_spec: ( [ "account" ], None, DEF_IDX0, "AcctDailyOrders", "daily_orders.csv", "book,portfolio,date,symbol,action,unit,price,linked_buy_pkey,pkey" ), # account
# --
class io_utility:
	def load(db_dir,account):
		return AcctDailyOrders_IO(load=True, **locals() )

	def create(base,df0):
		types_validate(base,msg="base",types=[ AcctDailyOrders_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	# --
	pass

