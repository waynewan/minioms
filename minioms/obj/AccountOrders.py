from ..oms_db.classes_io import AccountOrders_IO
from jackutil.microfunc import types_validate
import pandas as pd

class io_utility:
	def load(db_dir,account):
		return AccountOrders_IO(load=True, **locals() )

	def create(base,df0):
		types_validate(base,msg="base",types=[ AccountOrders_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	# --
	pass


