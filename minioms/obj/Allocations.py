from ..oms_db.classes_io import Allocations_IO
import pandas as pd

# --
# -- obj_spec: ( [ "account" ], None, DEF_IDX0, "Allocations", "allocation.csv", "book,portfolio,pkey,date,symbol,action,unit,exec_price,cost,linked_buy_pkey" ), # account
# --
class io_utility:
	def load(db_dir,account):
		return Allocations_IO(load=True, **locals() )

	def create(base,df0):
		lmf.types_validate(base,msg="base",types=[ Allocations_IO ],allow_none=False)
		lmf.types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	pass
