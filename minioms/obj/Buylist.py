from ..oms_db.classes_io import Buylist_IO
from jackutil.microfunc import types_validate
from jackutil.microfunc import dt_to_str,str_to_dt
from datetime import datetime
import pandas as pd
import numpy as np

# --
# -- obj_spec: ( [ "strategy", "portfolio" ], None, DEF_IDX0, "Buylist", "exit_cond.csv", "entry_exec_date,cost,action,symbol,unit,entry_price,pkey,uid,stops,exit_trigger,last_close,stops/symbol_dropped,stops/duration_stop,duration_stop" ), # portfolio
# --
class io_utility:
	def load(db_dir,strategy,portfolio):
		return Buylist_IO(load=True, **locals() )

	def create(base,df0):
		types_validate(base,msg="base",types=[ Buylist_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	pass

