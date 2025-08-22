from ..oms_db.classes_io import Matchings_IO
from jackutil.microfunc import types_validate
from copy import copy
import pandas as pd

# --
# -- obj_spec: ( [ "account" ], None, DEF_IDX0, "Matchings", "matching.csv", "date,symbol,ord_qty,exec_qty,exec_price,ttl_cost,match,exec_pkey" ), # account
# --
class io_utility:
	def load(db_dir,account):
		return Matchings_IO(load=True, **locals() )

	def create(base,df0):
		types_validate(base,msg="base",types=[ Matching ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	def hilite_bad_match(matching):
		matching = matching.copy()
		matching.loc[:,'bad_match'] = ''
		matching.loc[matching['match'] !='matched','bad_match'] = '<-----------'
		return matching

