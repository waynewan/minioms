from ..oms_db.classes_io import PortfDividendTxns_IO
from jackutil.microfunc import types_validate
from copy import copy
import pandas as pd

# --
# -- obj_spec: ( [ "strategy", "portfolio" ], None, DEF_IDX,  "PortfDividendTxns", "dividend_txn.csv", "line#,account,pay_date,enter_date,type,symbol,amount,dtxn_pkey,unit,note1" )
# --
class io_utility:
	def load(db_dir,strategy,portfolio):
		return PortfDividendTxns_IO(load=True, **locals() )

	def load_bulk(db_dir,strat_portf_pairs):
		result = {}
		for strat,portf in strat_portf_pairs:
			result[(strat,portf)] = io_utility.load(db_dir=db_dir,strategy=strat,portfolio=portf)
		return result

	def create(base,df0):
		types_validate(base,msg="base",types=[ PortfDividendTxns_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		if(df0 is None):
			return None
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

	def upgrade_v0(df0):
		df0 = df0[ df0['type']=='DIV' ]
		# --
		if('note1' not in df0.columns):
			df0['note1'] = None
		# --
		if('dtxn_pkey' not in df0.columns):
			df0['dtxn_pkey'] = None
		# --
		df0['legacy_key'] = df0[["type","pay_date","symbol"]].astype(str).agg("|".join,axis=1)
		# --
		if('unit' not in df0.columns):
			df0['unit'] = None
		# --
		# df0 = df0[PortfDividendTxns_IO.COLUMNS]
		return df0

class br_utility:
	pass

