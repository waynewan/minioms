from ..oms_db.classes_io import DividendTxnsStaging_IO
from ..oms_db.classes_io import DividendTxnsAdj_IO
from ..oms_db.classes_io import AcctDividendTxns_IO
from jackutil.microfunc import types_validate
import pandas as pd
import numpy as np

class io_utility:
	def load(db_dir,account):
		return DividendTxnsStaging_IO(load=True, **locals() )

class br_utility:
	# --
	# --
	# --
	def apply_adj_to(divadj, divtxn):
		types_validate(divadj,msg="divadj",types=[ DividendTxnsAdj_IO  ],allow_none=False)
		types_validate(divtxn,msg="divtxn",types=[ DividendTxnsStaging_IO ],allow_none=False)
		## --
		df0, df1 = divtxn.df, divadj.df
		for nn,rr in df1.iterrows():
			affected_txn_pkey = df0['pkey']==rr['pkey']
			affected_txn = df0[affected_txn_pkey]
			if(len(affected_txn)==0):
				print(ValueError(f"WARN: cannot find original div txn {rr}, maybe new value"))
			if(len(affected_txn)>1):
				raise ValueError(f"ERR: too many row affected {rr}, bad data")
			df0.loc[affected_txn_pkey,'Amount'] = rr['adj_Amount']
		return divtxn

