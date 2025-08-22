from ..oms_db.classes_io import AcctDividendTxns_IO
from ..oms_db.classes_io import PortfDividendTxns_IO
from ..oms_db.classes_io import Portfolios_IO
from ..oms_db.classes_io import DividendTxnsAdj_IO
from ..obj.PairedTxns import br_utility as ptxns_br
from ..obj.PortfPositions import br_utility as ppos_br
from ..obj.PortfDividendTxns import br_utility as pdiv_br
from jackutil.microfunc import types_validate
from jackutil.microfunc import dt_to_str,str_to_dt
from datetime import datetime
from copy import copy
import pandas as pd
import numpy as np

# --
# -- broker_div_txns and local_div_txns have the same format (always)
# -- DIV txn status: LOADED -- loaded from source
# --                 SKIPPED -- does not know how to allocate, needs intervention
# --                 IGNORED -- marked as ignore by user (done manually)
# --                 LEGACY -- dividend is processed by old processor
# --                 ALLOCATED -- processed
# --
# -- obj_spec: ( [ "account" ], None, DEF_IDX,  "AcctDividendTxns", "dividend_txn.csv", "line#,Date,Symbol,Amount,status,pkey" )
# --
class io_utility:
	def load(db_dir,account):
		return AcctDividendTxns_IO(load=True, **locals() )

	def create(base,df0):
		types_validate(base,msg="base",types=[ AcctDividendTxns_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	# --
	# -- replace acct div txn amount with adj amount if pkey matches
	# --
	def apply_adj_to(divadj, divtxn):
		types_validate(divadj,msg="divadj",types=[ DividendTxnsAdj_IO  ],allow_none=False)
		types_validate(divtxn,msg="divtxn",types=[ AcctDividendTxns_IO ],allow_none=False)
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

