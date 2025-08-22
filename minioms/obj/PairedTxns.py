from ..oms_db.classes_io import PairedTxns_IO
from jackutil.microfunc import types_validate
from jackutil.microfunc import dt_to_str,str_to_dt
from copy import copy
from datetime import datetime
import pandas as pd
import numpy as np

class io_utility:
	def load(db_dir,strategy,portfolio):
		return PairedTxns_IO(load=True, **locals() )

	def load_bulk(db_dir,strat_portf_pairs):
		result = {}
		for strat,portf in strat_portf_pairs:
			result[(strat,portf)] = io_utility.load(db_dir=db_dir,strategy=strat,portfolio=portf)
		return result

	def create(base,df0):
		types_validate(base,msg="base",types=[ PairedTxns_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	def filter_by_symbol(ptxns,symbol):
		types_validate(ptxns,msg="ptxns",types=[ PairedTxns_IO ],allow_none=False)
		types_validate(symbol,msg="symbol",types=[ str ])
		df0 = ptxns.df
		df1 = df0[df0['symbol']==symbol]
		return io_utility.create(ptxns,df1)

	# !!
	# !! this filter remove transactions without considering its BUY/SEL relationship
	# !! using after_dt, and at_after_dt might remove txn that paired with a later txn
	# !! using before_dt, and at_before_dt might remove txn that paired with an earlier txn
	# !!
	# -- consider using "rollback_to_date" instead
	# !!
	def filter_by_date(ptxns,before_dt=None,at_before_dt=None,after_dt=None,at_after_dt=None):
		types_validate(ptxns,msg="ptxns",types=[ PairedTxns_IO ],allow_none=False)
		types_validate(before_dt,msg="before_dt",types=[ str, datetime ])
		types_validate(at_before_dt,msg="at_before_dt",types=[ str, datetime ])
		types_validate(after_dt,msg="after_dt",types=[ str, datetime ])
		types_validate(at_after_dt,msg="at_after_dt",types=[ str, datetime ])
		df0 = ptxns.df
		df1 = df0.copy()
		if(before_dt is not None):
			df1 = df1[df1['date']<before_dt]
		if(at_before_dt is not None):
			df1 = df1[df1['date']<=at_before_dt]
		if(after_dt is not None):
			df1 = df1[df1['date']>after_dt]
		if(at_after_dt is not None):
			df1 = df1[df1['date']>=at_after_dt]
		return io_utility.create(ptxns,df1)

	# --
	# -- rewind time for PairedTxns properly (compare to filter_by_date)
	# --
	def rollback_to_date(ptxns,todate):
		types_validate(ptxns,msg="ptxns",types=[ PairedTxns_IO ],allow_none=False)
		types_validate(todate,msg="todate",types=[ str, datetime ])
		df0 = ptxns.df
		df1 = df0[pd.to_datetime(df0['date'])<=todate].copy()
		df2 = df0[(pd.to_datetime(df0['date'])>todate) * (df0['type']=='SEL')]
		for nn,rr in df2.iterrows():
			linked_sell_txn = rr['pkey']
			df1.loc[df1['linked_sell_txn']==linked_sell_txn,'linked_sell_txn'] = "--"
		return io_utility.create(ptxns,df1)

	def extract_openpos(ptxns):
		types_validate(ptxns,msg="ptxns",types=[ PairedTxns_IO ],allow_none=False)
		df0 = ptxns.df
		open_pos = df0[np.logical_and(df0["type"]=="BUY",df0["linked_sell_txn"]=="--")]
		open_pos = open_pos[open_pos['symbol'] !='--']
		open_pos = open_pos.drop(columns=["linked_sell_txn"])
		return io_utility.create(ptxns,open_pos)

	def summary(ptxns):
		types_validate(ptxns,msg="ptxns",types=[ PairedTxns_IO ],allow_none=False)
		df0 = ptxns.df
		df1 = df0.loc[:,['symbol','unit','cost']].copy()
		df1['first_trade_date'] = df0['date']
		df1['last_trade_date'] = df0['date']
		df2 = df1.groupby("symbol").aggregate({
			"unit" : "sum",
			"cost" : "sum",
			"first_trade_date" : "min",
			"last_trade_date" : "max",
		}).reset_index(drop=False)
		df2 = df2[df2['symbol'] !='--']
		return df2

