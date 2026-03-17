def write_header(openedfile):
	openedfile.write(
f"""from typing import Optional
import pandas as pd
import os
from . import datafile

""")

def write_class_code(params, subdir, idx_col, openedfile, classname, filename, columns):
	if(subdir is not None):
		write_class_code_with_subdir(subdir, idx_col, openedfile, classname, filename, columns)
		return
	if(params is not None):
		write_class_code_with_params(params, idx_col, openedfile, classname, filename, columns)
		return
	
def write_class_code_with_subdir(subdir, idx_col, openedfile, classname, filename, columns):
	plist0 = str(idx_col)
	openedfile.write(
f"""# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class {classname}_IO(datafile.DataFile):
	COLUMNS = {columns.split(",")}

	def __init__(self, db_dir, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.db_dir = db_dir
		self.subdir = "{subdir}"
		directory = os.path.join(db_dir, "{subdir}")
		if(load):
			super().__init__(directory, filename="{filename}", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col={plist0})
			else:
				super().read(drop=True,idx_col={plist0})
		else:
			super().__init__(directory, filename="{filename}", columns={classname}_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)=={classname}_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {{{classname}_IO}} object")
		if(raise_on_err):
			raise error
		return error
		
""")

def write_class_code_with_params(params, idx_col, openedfile, classname, filename, columns):
	plist0 = str(idx_col)
	plist1 = ",".join(params)
	assign = [ f"self.{pp} = {pp}" for pp in params ]
	assign = """		""".join(assign)
		
	openedfile.write(
f"""# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class {classname}_IO(datafile.DataFile):
	COLUMNS = {columns.split(",")}

	def __init__(self, db_dir, {plist1}, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		{assign}
		self.db_dir = db_dir
		directory = os.path.join(db_dir, {plist1})
		if(load):
			super().__init__(directory, filename="{filename}", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col={plist0})
			else:
				super().read(drop=True,idx_col={plist0})
		else:
			super().__init__(directory, filename="{filename}", columns={classname}_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)=={classname}_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {{{classname}_IO}} object")
		if(raise_on_err):
			raise error
		return error
		
""")

if __name__ == '__main__':
	# --
	# --
	# --
	output_filename = "classes_io.py"
	DEF_IDX = [ "line#" ]
	DEF_IDX0 = [ 0 ]
	# --
	# --
	# --
	class_list = [
		( None, "_tbsys_", DEF_IDX,  "Accounts", "accounts.csv", "line#,key,broker,account,sub-account,URL" ), # _tbsys_
		( None, "_tbsys_", DEF_IDX,  "Books", "books.csv", "line#,chip,book,URL" ), # _tbsys_
		( None, "_tbsys_", DEF_IDX,  "Portfolios", "portfs.csv", "line#,book,portfolio,trade_acct,book_URL,tradebot_URL" ), # _tbsys_
		( None, "_tbsys_", None,     "SymbolsMap", "symbols_map.csv", "tradebot,norgate,google,fmp,fidelity,stockcharts" ), # _tbsys_
		# --
		( [ "account" ], None, None,     "AccountOrders", "account_orders.csv", "symbol,unit" ), # account
		( [ "account" ], None, DEF_IDX0, "Allocations", "allocation.csv", "book,portfolio,pkey,date,symbol,action,unit,exec_price,cost,linked_buy_pkey" ), # account
		( [ "account" ], None, DEF_IDX0, "AcctDailyOrders", "daily_orders.csv", "book,portfolio,date,symbol,action,unit,price,linked_buy_pkey,pkey" ), # account
		( [ "account" ], None, DEF_IDX,  "AcctDividendTxns", "dividend_txn.csv", "line#,Date,Symbol,Amount,status,pkey" ), # account
		( [ "account" ], None, DEF_IDX,  "DividendTxnsAdj", "dividend_txn_adj.csv", "line#,pkey,adj_Amount,note" ), # account
		( [ "account" ], None, DEF_IDX,  "DividendTxnsStaging", "dividend_txn_staging.csv", "line#,Date,S_IOymbol,Amount,status,pkey" ), # account
		( [ "account" ], None, DEF_IDX,  "Executions", "executions.csv", "line#,Symbol,Shares,Price,Amount" ), # account
		( [ "account" ], None, DEF_IDX0, "Matchings", "matching.csv", "date,symbol,ord_qty,exec_qty,exec_price,ttl_cost,match,exec_pkey" ), # account
		( [ "account" ], None, DEF_IDX,  "OtherHoldings", "other_holdings.csv", "line#,symbol,quantity,note" ), # account
		( [ "account" ], None, DEF_IDX,  "AcctPositions", "positions.csv", "line#,Symbol,Desc,Last Price,Price Change,$ Gain/Loss,% Gain/Loss,$ Total Gain/Loss,% Total Gain/Loss,Current Value,% of Account,Quantity,Per Share,Total Cost,52w lo,52w hi" ), # account
		( [ "account" ], None, DEF_IDX, "AcctPositionReport", "position_report.csv", "line#,index,account_holding,portfs_holding,other_holding,holding_diff" ), # account
		# --
		( [ "strategy", "portfolio" ], None, DEF_IDX0, "PortfSetting", "portf_setting.csv", "value,dtype" ), # portfolio
		( [ "strategy", "portfolio" ], None, DEF_IDX0, "PortfDailyOrders", "daily_orders.csv", "book,portfolio,date,symbol,action,unit,price,linked_buy_pkey,pkey" ), # portfolio
		( [ "strategy", "portfolio" ], None, DEF_IDX0, "ExitConds", "exit_cond.csv", "entry_exec_date,cost,action,symbol,unit,entry_price,pkey,uid,stops,exit_trigger,last_close,stops/symbol_dropped,stops/duration_stop,duration_stop" ), # portfolio
		( [ "strategy", "portfolio" ], None, DEF_IDX,  "PortfDividendTxns", "dividend_txn.csv", "line#,account,pay_date,enter_date,type,symbol,amount,dtxn_pkey,unit,note1" ), # portfolio
		( [ "strategy", "portfolio" ], None, DEF_IDX,  "PortfPositions", "open_pos.csv", "line#,date,cost,type,symbol,unit,entry price,pkey" ), # portfolio
		( [ "strategy", "portfolio" ], None, DEF_IDX0, "PairedTxns", "paired_txn.csv", "date,cost,type,symbol,unit,entry price,pkey,linked_sell_txn" ), # portfolio
		( [ "strategy", "portfolio" ], None, None,     "Buylist", "buylist.csv", "symbol" ), # portfolio
		# --
		# -- legacy (used by portfolio_daily_update_v5_01.py only)
		# --
		( [ "strategy", "portfolio" ], None, DEF_IDX0, "ExitConds_v5", "exit_cond_v5_01.csv", "entry_exec_date,cost,action,symbol,unit,entry_price,pkey,uid,stops,exit_trigger,last_close,stops/symbol_dropped,stops/duration_stop,duration_stop" ), # portfolio
		( [ "strategy", "portfolio" ], None, None,     "Buylist_v5", "buylist_v5_01.csv", "symbol" ), # portfolio
	]
	# --
	# --
	# --
	print(f"generating the file: {output_filename}")
	# --
	# --
	# --
	with open(output_filename, 'w') as outfile:
		write_header(outfile)
		for (params, subdir, idx_col, classname, filename, columns) in class_list:
			write_class_code(params, subdir, idx_col, outfile, classname, filename, columns)
			print(f"It contains a class named: {classname} with attributes: {columns}")
	# --
	# --
	# --
	print(f"generated the file: {output_filename}")
	# --
	# --
	# --

