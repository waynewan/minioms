from typing import Optional
import pandas as pd
import os
from . import datafile

# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class Accounts_IO(datafile.DataFile):
	COLUMNS = ['line#', 'key', 'broker', 'account', 'sub-account', 'URL']

	def __init__(self, db_dir, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.db_dir = db_dir
		self.subdir = "_tbsys_"
		directory = os.path.join(db_dir, "_tbsys_")
		if(load):
			super().__init__(directory, filename="accounts.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="accounts.csv", columns=Accounts_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==Accounts_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {Accounts_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class Books_IO(datafile.DataFile):
	COLUMNS = ['line#', 'chip', 'book', 'URL']

	def __init__(self, db_dir, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.db_dir = db_dir
		self.subdir = "_tbsys_"
		directory = os.path.join(db_dir, "_tbsys_")
		if(load):
			super().__init__(directory, filename="books.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="books.csv", columns=Books_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==Books_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {Books_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class Portfolios_IO(datafile.DataFile):
	COLUMNS = ['line#', 'book', 'portfolio', 'trade_acct', 'book_URL', 'tradebot_URL']

	def __init__(self, db_dir, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.db_dir = db_dir
		self.subdir = "_tbsys_"
		directory = os.path.join(db_dir, "_tbsys_")
		if(load):
			super().__init__(directory, filename="portfs.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="portfs.csv", columns=Portfolios_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==Portfolios_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {Portfolios_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class SymbolsMap_IO(datafile.DataFile):
	COLUMNS = ['tradebot', 'norgate', 'google', 'fmp', 'fidelity', 'stockcharts']

	def __init__(self, db_dir, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.db_dir = db_dir
		self.subdir = "_tbsys_"
		directory = os.path.join(db_dir, "_tbsys_")
		if(load):
			super().__init__(directory, filename="symbols_map.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=None)
			else:
				super().read(drop=True,idx_col=None)
		else:
			super().__init__(directory, filename="symbols_map.csv", columns=SymbolsMap_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==SymbolsMap_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {SymbolsMap_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class AccountOrders_IO(datafile.DataFile):
	COLUMNS = ['symbol', 'unit']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="account_orders.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=None)
			else:
				super().read(drop=True,idx_col=None)
		else:
			super().__init__(directory, filename="account_orders.csv", columns=AccountOrders_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==AccountOrders_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {AccountOrders_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class Allocations_IO(datafile.DataFile):
	COLUMNS = ['book', 'portfolio', 'pkey', 'date', 'symbol', 'action', 'unit', 'exec_price', 'cost', 'linked_buy_pkey']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="allocation.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=[0])
			else:
				super().read(drop=True,idx_col=[0])
		else:
			super().__init__(directory, filename="allocation.csv", columns=Allocations_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==Allocations_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {Allocations_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class AcctDailyOrders_IO(datafile.DataFile):
	COLUMNS = ['book', 'portfolio', 'date', 'symbol', 'action', 'unit', 'price', 'linked_buy_pkey', 'pkey']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="daily_orders.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=[0])
			else:
				super().read(drop=True,idx_col=[0])
		else:
			super().__init__(directory, filename="daily_orders.csv", columns=AcctDailyOrders_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==AcctDailyOrders_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {AcctDailyOrders_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class AcctDividendTxns_IO(datafile.DataFile):
	COLUMNS = ['line#', 'Date', 'Symbol', 'Amount', 'status', 'pkey']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="dividend_txn.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="dividend_txn.csv", columns=AcctDividendTxns_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==AcctDividendTxns_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {AcctDividendTxns_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class DividendTxnsAdj_IO(datafile.DataFile):
	COLUMNS = ['line#', 'pkey', 'adj_Amount', 'note']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="dividend_txn_adj.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="dividend_txn_adj.csv", columns=DividendTxnsAdj_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==DividendTxnsAdj_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {DividendTxnsAdj_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class DividendTxnsStaging_IO(datafile.DataFile):
	COLUMNS = ['line#', 'Date', 'S_IOymbol', 'Amount', 'status', 'pkey']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="dividend_txn_staging.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="dividend_txn_staging.csv", columns=DividendTxnsStaging_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==DividendTxnsStaging_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {DividendTxnsStaging_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class Executions_IO(datafile.DataFile):
	COLUMNS = ['line#', 'Symbol', 'Shares', 'Price', 'Amount']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="executions.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="executions.csv", columns=Executions_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==Executions_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {Executions_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class Matchings_IO(datafile.DataFile):
	COLUMNS = ['date', 'symbol', 'ord_qty', 'exec_qty', 'exec_price', 'ttl_cost', 'match', 'exec_pkey']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="matching.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=[0])
			else:
				super().read(drop=True,idx_col=[0])
		else:
			super().__init__(directory, filename="matching.csv", columns=Matchings_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==Matchings_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {Matchings_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class OtherHoldings_IO(datafile.DataFile):
	COLUMNS = ['line#', 'symbol', 'quantity', 'note']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="other_holdings.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="other_holdings.csv", columns=OtherHoldings_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==OtherHoldings_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {OtherHoldings_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class AcctPositions_IO(datafile.DataFile):
	COLUMNS = ['line#', 'Symbol', 'Desc', 'Last Price', 'Price Change', '$ Gain/Loss', '% Gain/Loss', '$ Total Gain/Loss', '% Total Gain/Loss', 'Current Value', '% of Account', 'Quantity', 'Per Share', 'Total Cost', '52w lo', '52w hi']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="positions.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="positions.csv", columns=AcctPositions_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==AcctPositions_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {AcctPositions_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class AcctPositionReport_IO(datafile.DataFile):
	COLUMNS = ['line#', 'index', 'account_holding', 'portfs_holding', 'other_holding', 'holding_diff']

	def __init__(self, db_dir, account, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.account = account
		self.db_dir = db_dir
		directory = os.path.join(db_dir, account)
		if(load):
			super().__init__(directory, filename="position_report.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="position_report.csv", columns=AcctPositionReport_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==AcctPositionReport_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {AcctPositionReport_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class PortfSetting_IO(datafile.DataFile):
	COLUMNS = ['value', 'dtype']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="portf_setting.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=[0])
			else:
				super().read(drop=True,idx_col=[0])
		else:
			super().__init__(directory, filename="portf_setting.csv", columns=PortfSetting_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==PortfSetting_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {PortfSetting_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class PortfDailyOrders_IO(datafile.DataFile):
	COLUMNS = ['book', 'portfolio', 'date', 'symbol', 'action', 'unit', 'price', 'linked_buy_pkey', 'pkey']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="daily_orders.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=[0])
			else:
				super().read(drop=True,idx_col=[0])
		else:
			super().__init__(directory, filename="daily_orders.csv", columns=PortfDailyOrders_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==PortfDailyOrders_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {PortfDailyOrders_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class ExitConds_IO(datafile.DataFile):
	COLUMNS = ['entry_exec_date', 'cost', 'action', 'symbol', 'unit', 'entry_price', 'pkey', 'uid', 'stops', 'exit_trigger', 'last_close', 'stops/symbol_dropped', 'stops/duration_stop', 'duration_stop']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="exit_cond.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=[0])
			else:
				super().read(drop=True,idx_col=[0])
		else:
			super().__init__(directory, filename="exit_cond.csv", columns=ExitConds_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==ExitConds_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {ExitConds_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class PortfDividendTxns_IO(datafile.DataFile):
	COLUMNS = ['line#', 'account', 'pay_date', 'enter_date', 'type', 'symbol', 'amount', 'dtxn_pkey', 'unit', 'note1']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="dividend_txn.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="dividend_txn.csv", columns=PortfDividendTxns_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==PortfDividendTxns_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {PortfDividendTxns_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class PortfPositions_IO(datafile.DataFile):
	COLUMNS = ['line#', 'date', 'cost', 'type', 'symbol', 'unit', 'entry price', 'pkey']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="open_pos.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=['line#'])
			else:
				super().read(drop=True,idx_col=['line#'])
		else:
			super().__init__(directory, filename="open_pos.csv", columns=PortfPositions_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==PortfPositions_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {PortfPositions_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class PairedTxns_IO(datafile.DataFile):
	COLUMNS = ['date', 'cost', 'type', 'symbol', 'unit', 'entry price', 'pkey', 'linked_sell_txn']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="paired_txn.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=[0])
			else:
				super().read(drop=True,idx_col=[0])
		else:
			super().__init__(directory, filename="paired_txn.csv", columns=PairedTxns_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==PairedTxns_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {PairedTxns_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class Buylist_IO(datafile.DataFile):
	COLUMNS = ['symbol']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="buylist.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=None)
			else:
				super().read(drop=True,idx_col=None)
		else:
			super().__init__(directory, filename="buylist.csv", columns=Buylist_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==Buylist_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {Buylist_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class ExitConds_v5_IO(datafile.DataFile):
	COLUMNS = ['entry_exec_date', 'cost', 'action', 'symbol', 'unit', 'entry_price', 'pkey', 'uid', 'stops', 'exit_trigger', 'last_close', 'stops/symbol_dropped', 'stops/duration_stop', 'duration_stop']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="exit_cond_v5_01.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=[0])
			else:
				super().read(drop=True,idx_col=[0])
		else:
			super().__init__(directory, filename="exit_cond_v5_01.csv", columns=ExitConds_v5_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==ExitConds_v5_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {ExitConds_v5_IO} object")
		if(raise_on_err):
			raise error
		return error
		
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
# -- ---------------------------------------------------------------------------------
class Buylist_v5_IO(datafile.DataFile):
	COLUMNS = ['symbol']

	def __init__(self, db_dir, strategy,portfolio, df0: Optional[pd.DataFrame] = None, load=False, create=True):
		self.strategy = strategy
		self.portfolio = portfolio
		self.db_dir = db_dir
		directory = os.path.join(db_dir, strategy,portfolio)
		if(load):
			super().__init__(directory, filename="buylist_v5_01.csv", columns=None, df0=None)
			if(create):
				super().read(drop=True,columns=self.COLUMNS,idx_col=None)
			else:
				super().read(drop=True,idx_col=None)
		else:
			super().__init__(directory, filename="buylist_v5_01.csv", columns=Buylist_v5_IO.COLUMNS, df0=df0)

	def _type_validate_(data,raise_on_err=True):
		matching_type = type(data)==Buylist_v5_IO
		if(matching_type):
			return None
		error = ValueError(f"Only applicable to {Buylist_v5_IO} object")
		if(raise_on_err):
			raise error
		return error
		
