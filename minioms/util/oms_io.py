# --
# -- basic lingo
# -- algo      -- market independent entry/exit algo (eg. sotm_rdex)
# -- strategy  -- algo + market (eg. sotm_rdex_n100)
# -- portfolio -- an instance, with actual txn & positions, of strategy (eg. v0_1)
# --
# !! book -- has no meaning in this system, except in legacy code and csv files
# !!      -- which should be replace with up-to-date naming scheme (eventually)
# !!      -- new code should not use "book" except interface with old schema
# --
from ..oms_db import classes_io as oio
from ..obj.SymbolsMap import io_utility as symm_u_io
# --
# --
# --
# --
def load_symbols_map__sfunc(directory):
	symmap = oio.SymbolsMap_IO(db_dir=directory,load=True)
	return symmap.df.copy()

# --
# --
# --
from ..obj.OtherHoldings import io_utility as othh_u_io
# --
def load_other_holdings_for_acct__bk_rpt(*,db_folder,account):
	other_holdings = oio.OtherHoldings_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return othh_u_io.bookkeeper_report_load_wrapper(
		other_holdings.df.copy()
	)

# --
# --
# --
from ..obj.AccountOrders import io_utility as ao_u_io
# --
def write_to_account_orders__bk_dord(*,db_folder,account,df0):
	acc_ord = oio.AccountOrders_IO(
		db_dir=db_folder,
		account=account,
		df0=df0
	)
	acc_ord.write()

def load_account_orders__bk_pospro(*,db_folder,account):
	acc_ord = oio.AccountOrders_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return acc_ord.df.copy()

def load_account_orders__bk_rpt(*,db_folder,account):
	return load_account_orders__bk_pospro(**locals)

# -- rm -- def load_account_orders__bk_rpt(*,db_folder,account):
# -- rm -- 	acc_ord = oio.AccountOrders_IO(
# -- rm -- 		db_dir=db_folder,
# -- rm -- 		account=account,
# -- rm -- 		load=True
# -- rm -- 	)
# -- rm -- 	return acc_ord.df.copy()

# --
# --
# --
from ..obj.Accounts import io_utility as acct_u_io
# --
def load_tbsys_accounts__bk_rpt(*,db_folder):
	return oio.Accounts_IO(
		db_dir=db_folder,
		load=True
	).df.copy()

# --
# --
# --
from ..obj.Books import io_utility as book_u_io
# --
def load_tbsys_books__bk_rpt(*,db_folder):
	return oio.Books_IO(
		db_dir=db_folder,
		load=True
	).df.copy()

# --
# --
# --
from ..obj.Portfolios import io_utility as portf_u_io
# --
def load_tbsys_portfs__bk_rpt(*,db_folder):
	return oio.Portfolios_IO(
		db_dir=db_folder,
		load=True
	).df.copy()

def load_tbsys_portf_account_map__bk_dord(*,db_folder):
	return load_tbsys_portfs__bk_rpt(db_folder=db_folder)

# -- rm -- def load_tbsys_portf_account_map__bk_dord(*,db_folder):
# -- rm -- 	portfs = oio.Portfolios_IO(
# -- rm -- 		db_dir=db_folder,
# -- rm -- 		load=True
# -- rm -- 	)
# -- rm -- 	return portfs.df.copy()

# --
# --
# --
from ..obj.Allocations import io_utility as alloc_u_io
# --
def write_to_allocation__bk_pospro(*,db_folder,account,df0):
	allocation = oio.Allocations_IO(
		db_dir=db_folder,
		account=account,
		df0=df0
	)
	allocation.write()

# --
# --
# --
from ..obj.Executions import io_utility as exec_u_io
# --
def load_account_executions_raw__bk_pospro(*,db_folder,account):
	acc_ord = oio.Executions_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return acc_ord.df.copy()

def write_executions__tbcom(*,db_folder,account,df0):
	df0.index.name = 'line#'
	executions = oio.Executions_IO(
		db_dir=db_folder,
		account=account,
		df0=df0
	)
	return executions.write()

# --
# --
# --
from ..obj.Matchings import io_utility as match_u_io
# --
def write_to_matchings__bk_pospro(*,db_folder,account,df0):
	matchings = oio.Matchings_IO(
		db_dir=db_folder,
		account=account,
		df0=df0
	)
	matchings.write()

# --
# --
# --
def load_account_positions__bk_rpt(*,db_folder,account):
	positions = oio.AcctPositions_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return positions.df.copy()

def write_acct_positions__tbcom(*,db_folder,account,df0):
	df0.index.name = 'line#'
	positions = oio.AcctPositions_IO(
		db_dir=db_folder,
		account=account,
		df0=df0
	)
	return positions.write()

# --
# --
# --
def write_div_txns_staging__bk_pospro(*,db_folder,account,df0):
	df0.index.name = 'line#'
	div_staging = oio.DividendTxnsStaging_IO(
		db_dir=db_folder,
		account=account,
		df0=df0
	)
	div_staging.write()

def load_div_txns_staging__bk_div(*,db_folder,account):
	div_staging = oio.DividendTxnsStaging_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return div_staging.df.copy()

# --
# --
# --
def load_div_txns_adj__bk_div(*,db_folder,account):
	div_adj = oio.DividendTxnsAdj_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return div_adj.df.copy()

# --
# --
# --
def write_div_txns__bk_div(*,db_folder,account,df0):
	df0.index.name = 'line#'
	div_txn = oio.AcctDividendTxns_IO(
		db_dir=db_folder,
		account=account,
		df0=df0
	)
	div_txn.write()

def load_div_txns__bk_div(*,db_folder,account):
	div_txn = oio.AcctDividendTxns_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return div_txn.df.copy()

# -- dup of load_portf_div_txns__bk_div
# -- rm -- def load_portf_dividend__bk_div(*,db_folder,strategy,portfolio):
# -- rm -- 	div_txn = oio.PortfDividendTxns_IO(
# -- rm -- 		db_dir=db_folder,
# -- rm -- 		strategy=strategy,
# -- rm -- 		portfolio=portfolio,
# -- rm -- 		load=True
# -- rm -- 	)
# -- rm -- 	return div_txn.df.copy()

# --
# --
# --
def load_portf_div_txns__bk_div(*,db_folder,strategy,portfolio):
	div_txn = oio.PortfDividendTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return div_txn.df.copy()

def write_portf_div_txns__bk_div(*,db_folder,strategy,portfolio,df0):
	div_txn = oio.PortfDividendTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		df0=df0
	)
	div_txn.write()

def load_portf_div_txns__bk_exp_gsp(*,db_folder,strategy,portfolio):
	div_txn = oio.PortfDividendTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	# --
	# -- not sure what the 'line#' column for, remove it for now
	# !! might need to fix the source
	# --
	return div_txn.df.reset_index(drop=True)

def load_dividend__bk_dord(*,db_folder,strategy,portfolio):
	div_txn = oio.PortfDividendTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return div_txn.df.copy()

def load_dividend__bk_rpt(*,db_folder,strategy,portfolio):
	div_txn = oio.PortfDividendTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return div_txn.df.copy()

# --
# --
# --
def load_account_daily_orders__bk_dord(*,db_folder,account):
	acct_dord = oio.AcctDailyOrders_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return acct_dord.df.copy()

def write_account_daily_orders__bk_dord(*,db_folder,account,df0):
	acct_dord = oio.AcctDailyOrders_IO(
		db_dir=db_folder,
		account=account,
		df0=df0
	)
	acct_dord.write()

def load_daily_orders__bk_pospro(*,db_folder,account):
	acct_dord = oio.AcctDailyOrders_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return acct_dord.df.copy()

# --
# --
# --
def load_portf_orders__bk_dord(*,db_folder,strategy,portfolio):
	portf_dord = oio.PortfDailyOrders_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return portf_dord.df.copy()

def load_daily_orders__bk_rpt(*,db_folder,strategy,portfolio):
	portf_dord = oio.PortfDailyOrders_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return portf_dord.df.copy()

# --
# --
# --
def load_openpos__bk_dord(*,db_folder,strategy,portfolio):
	openpos = oio.PortfPositions_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return openpos.df.copy()

def load_open_positions__bk_exp_gsp(*,db_folder,strategy,portfolio):
	openpos = oio.PortfPositions_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return openpos.df.reset_index(drop=True)

def write_open_pos__bk_pospro(*,db_folder,strategy,portfolio,df0):
	df0.index.name = 'line#'
	open_pos = oio.PortfPositions_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		df0=df0
	)
	open_pos.write()

def load_open_pos__bk_rpt(*,db_folder,strategy,portfolio):
	openpos = oio.PortfPositions_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return openpos.df.reset_index(drop=True)

def load_open_pos__bk_d_upd(*,db_folder,strategy,portfolio):
	openpos = oio.PortfPositions_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return openpos.df.copy()

# --
# --
# --
def load_paired_txn__bk_dord(*,db_folder,strategy,portfolio):
	pairedtxn = oio.PairedTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return pairedtxn.df.copy()

def load_paired_txn__bk_div(*,db_folder,strategy,portfolio):
	pairedtxn = oio.PairedTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return pairedtxn.df.copy()

def load_paired_txn__bk_exp_gsp(*,db_folder,strategy,portfolio):
	pairedtxn = oio.PairedTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return pairedtxn.df.copy()

def load_paired_txn__bk_pospro(*,db_folder,strategy,portfolio):
	pairedtxn = oio.PairedTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return pairedtxn.df.copy()

def write_paired_txn__bk_pospro(*,db_folder,strategy,portfolio,df0):
	pairedtxn = oio.PairedTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		df0=df0
	)
	pairedtxn.write()

def load_paired_txns_bk_rpt(*,db_folder,strategy,portfolio):
	pairedtxn = oio.PairedTxns_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return pairedtxn.df.copy()

# --
# --
# --
def load_exitcond__bk_dord(*,db_folder,strategy,portfolio):
	exitconds = oio.ExitConds_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		load=True
	)
	return exitconds.df.copy()

def write_exitcond_v5__pf_upd_v5(*,db_folder,strategy,portfolio,df0):
	exitcond = oio.ExitConds_v5_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		df0=df0
	)
	exitcond.write()

def write_buylist_v5__pf_upd_v5(*,db_folder,strategy,portfolio,df0):
	buylist = oio.Buylist_v5_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=portfolio,
		df0=df0
	)
	buylist.write()

def load_buylist__op_gen_portf_orders(*,db_folder,strategy,book_name):
	buylist = oio.Buylist_IO(
		db_dir=db_folder,
		strategy=strategy,
		portfolio=book_name,
		load=True
	)
	return buylist.df.copy()

