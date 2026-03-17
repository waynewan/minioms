import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF8')
# --
import sys
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: os is only used by the sys.path.append scaffolding block below
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: (marked REVIEWED;pending_rm). Once that block is removed, os
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: becomes an orphaned import and should be removed alongside it.
# -- (HUM) REVIEWED;pending_rm -- import os
# -- (HUM) REVIEWED;pending_rm -- # !!
# -- (HUM) REVIEWED;pending_rm -- # !! this is a bad idea, possible solution,
# -- (HUM) REVIEWED;pending_rm -- # !! publish the library (oms) as a package
# -- (HUM) REVIEWED;pending_rm -- # !!
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: sys.path.append at module level is an import-time side effect and
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: fragile (relies on relative directory traversal). The same pattern
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: exists in op_gen_portf_orders.py and was removed there.
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: Fix: publish minioms as a proper package (as the !! note above suggests),
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: then remove these four lines and the sys/os imports if no longer needed.
# -- (HUM) REVIEWED;pending_rm -- __abspath = os.path.abspath(__file__)
# -- (HUM) REVIEWED;pending_rm -- __dirname = os.path.dirname(__abspath)
# -- (HUM) REVIEWED;pending_rm -- common_dir = f"{__dirname}/../../../../../common"
# -- (HUM) REVIEWED;pending_rm -- sys.path.append(f"{common_dir}/lib/quick_func")
# --
import pandas as pd
import numpy as np
import datetime
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
# --
import re
from .external_interface import mktprc_loader, load_market_price, load_market_price_impl
from itertools import product
from ..obj.PortfSetting import io_utility as portfset_io
from ..obj.PortfSetting import br_utility as portfset_br
from ..obj.Accounts import io_utility as acct_u_io
from ..obj.Books import io_utility as books_u_io
from ..obj.Portfolios import io_utility as portfs_u_io
from ..obj.PortfDailyOrders import io_utility as portfdord_u_io
from ..obj.AcctPositions import io_utility as acctpos_u_io
from ..obj.PortfPositions import io_utility as portfpos_u_io
from ..obj.PairedTxns import io_utility as pairedtxns_u_io
from ..obj.PortfDividendTxns import io_utility as portfdtxns_u_io
from ..obj.OtherHoldings import io_utility as othh_u_io
from ..obj.OtherHoldings import br_utility as othh_u_br
from ..obj.AcctPositionReport import io_utility as acctposrpt_u_io
# --

def __p__(*args):
	print(*args)

# --
# --
# --

def __load_other_holdings_for_acct__bk_rpt(*,db_folder,account):
	from ..oms_db import classes_io as oio
	other_holdings = oio.OtherHoldings_IO(
		db_dir=db_folder,
		account=account,
		load=True
	)
	return othh_u_br.group_by_symbol(
			othh_u_io.bookkeeper_report_load_wrapper(
				other_holdings.df.copy()
			)
		)

def check_version(book_version,version):
	if(version>book_version):
		raise Exception(f"book version is {book_version}; require version is {version} or above")
	print(f"book version is {book_version}; require version is {version} or above")


def load_tbsys_accounts(*,db_folder):
	return acct_u_io.load(db_dir=db_folder).df.copy()

# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: load_tbsys_books has no callers in the util directory.
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: Verify whether it is called externally (e.g. notebooks, scripts).
# -- (HUM) REVIEWED;pending_rm -- # (CLU) NEED_REVIEW: If not, remove it and the books_u_io import.
# -- (HUM) REVIEWED;pending_rm -- def load_tbsys_books(*,db_folder):
# -- (HUM) REVIEWED;pending_rm -- 	return books_u_io.load(db_dir=db_folder).df.copy()

def load_tbsys_portfs(*,db_folder):
	return portfs_u_io.load(db_dir=db_folder).df.copy()

def load_daily_orders(*,db_folder,book,portf):
	orders = portfdord_u_io.load(db_dir=db_folder,strategy=book,portfolio=portf).df.copy()
	orders = orders.iloc[:,1:]
	return orders


def load_account_positions(*,db_folder,account):
	return acctpos_u_io.load(db_dir=db_folder,account=account).df.copy()
	

def load_openpos(*,db_folder,strategy,book_name,incl_rt=True):
	openpos = portfpos_u_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.reset_index(drop=True)
	summary = openpos.groupby("symbol").sum()["unit"]
	if(incl_rt):
		mktval = load_market_price(pd.DataFrame(summary).reset_index(inplace=False))
		mktval['value'] = mktval['unit'] * mktval['price']
	else:
		mktval = pd.DataFrame(summary).reset_index(inplace=False)
		mktval['price'] = 0
		mktval['value'] = 0
	return (openpos,mktval)

def load_txns(*,db_folder,strategy,book_name,details_only=False,drop_cash_txn=True):
	txns = pairedtxns_u_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.copy()
	if(drop_cash_txn):
		txns = txns[ (txns['type']=='BUY') + (txns['type']=='SEL') ]
	if(details_only):
		return txns 
	balance = txns['cost'].sum()
	return txns,balance

def load_dividend(*,db_folder,strategy,book_name,details_only=False):
	dividend = portfdtxns_u_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.copy()
	if(details_only):
		return dividend
	ttl_divy = dividend['amount'].sum()
	return dividend,ttl_divy

def compute_benchmark_value_for_portf(*,symbol,fromDate,toDate=None,ndays=None):
	raw0 = mktprc_loader().get_eod_hist(symbol=symbol,fromDate=fromDate,toDate=toDate,ndays=ndays)
	rtQuote = mktprc_loader().get_simple_quote([symbol])
	# print(rtQuote)
	df0 = pd.DataFrame(raw0['historical']).set_index('date',drop=True).sort_index(ascending=True)
	# print(df0.iloc[[0,-1]])
	# df1 = df0.iloc[[0,-1]]['close']
	df1 = df0.iloc[[0,-1]]['adjClose']
	# print(symbol)
	# print(df1)
	# return df1.iloc[1] / df1.iloc[0]
	return rtQuote[0]['price'] / df1.iloc[0]

def publish_portfolio_to_gspread(*,db_folder,strategy,market,portf):
	book = f"{strategy}_{market}"
	settings = portfset_io.load(db_dir=db_folder,strategy=book,portfolio=portf)
	basic_info = portfset_br.basic_info_from(portf_setting=settings,optional={"benchmark":portfset_br.get_def_benchmark(book=market)})
	_,dividend = load_dividend(db_folder=db_folder,strategy=book,book_name=portf)
	_,txn_balance = load_txns(db_folder=db_folder,strategy=book,book_name=portf)
	openpos,pos_summary = load_openpos(db_folder=db_folder,strategy=book,book_name=portf)
	n_openpos = len(openpos)
	n_empty = basic_info['maxpos'] - n_openpos
	principle = basic_info["principle"]
	cashval = principle + txn_balance + dividend
	mktval = pos_summary['value'].sum()
	ttlval = mktval + cashval
	p_and_l = ttlval - principle
	benchmark_market_value = principle * compute_benchmark_value_for_portf(
		symbol=basic_info['benchmark'],
		fromDate=basic_info['start_date'],
		toDate=None,
		ndays=30,
	)
	dollar_alpha = ttlval - benchmark_market_value
	# --
	# -- format data
	# --
	df0 = pd.DataFrame(
		index=[
			"strategy",
			"market",
			"book",
			"portf",
			"startdate",
			"maxpos",
			"#position",
			"#empty slot",
			"principle",
			"dividend",
			"txn balance",
			"cash value",
			"market value",
			"total value",
			"P&L",
			"#0#",
			"bmk mkt val",
			"$ alpha",
			"###",
		],
		data=np.array([
			strategy,
			market,
			book,
			portf,
			basic_info["start_date"],
			basic_info["maxpos"],
			n_openpos,
			n_empty,
			basic_info["principle"],
			dividend,
			txn_balance,
			cashval,
			mktval,
			ttlval,
			p_and_l,
			"",
			benchmark_market_value,
			dollar_alpha,
			"",
		],
	))
	# --
	# -- pos and mktval
	# --
	pos_summary = pos_summary.set_index('symbol')
	pos_summary.columns = [ 0, 1, 2 ]
	df0 = pd.concat([df0, pos_summary], axis=0)
	# --
	return df0

def load_report_for_book(*,db_folder,strategy,market):
	book = f"{strategy}_{market}"
	portf = load_tbsys_portfs(db_folder=db_folder)
	portf = portf[portf['book']==book]
	portfs = portf['portfolio'].tolist()
	dfs = []
	for portf in portfs:
		df0 = publish_portfolio_to_gspread(db_folder=db_folder,strategy=strategy,market=market,portf=portf)
		dfs.append(df0)
	# df0 = pd.concat(dfs,axis=1,keys=portfs)
	df0 = pd.concat(dfs,axis=1,keys=product([strategy],[market],portfs))
	return df0

def format_report_1(df0):
	# --
	pd.set_option('future.no_silent_downcasting', True)
	# --
	float_columns = [ 'principle', 'dividend', 'txn balance', 'cash value', 'market value', 'total value', 'bmk mkt val', '$ alpha' ]
	int_columns = [ 'maxpos', '#position', '#empty slot' ]
	df1 = df0.transpose()
	for scol in df1.columns:
		if(scol[0].isupper() or scol in float_columns):
			df1[scol] = df1[scol].fillna(0)
			df1[scol] = df1[scol].astype(np.float64).round(2)
		elif(scol in int_columns):
			df1[scol] = df1[scol].fillna(0)
			df1[scol] = df1[scol].astype(np.int64)
		else:
			df1[scol] = df1[scol].fillna('--')
	return df1

def create_report_for_strategy(*,db_folder,strategy,formatter=None,single_df=False):
	if(single_df):
		result = create_report_for_strategy_impl(db_folder=db_folder,strategy=strategy,formatter=None)
		df_strategy = pd.concat([ result['n100'], result['s500'], result['r1000'], ],axis=1)
		if(formatter is None):
			return df_strategy
		else:
			return formatter(df_strategy)
	else:
		result = create_report_for_strategy_impl(db_folder=db_folder,strategy=strategy,formatter=formatter)
		return result

def create_report_for_strategy_impl(*,db_folder,strategy,formatter=None):
	df0 = load_report_for_book(db_folder=db_folder,strategy=strategy,market="n100")
	if(formatter is not None):
		df0 = formatter(df0)
	# --
	df1 = load_report_for_book(db_folder=db_folder,strategy=strategy,market="s500")
	if(formatter is not None):
		df1 = formatter(df1)
	# --
	df2 = load_report_for_book(db_folder=db_folder,strategy=strategy,market="r1000")
	if(formatter is not None):
		df2 = formatter(df2)
	# --
	return {'n100':df0, 's500':df1, 'r1000':df2 }

def print_report_for_strategy(*,db_folder,strategy,formatter=None):
	result = create_report_for_strategy(db_folder=db_folder,strategy=strategy,formatter=formatter)
	# --
	print(result['n100'])
	print()
	print("#"*120)
	# --
	print(result['s500'])
	print()
	print("#"*120)
	# --
	print(result['r1000'])
	print()
	print("#"*120)
	# --
	return result

# --
# -- _10_pm_bank_pos_recon
# --
def load_portfs_for_account(*,db_folder,account):
	portfs = load_tbsys_portfs(db_folder=db_folder)
	portfs = portfs[portfs['trade_acct']==account]
	return portfs.iloc[:,:2]

def load_account_blotter(*,db_folder,account):
	positions = load_account_positions(db_folder=db_folder,account=account)
	positions = positions.loc[:,("Symbol","Quantity")].dropna()
	positions["Quantity"] = positions["Quantity"].astype(int)
	return positions
	
def load_openpos_for_portf(*,db_folder,book,portf):
	openpos = load_openpos(db_folder=db_folder,strategy=book,book_name=portf,incl_rt=False)
	return openpos[1].iloc[:,:2]

def load_other_holdings_for_acct(*,db_folder,account):
	return __load_other_holdings_for_acct__bk_rpt(**locals())

def load_account_position_report(*,db_folder,account):
	return acctposrpt_u_io.load(db_dir=db_folder,account=account).df.copy()

def write_account_position_report(*,db_folder,account,report):
	acctposrpt_u_io.save(db_dir=db_folder,account=account,report_df=report)

def parse_portf_name(portf):
	parser = re.compile('^(.*)_(n100|s500|r1000)[_|]([A-Z_a-z0-9]*)$')
	result = parser.match(portf)
	if(result is None):
		raise ValueError(f"[{portf}] not a portf name")
	strat,book,portf = result.groups()
	return strat,book,portf

def parse_book_name(book):
	parser = re.compile('^(.*)_(n100|s500|r1000)$')
	result = parser.match(book)
	if(result is None):
		raise ValueError(f"[{book}] not a book name")
	strat,book = result.groups()
	return strat,book

def load_openpos_for_account(*,db_folder,account):
	book_portfs = load_portfs_for_account(db_folder=db_folder,account=account)
	positions = []
	keys = []
	for nn,rr in book_portfs.iterrows():
		book,portf = rr.tolist()
		keys.append(f"{book}_{portf}")
		positions.append(load_openpos_for_portf(db_folder=db_folder,book=book,portf=portf).set_index("symbol"))
	positions = pd.concat(positions, keys=keys,axis=1)
	positions = positions.droplevel(level=1,axis=1)
	return positions

def compare_account_portfs_holding(*,db_folder,account):
	account_holding = None
	try:
		account_holding = load_account_blotter(db_folder=db_folder,account=account)
		account_holding.columns = ['symbol','account_holding']
	except Exception as ex:
		print(f"ERR(account={account}): {ex}")
		return None
	# --
	recorded_positions = load_openpos_for_account(db_folder=db_folder,account=account)
	other_holdings = load_other_holdings_for_acct(db_folder=db_folder,account=account)
	# --
	total_positions = pd.DataFrame( recorded_positions.sum(axis=1).astype(int), columns=['portfs_holding'])
	account_holding = pd.merge( account_holding, total_positions, how="outer", left_on="symbol", right_index=True)
	account_holding = pd.merge( account_holding, other_holdings, how="outer", on="symbol")
	# !!
	# !! can only do this after other_holding is expanded to account_holding dimension !!
	# !!
	account_holding['account_holding'] = account_holding['account_holding'].fillna(0)
	account_holding['portfs_holding'] = account_holding['portfs_holding'].fillna(0)
	account_holding['other_holding'] = account_holding['other_holding'].fillna(0)
	account_holding['holding_diff'] = account_holding['account_holding'] - account_holding['portfs_holding'] - account_holding['other_holding']
	account_holding = account_holding.replace(0,"--")
	account_holding = account_holding.sort_values(by="symbol").reset_index()
	# -- DEBUG -- display(account_holding)
	# --
	holding_cmp = pd.merge(account_holding, recorded_positions, how="outer", left_on="symbol", right_index=True)
	holding_cmp = holding_cmp.fillna("--")
	holding_cmp = holding_cmp.set_index("symbol")
	return holding_cmp

def compare_all_accounts_holding(*,db_folder,accounts):
	holding_cmp_dfs = []
	holding_diff_dfs = []
	for account in accounts:
		holding_cmp = compare_account_portfs_holding(db_folder=db_folder,account=account)
		if(holding_cmp is None):
			holding_cmp_dfs.append(pd.DataFrame())
			holding_diff_dfs.append(pd.Series())
		else:
			holding_cmp_dfs.append(holding_cmp)
			holding_diff_dfs.append(holding_cmp['holding_diff'])
	return holding_cmp_dfs,holding_diff_dfs

def format_holding_diffs(*,accounts,holding_diff_dfs):
	holding_diffs = pd.concat(holding_diff_dfs, keys=accounts, axis=1)
	holding_diffs = holding_diffs.fillna("--").replace("--",np.nan).dropna(axis=0,how='all').fillna("--")
	return holding_diffs

# --
# -- _11_pm_report_portfolio_summary
# --
def format_strategy_report(df0):
	frame0 = df0.loc[(slice(None),slice(None),slice(None),0)]
	book_summary = frame0.groupby("book").sum().loc[:,"maxpos":]
	strat_summary = frame0.groupby("strategy").sum().loc[:,"maxpos":]
	# --
	# -- display df0 in 2 part, book_details, and holding table
	# --
	# -- pt1
	book_details = df0.loc[(slice(None),slice(None),slice(None),0),:'###']
	# -- pt2
	holdings = df0.transpose().iloc[book_details.shape[1]:]
	return ( 
		pd.DataFrame(strat_summary), 
		book_summary, 
		book_details, 
		holdings.sort_index().replace(0,"--"), 
	)
		
def format_all_strats_summary(df_strats):
	df_all = pd.concat(df_strats.values(),axis=0)
	df_all_info = df_all.loc[:,:'###']
	df_all_info['#0#'] = u"\u258D"
	df_all_info['###'] = u"\u258D"
	df_all_holding = df_all.iloc[:,df_all_info.shape[1]:].transpose().fillna(0).astype(int)
	total_positions = pd.DataFrame(np.array([
		df_all_holding.index,
		df_all_holding.sum(axis=1)
	]).transpose(), columns=['symbol','quantity'])
	total_positions = load_market_price(total_positions)
	total_positions = pd.merge(df_all_holding, total_positions, left_index=True, right_index=False, right_on='symbol')
	total_positions.set_index("symbol",drop=True,inplace=True)
	total_positions['mktval'] = (total_positions['quantity'] * total_positions['price']).astype(np.float64)
	total_mktval = total_positions['mktval'].sum()
	total_positions['%-ttl'] = np.round( total_positions['mktval']/total_mktval * 100,2 )
	total_positions['%-bar'] = total_positions['%-ttl'].apply(lambda r: "#" * int(round(max(1.0,r),0)) )
	# --
	total = df_all.sum()
	total_summary = total.loc[:'###']
	total_summary.drop(["#0#","###"],inplace=True)
	# --
	return total_summary,df_all_info,total_positions

# --
# --
# --
def parse_options(argv):
	portf = f"strat1_0LPAF2_{argv[1]}"
	book = argv[2]
	in_opt = argv[3:]
	options = {
		"portf" : portf,
		"book" : book,
		"dry_run" : "dr" in in_opt,
	}
	return options

def main(argv):
	__p__("#", "-" * 80)
	options = parse_options(argv)
	__p__("# --", options)
	__p__("#", "-" * 80)

if(__name__=="__main__"):
	main(sys.argv)

