import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF8')
# --
import pickle
import time
import pandas as pd
import numpy as np
import copy
from collections import defaultdict
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
from jackutil.microfunc import retry,dt_to_str
from jackutil import containerutil as cutil
# --
from pprint import pprint
from pathlib import Path
from simple_func import convert_columns_to_string
from simple_func import get_syst_var
from . import oms_io
import gspread_util as gsu
import sys
import os 
import gspread
from datetime import datetime

# --
# --
db_dir = get_syst_var("db_dir")
svc_cred_fname = get_syst_var("google_svc_cred_filename")
# --
# --

def __p__(*args):
	print(*args)

# --
# -- strategy = books.xml:/*/wb_name
# -- book_name = books.xml:/*/sh_name
# --
# def read_db_path(*,db_folder="../../../algo-active/db",account=None,strategy=None,book_name=None):
def read_db_path(*,db_folder=db_dir,account=None,strategy=None,book_name=None):
	portf_db_dir = None
	if(book_name is not None):
		portf_db_dir = f"{db_folder}/{strategy}/{book_name}"
	elif(account is not None):
		portf_db_dir = f"{db_folder}/{account}"
	elif(strategy is not None):
		portf_db_dir = f"{db_folder}/{strategy}"
	else:
		portf_db_dir = f"{db_folder}/_tbsys_"
	# --
	return portf_db_dir

# --
# --
# --
def open_workbook(wb_name):
	return gsu.authenticate_and_open_tradebook(svc_cred_fname, wb_name)

def open_workbook2(gs_client, wb_name):
	return retry(
		lambda : gs_client.open(wb_name),
		retry=5, exceptTypes=(BaseException,Exception),cooldown=90,rtnEx=False,silent=False
	)

def load_setting_as_df(books,val_as_txt=False):
	flat_portolios = []
	portfolio_names = []
	for portf in books.portfolios:
		flat_portf = cutil.flattenContainer(portf,inclroot=False)
		portfolio_names.append(f"{index_short_from_(flat_portf['wb_name'])}/{flat_portf['sh_name']}")
		flat_portolios.append(flat_portf)
	flat_portolios = pd.DataFrame(flat_portolios,index=portfolio_names).transpose()
	if(val_as_txt):
		for col in flat_portolios.columns:
			flat_portolios[col] = flat_portolios[col].astype(str)
	return flat_portolios

def load_paired_txns(*,db_folder,strategy,book_name,details_only=False,drop_cash_txn=True):
# -- rm -- 	portf_folder = read_db_path(db_folder=db_folder,strategy=strategy,book_name=book_name)
# -- rm -- 	txns = pd.read_csv(f"{portf_folder}/paired_txn.csv")
	txns = oms_io.load_paired_txn__bk_exp_gsp(db_folder=db_folder,strategy=strategy,portfolio=book_name)
	if(drop_cash_txn):
		txns = txns[ (txns['type']=='BUY') + (txns['type']=='SEL') ]
	txns = txns.iloc[:,1:].reset_index(drop=True)
	if(details_only):
		return txns 
	balance = txns['cost'].sum()
	return txns,balance

# -- rm -- def load_open_positions(*,db_folder,strategy,book_name):
# -- rm -- 	portf_folder = read_db_path(db_folder=db_folder,strategy=strategy,book_name=book_name)
# -- rm -- 	# --
# -- rm -- 	# -- not sure what the 'line#' column for, remove it for now
# -- rm -- 	# !! might need to fix the source
# -- rm -- 	# --
# -- rm -- 	positions = pd.read_csv(f"{portf_folder}/open_pos.csv",index_col='line#').reset_index(drop=True)
# -- rm -- 	return positions

def load_open_positions(*,db_folder,strategy,book_name):
	return oms_io.load_open_positions__bk_exp_gsp(db_folder=db_folder,strategy=strategy,portfolio=book_name)

def is_old_dividend_txn_format(txns):
	return "unit" not in txns.columns

def update_dividend_txn_format(txns):
	# -- old -- account,pay_date,enter_date,amount,type,symbol
	# -- new -- account,pay_date,enter_date,type,symbol,amount,dtxn_pkey,unit,note1
	newfmt = txns['account,pay_date,enter_date,type,symbol,amount'.split(',')].copy()
	newfmt['dtxn_pkey'] = '--'
	newfmt['unit'] = '--'
	newfmt['note1'] = '--'
	return newfmt

def load_dividend(*,db_folder,strategy,book_name,details_only=False,drop_cash_txn=True):
	portf_folder = read_db_path(db_folder=db_folder,strategy=strategy,book_name=book_name)
	# --
	# -- not sure what the 'line#' column for, remove it for now
	# !! might need to fix the source
	# --
# -- rm -- 	txns = pd.read_csv(f"{portf_folder}/dividend_txn.csv",index_col='line#').reset_index(drop=True)
	txns = oms_io.load_portf_div_txns__bk_exp_gsp(db_folder=db_folder,strategy=strategy,portf=book_name)
	if(is_old_dividend_txn_format(txns)):
		txns = update_dividend_txn_format(txns)
	if(drop_cash_txn):
		txns = txns[ txns['type']=='DIV' ]
	txns = txns.fillna('--')
	if(details_only):
		return txns 
	balance = txns['amount'].sum()
	return txns,balance

def extract_strategy_bookname(portf_setting):
	portf_setting = portf_setting.set_index('index')
	wb_name = portf_setting.loc['wb_name'].values[0]
	sh_name = portf_setting.loc['sh_name'].values[0]
	return wb_name, sh_name

def index_short_from_(wb_name):
	index_short = list(wb_name.split('_'))[-1]
	return index_short

def join_dataframes(data=[]):
	spacer = pd.DataFrame(np.ndarray(shape=(1,2)), columns=('',''))
	output = []
	for datum in data:
		datum = datum.reset_index(drop=True)
		# display(datum.head())
		output.append(datum)
		output.append(spacer.copy())
	output = pd.concat(output, axis=1)
	# --
	# -- post process cleanup
	# --
	for col in range(len(output.columns)):
		# -- cleanup spacer row
		if(output.columns[col]==''):
			output.iloc[:,col] = ''
	# --
	# -- after concat, a lots of cell will have None, and NaN
	# --
	output.replace(np.nan,'',inplace=True)
	return output

def aggregate_openpos(openpos):
	aggregate = openpos.groupby(by='symbol').agg({'unit':'sum','cost':'sum',})
	aggregate = aggregate.reset_index(drop=False)
	aggregate['cost'] = -aggregate['cost']
	aggregate['avg_prc'] = aggregate['cost'] / aggregate['unit']
	return aggregate

def aggregate_all_openpos_by_sym(all_openpos):
	aggregate = all_openpos.groupby(by='symbol').agg({'unit':'sum','cost':'sum',})
	aggregate = aggregate.reset_index(drop=False)
	aggregate['cost'] = aggregate['cost']
	aggregate['avg_prc'] = aggregate['cost'] / aggregate['unit']
	aggregate['current'] = '=lookup("'+aggregate['symbol']+'",pricer!$A$2:$A$600,pricer!$H$2:$H$600)'
	return aggregate

def aggregate_all_openpos_by_idx_sym(all_openpos):
	aggregate = all_openpos.groupby(by=['strategy','symbol']).agg({'unit':'sum','cost':'sum',})
	aggregate = aggregate.reset_index(drop=False)
	aggregate['cost'] = aggregate['cost']
	aggregate['avg_prc'] = aggregate['cost'] / aggregate['unit']
	return aggregate

def pivot_all_openpos(all_openpos):
	# --
	# -- fold the unit, cost value into a single column (tag the rows)
	# --
	unit = all_openpos.loc[:,['strategy','book_name','symbol']]
	unit['value'] = all_openpos['unit']
	unit['type'] = 'unit'
	cost = all_openpos.loc[:,['strategy','book_name','symbol']]
	cost['value'] = all_openpos['cost']
	cost['type'] = 'cost'
	avg_prc = all_openpos.loc[:,['strategy','book_name','symbol']]
	avg_prc['value'] = all_openpos['cost'] / all_openpos['unit']
	avg_prc['type'] = 'avg_prc'
	# --
	# -- pivoted
	# --
	combined = pd.concat([unit,cost,avg_prc],axis=0)
	combined = combined.set_index(['strategy','book_name','symbol','type'])
	combined = combined.unstack(['strategy','book_name']).fillna('--')
	combined = combined.sort_index(ascending=[True,False],axis=0)
	combined = combined.sort_index(ascending=[True,True],axis=1)
	return combined

def write_strategy_page(*,workbook,sh_name,setting,open_pos,aggregated,paired_txn,div_txn):
	# --
	# -- "setting" : flattened books.py
	# --
	setting = setting.reset_index(drop=True)
	setting.columns = ( 'key','value' )
	joined = join_dataframes(data=[setting,open_pos,aggregated,paired_txn,div_txn])
	# --
	# -- don't know why there is level_0 column, maybe from concat?
	# --
	retry(
		lambda : gsu.write(None, None, sh_name, "A1:AZ1000", joined, write_header=True, clear_range=True, create_sheet=True, workbook=workbook),
		cooldown = 60,
		silent = False,
	)
	
def write_settings_page(settings, workbook):
	range_spec = "A1:AA1000"
	df0 = settings.copy()
	df0.reset_index(inplace=True)
	# --
	# -- some values, such as <class>, cannot be exported, convert to type str
	# --
	for col in df0.columns:
		df0[col] = df0[col].astype(str)
	# --
	retry(
		lambda : gsu.write(None, None, "settings", "A1:AA1000", df0, write_header=True, clear_range=True,create_sheet=True, workbook=workbook),
		cooldown = 60,
		silent = False,
	)

def write_positions_page(workbook, aggregated_all_openpos_by_idx_sym, aggregated_all_openpos_by_sym, pivoted):
	pivoted.columns = [ "#".join(col[1:]) for col in pivoted.columns ]
	pivoted = pivoted.reset_index(drop=False)
	joined = join_dataframes(data=[aggregated_all_openpos_by_idx_sym,aggregated_all_openpos_by_sym,pivoted])
	# --
	retry(
		lambda : gsu.write(None, None, "positions", "A1:AZ1000", joined, write_header=True, clear_range=True, create_sheet=True, workbook=workbook),
		cooldown = 60,
		silent = False,
	)

def write_dividends_page(workbook, all_dividends):
	# --
	# -- some values, such as <class>, cannot be exported, convert to type str
	# --
	for col in all_dividends.columns:
		all_dividends[col] = all_dividends[col].astype(str)
	# --
	retry(
		lambda : gsu.write(None, None, "dividends", "A1:AA1000", all_dividends, write_header=True, clear_range=True,create_sheet=True, workbook=workbook),
		cooldown = 60,
		silent = False,
	)

def insert_header_col(strategy,book_name,df0):
	header = pd.DataFrame([[strategy,book_name]]*len(df0), columns=['strategy','book_name'])
	header.index = df0.index
	return pd.concat([header,df0],axis=1)

# --
# -- <strategy>-export_to_gspread.ipynb
# --
def export_books_to_gspread(*,db_folder,books):
	export_wb_name = books.portfolios[0]['wb_name2']
	workbook = open_workbook(export_wb_name)
	# --
	print(f"working ... {export_wb_name}/settings")
	settings = load_setting_as_df(books,val_as_txt=True)
	write_settings_page(settings, workbook)
	# --
	all_openpos = []
	all_dividends = []
	for col in settings.columns:
		pf0_setting = settings[col].reset_index(drop=False)
		strategy,book_name = extract_strategy_bookname(pf0_setting)
		index_short = index_short_from_(strategy)
		sh_name = f"{index_short}/{book_name}"
		print(f"working ... {sh_name}")
		pairedtxns = load_paired_txns(db_folder=db_folder,strategy=strategy,book_name=book_name,details_only=True,drop_cash_txn=True)
		div_txns = load_dividend(db_folder=db_folder,strategy=strategy,book_name=book_name,details_only=True,drop_cash_txn=True)
		openpos = load_open_positions(db_folder=db_folder,strategy=strategy,book_name=book_name)
		aggregated_openpos = aggregate_openpos(openpos)
		write_strategy_page(workbook=workbook, sh_name=sh_name, setting=pf0_setting, open_pos=openpos, aggregated=aggregated_openpos, paired_txn=pairedtxns, div_txn=div_txns)
		# --
		# -- prepare data for strategy position aggregate
		# --
		aggregated_openpos = aggregated_openpos.loc[:,['symbol','cost','unit']]
		aggregated_openpos['current'] = '=lookup("'+aggregated_openpos['symbol']+'",pricer!$A$2:$A$600,pricer!$H$2:$H$600)'
		aggregated_openpos['mkt_val'] = '=INDIRECT(address(row(),column()-1))*INDIRECT(address(row(),column()-2))'
		all_openpos.append(insert_header_col(strategy,book_name,aggregated_openpos))
		all_dividends.append(insert_header_col(strategy,book_name,div_txns))
	# --
	# -- write position summary (aggregate by symbol)
	# --
	all_openpos = pd.concat(all_openpos,axis=0)
	aggregated_all_openpos_by_sym = aggregate_all_openpos_by_sym(all_openpos)
	aggregated_all_openpos_by_idx_sym = aggregate_all_openpos_by_idx_sym(all_openpos)
	# --
	# -- write position summary (aggregate by index,symbol)
	# --
	# --
	# -- write position summary (pivot: symbol x portf)
	# --
	pivoted_all_openpos = pivot_all_openpos(all_openpos)
	# write_positions_page(workbook=workbook, aggregated_all_openpos_by_idx_sym=aggregated_all_openpos_by_idx_sym, aggregated_all_openpos_by_sym=aggregated_all_openpos_by_sym, pivoted=pivoted_all_openpos)
	write_positions_page(workbook=workbook, aggregated_all_openpos_by_idx_sym=all_openpos, aggregated_all_openpos_by_sym=aggregated_all_openpos_by_sym, pivoted=pivoted_all_openpos)
	# --
	# -- write dividends page
	# --
	all_dividends = pd.concat(all_dividends,axis=0)
	write_dividends_page(workbook=workbook, all_dividends=all_dividends)

# --
# --
# --
def write_orders_page(workbook, all_orders):
	# --
	# -- some values, such as <class>, cannot be exported, convert to type str
	# --
	for col in all_orders.columns:
		all_orders[col] = all_orders[col].astype(str)
	# --
	retry(
		lambda : gsu.write(None, None, "imported_orders", "A1:AA1000", all_orders, write_header=True, clear_range=True,create_sheet=True, workbook=workbook),
		cooldown = 60,
		silent = False,
	)

# --
# -- copied from bookkeeper_daily_orders.py
# --
def local__load_portf_orders(*,db_folder,book,portf):
	return oms_io.load_portf_orders__bk_dord(db_folder=db_folder,strategy=book,portfolio=portf)

# --
# -- copied from bookkeeper_report.py
# --
def local__load_tbsys_portfs(*,db_folder):
	return oms_io.load_tbsys_portfs__bk_rpt(**locals())

def load_all_orders(*,db_folder,dtstr=None):
	portfs = local__load_tbsys_portfs(db_folder=db_folder)
	# --
	all_orders = []
	for nn,rr in portfs.iterrows():
		book,portf = rr[['book','portfolio']].tolist()
		portf_orders = local__load_portf_orders(db_folder=db_folder,book=book,portf=portf)
		if(portf_orders is not None):
			portf_orders['account'] = rr['trade_acct']
			portf_orders = portf_orders.iloc[:,1:]
			all_orders.append(portf_orders)
	all_orders = pd.concat(all_orders,axis=0)
	all_orders = all_orders[[all_orders.columns[-1]]+list(all_orders.columns[:-1])]
	if(dtstr is not None):
		all_orders = all_orders[all_orders['date']==dtstr]
	return all_orders

def export_orders_to_gspread(*,db_folder,dtstr=None):
	workbook = open_workbook("tb2_tradebot")
	all_orders = load_all_orders(db_folder=db_folder,dtstr=dtstr)
	write_orders_page(workbook, all_orders)

# --
# --
# --
def write_blotters_page(workbook, all_blotters, am_pm):
	if(am_pm not in ["AM","PM"]):
		am_pm = "AM"
	# --
	# -- some values, such as <class>, cannot be exported, convert to type str
	# --
	for col in all_blotters.columns:
		all_blotters[col] = all_blotters[col].astype(str)
	# --
	retry(
		lambda : gsu.write(None, None, f"imported_blotters_{am_pm}", "A1:AA1000", all_blotters, write_header=True, clear_range=True, create_sheet=False, workbook=workbook),
		cooldown = 60,
		silent = False,
	)


# --
# -- copied from bookkeeper_report.py
# --
def local__load_account_positions(*,db_folder,account):
	return oms_io.load_account_positions__bk_rpt(**locals())
	
def load_all_blotters(*,db_folder):
	portfs = local__load_tbsys_portfs(db_folder=db_folder)
	# --
	all_blotters = []
	for acct in portfs['trade_acct'].unique():
		acct_blotters = local__load_account_positions(db_folder=db_folder,account=acct)
		if(acct_blotters is not None):
			acct_blotters['account'] = acct
			all_blotters.append(acct_blotters)
	all_blotters = pd.concat(all_blotters,axis=0)
	all_blotters = all_blotters[[all_blotters.columns[-1]]+list(all_blotters.columns[:-1])]
	return all_blotters

def export_blotters_to_gspread(*,db_folder,am_pm="AM"):
	workbook = open_workbook("tb2_tradebot")
	all_blotters = load_all_blotters(db_folder=db_folder)
	write_blotters_page(workbook, all_blotters,am_pm)

# --
# --
# --
def write_symbol_to_market_pricer(*,inPos=None,tradeLst=None,index_n_ETF=None,miscSym=None):
	upd_batch = []
	if(inPos is not None):
		if(type(inPos)==type([])):
			inPos = "#".join(inPos)
		upd_batch.append({ "range" : "C1", "values" : [[ inPos ]] })
	if(tradeLst is not None):
		if(type(tradeLst)==type([])):
			tradeLst = "#".join(tradeLst)
		upd_batch.append({ "range" : "C2", "values" : [[ tradeLst ]] })
	if(index_n_ETF is not None):
		if(type(index_n_ETF)==type([])):
			index_n_ETF = "#".join(index_n_ETF)
		upd_batch.append({ "range" : "C3", "values" : [[ index_n_ETF ]] })
	if(miscSym is not None):
		if(type(miscSym)==type([])):
			miscSym = "#".join(miscSym)
		upd_batch.append({ "range" : "C4", "values" : [[ miscSym ]] })
	if(len(upd_batch)==0):
		return
	# --
	pprint(upd_batch)
	workbook = open_workbook("Market_Pricer_Sink")
	worksheet = workbook.worksheet("symbols")
	retry(
		lambda : worksheet.batch_update(upd_batch),
		cooldown = 60,
		silent = False,
	)

# --
# --
# --
def write_execs_page(workbook, all_execs):
	# --
	# -- some values, such as <class>, cannot be exported, convert to type str
	# --
	for col in all_execs.columns:
		all_execs[col] = all_execs[col].astype(str)
	# --
	retry(
		lambda : gsu.write(None, None, "imported_execs", "A1:AA1000", all_execs, write_header=True, clear_range=True,create_sheet=True, workbook=workbook),
		cooldown = 60,
		silent = False,
	)

# --
# -- copied from bookkeeper_post_process.py
# --
def local__load_account_executions_raw(db_folder,account):
	return oms_io.load_account_executions_raw__bk_pospro(
		db_folder=db_folder, 
		account=account
	)

def safe_load_account_executions(db_folder,account):
	try:
		acct_folder = read_db_path(db_folder=db_folder,account=account)
		acct_execs = local__load_account_executions_raw(acct_folder)
		return acct_execs
	except Exception as ex:
		print(f"WARN/INGORNED:{account}:{ex}")
		return pd.DataFrame(columns='Symbol,Shares,Price,Amount'.split(','))

def load_all_execs(*,db_folder):
	portfs = local__load_tbsys_portfs(db_folder=db_folder)
	# --
	all_execs = []
	for acct in portfs['trade_acct'].unique():
		acct_execs = safe_load_account_executions(db_folder=db_folder,account=acct)
		acct_execs['account'] = acct
		all_execs.append(acct_execs)
	all_execs = pd.concat(all_execs,axis=0)
	all_execs = all_execs[[all_execs.columns[-1]]+list(all_execs.columns[:-1])]
	return all_execs

def export_execs_to_gspread(*,db_folder):
	workbook = open_workbook("tb2_tradebot")
	all_execs = load_all_execs(db_folder=db_folder)
	write_execs_page(workbook, all_execs)

# --
# -- Example usage (assuming `workbook` is a gspread spreadsheet object)
# -- directories = ['path/to/main/directory1', 'path/to/main/directory2']
# -- workbook = client.open_by_key('your_google_sheet_key')
# -- merge_csv_files_to_gsheet(directories, workbook, 'file_name.csv')
# --
def update_maint_sheet(workbook, sheet_name, last_update_time, latest_file_time):
	maint_sheet = gsu.get_or_create_worksheet(workbook,"maint",create_if_missing=True,clear_ws=False)
	existing_data = maint_sheet.get_all_values()
	# --
	# -- Check if the sheet_name already exists in the maintenance sheet
	# --
	for i, row in enumerate(existing_data[1:], start=2):  # Skip header row
		if row[0] == sheet_name:
			existing_data[i-1] = [sheet_name, last_update_time, latest_file_time]
			break
	else:
		existing_data.append([sheet_name, last_update_time, latest_file_time])
	# Update the worksheet with the modified data
	maint_sheet.update('A1', existing_data)

def get_gsheet_last_update_time(workbook,sheet_name):
	maint_sheet = gsu.get_or_create_worksheet(workbook,"maint",create_if_missing=True,clear_ws=False)
	try:
		existing_data = maint_sheet.get_all_values()
		for row in existing_data[1:]:  # Skip header row
			if row[0] == sheet_name:
				return datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
	except Exception as e:
		print(f"Error fetching last update time: {e}")
	return None

# -- rm -- def merge_csv_files_to_gsheet_with_retry(*, directories, workbook, fname, retry_count=10, retry_pause=60):
# -- rm -- 	return retry(
# -- rm -- 		lambda : merge_csv_files_to_gsheet(directories=directories,workbook=workbook,fname=fname),
# -- rm -- 		retry=retry_count, exceptTypes=(BaseException,Exception),cooldown=retry_pause,rtnEx=False,silent=False
# -- rm -- 	)
# -- rm -- 
# -- rm -- def merge_csv_files_to_gsheet(*,directories, workbook, fname):
# -- rm -- 	all_data = []
# -- rm -- 	max_last_mod_time = None
# -- rm -- 	last_update_time_logged = get_gsheet_last_update_time(workbook,fname)
# -- rm -- 	file_df = None
# -- rm -- 	for directory in directories:
# -- rm -- 		for subdir, _, files in os.walk(directory):
# -- rm -- 			for file in files:
# -- rm -- 				if file == fname:
# -- rm -- 					file_path = os.path.join(subdir, file)
# -- rm -- 					# last_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
# -- rm -- 					last_mod_time = os.path.getmtime(file_path)
# -- rm -- 					if(max_last_mod_time is None):
# -- rm -- 						max_last_mod_time = last_mod_time
# -- rm -- 					max_last_mod_time = max(last_mod_time,max_last_mod_time)
# -- rm -- 					subdir_name = os.path.basename(subdir)
# -- rm -- 					dir_name = os.path.basename(directory)
# -- rm -- 					file_df = pd.read_csv(file_path)
# -- rm -- 					file_df.insert(0, 'Subdirectory', subdir_name)
# -- rm -- 					file_df.insert(0, 'Directory', dir_name)
# -- rm -- 					if(len(file_df)>0):
# -- rm -- 						all_data.append(file_df)
# -- rm -- 	# --
# -- rm -- 	# -- check if the data is newer than last export
# -- rm -- 	# --
# -- rm -- 	if(max_last_mod_time):
# -- rm -- 		max_last_mod_time = datetime.fromtimestamp(max_last_mod_time)
# -- rm -- 	updated = False
# -- rm -- 	if(max_last_mod_time and last_update_time_logged):
# -- rm -- 		if max_last_mod_time > last_update_time_logged:
# -- rm -- 			updated = True
# -- rm -- 	elif(max_last_mod_time and not last_update_time_logged):
# -- rm -- 		updated = True
# -- rm -- 	# --
# -- rm -- 	# -- update only when the file is newer
# -- rm -- 	# --
# -- rm -- 	print(f"fname:{fname}; updated:{updated} -- max_last_mod_time:{max_last_mod_time}; last_update_time_logged:{last_update_time_logged}")
# -- rm -- 	if(updated):
# -- rm -- 		# --
# -- rm -- 		combined_df = file_df
# -- rm -- 		if(len(all_data)>0):
# -- rm -- 			combined_df = pd.concat(all_data, ignore_index=True)
# -- rm -- 		combined_df = convert_columns_to_string(combined_df)
# -- rm -- 		sheet = gsu.get_or_create_worksheet(workbook,fname,create_if_missing=True,clear_ws=True)
# -- rm -- 		write_values=[combined_df.columns.values.tolist()] + combined_df.values.tolist()
# -- rm -- 		range_LR = gsu.to_a1( np.asarray(write_values).shape )
# -- rm -- 		sheet.update(range_name=f"A1:{range_LR}",values=write_values,value_input_option="USER_ENTERED")
# -- rm -- 		# --
# -- rm -- 		# -- Update the "maint" worksheet
# -- rm -- 		# --
# -- rm -- 		last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# -- rm -- 		update_maint_sheet(workbook, fname, last_update_time, max_last_mod_time.isoformat())
# -- rm -- 		gsu.move_worksheet_to_second_position(workbook, fname)

# --
# -- merge multiple csv files (with same columns spec, enforced)
# -- into a single csv files with the same columns
# -- over write the old single csv if any of the source files is newer
# --
# -- rm -- def merge_csv_files(*, directories, fname, destination):
# -- rm -- 	all_data = []
# -- rm -- 	max_last_mod_time = None
# -- rm -- 	destination_fname = f'{destination}/{fname}'
# -- rm -- 	# --
# -- rm -- 	last_update_time_logged = datetime.fromtimestamp(1)
# -- rm -- 	if(os.path.exists(destination_fname)):
# -- rm -- 		last_update_time_logged = datetime.fromtimestamp(os.path.getmtime(destination_fname))
# -- rm -- 	# --
# -- rm -- 	file_df = None
# -- rm -- 	for directory in directories:
# -- rm -- 		for subdir, _, files in os.walk(directory):
# -- rm -- 			for file in files:
# -- rm -- 				if file == fname:
# -- rm -- 					file_path = os.path.join(subdir, file)
# -- rm -- 					last_mod_time = os.path.getmtime(file_path)
# -- rm -- 					if(max_last_mod_time is None):
# -- rm -- 						max_last_mod_time = last_mod_time
# -- rm -- 					max_last_mod_time = max(last_mod_time,max_last_mod_time)
# -- rm -- 					subdir_name = os.path.basename(subdir)
# -- rm -- 					dir_name = os.path.basename(directory)
# -- rm -- 					file_df = pd.read_csv(file_path)
# -- rm -- 					file_df.insert(0, 'Subdirectory', subdir_name)
# -- rm -- 					file_df.insert(0, 'Directory', dir_name)
# -- rm -- 					if(len(file_df)>0):
# -- rm -- 						all_data.append(file_df)
# -- rm -- 	# --
# -- rm -- 	# -- check if the data is newer than last export
# -- rm -- 	# --
# -- rm -- 	if(max_last_mod_time):
# -- rm -- 		max_last_mod_time = datetime.fromtimestamp(max_last_mod_time)
# -- rm -- 	updated = False
# -- rm -- 	if(max_last_mod_time and last_update_time_logged):
# -- rm -- 		if(max_last_mod_time > last_update_time_logged):
# -- rm -- 			updated = True
# -- rm -- 	elif(max_last_mod_time and not last_update_time_logged):
# -- rm -- 		updated = True
# -- rm -- 	# --
# -- rm -- 	# -- update only when the file is newer
# -- rm -- 	# --
# -- rm -- 	print(f"fname:{destination_fname}; updated:{updated} -- max_last_mod_time:{max_last_mod_time}; last_update_time_logged:{last_update_time_logged}")
# -- rm -- 	if(updated):
# -- rm -- 		# --
# -- rm -- 		combined_df = file_df
# -- rm -- 		if(len(all_data)>0):
# -- rm -- 			combined_df = pd.concat(all_data, ignore_index=True)
# -- rm -- 		combined_df = convert_columns_to_string(combined_df)
# -- rm -- 		combined_df.to_csv(destination_fname,index=False,float_format="%0.0f")
# -- rm -- 		print(combined_df)
# -- rm -- 		
# -- rm -- # -- 		sheet = gsu.get_or_create_worksheet(workbook,fname,create_if_missing=True,clear_ws=True)
# -- rm -- # -- 		write_values=[combined_df.columns.values.tolist()] + combined_df.values.tolist()
# -- rm -- # -- 		range_LR = gsu.to_a1( np.asarray(write_values).shape )
# -- rm -- # -- 		sheet.update(range_name=f"A1:{range_LR}",values=write_values,value_input_option="USER_ENTERED")
# -- rm -- # -- 		# --
# -- rm -- # -- 		# -- Update the "maint" worksheet
# -- rm -- # -- 		# --
# -- rm -- # -- 		last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# -- rm -- # -- 		update_maint_sheet(workbook, fname, last_update_time, max_last_mod_time.isoformat())
# -- rm -- # -- 		gsu.move_worksheet_to_second_position(workbook, fname)

def merge_csv_files_as_df(*, directories, fname):
	all_data = []
	max_last_mod_time = None
	# --
	file_df = None
	for directory in directories:
		for subdir, _, files in os.walk(directory):
			for file in files:
				if file == fname:
					file_path = os.path.join(subdir, file)
					last_mod_time = os.path.getmtime(file_path)
					if(max_last_mod_time is None):
						max_last_mod_time = last_mod_time
					max_last_mod_time = max(last_mod_time,max_last_mod_time)
					subdir_name = os.path.basename(subdir)
					dir_name = os.path.basename(directory)
					file_df = pd.read_csv(file_path)
					file_df.insert(0, 'Subdirectory', subdir_name)
					file_df.insert(0, 'Directory', dir_name)
					if(len(file_df)>0):
						all_data.append(file_df)
	# --
	# -- check if the data is newer than last export
	# --
	if(max_last_mod_time):
		max_last_mod_time = datetime.fromtimestamp(max_last_mod_time)
	combined_df = file_df
	if(len(all_data)>0):
		combined_df = pd.concat(all_data, ignore_index=True)
		combined_df = convert_columns_to_string(combined_df)

	# print(f"fname:{fname}; max_last_mod_time:{max_last_mod_time};")
	return { "fname" : fname, "df" : combined_df, "max_last_mod_time" : max_last_mod_time }

# -- ----------------------------------------------------
# -- with_chk -------------------------------------------
# -- ----------------------------------------------------
def merged_csv_files_save_db(*, destination, merge_res=None):
	max_last_mod_time = merge_res['max_last_mod_time']
	combined_df = merge_res['df']
	fname = merge_res['fname']
	destination_fname = f'{destination}/{fname}'
	# --
	last_update_time_logged = datetime.fromtimestamp(1)
	if(os.path.exists(destination_fname)):
		last_update_time_logged = datetime.fromtimestamp(os.path.getmtime(destination_fname))
	# --
	# -- check if the data is newer than last export
	# --
	updated = False
	if(max_last_mod_time and last_update_time_logged):
		if(max_last_mod_time > last_update_time_logged):
			updated = True
	elif(max_last_mod_time and not last_update_time_logged):
		updated = True
	# --
	# -- update only when the file is newer
	# --
	if(updated):
		combined_df.to_csv(destination_fname,index=False,float_format="%0.0f")
	return { "dest" : destination_fname, "file_updated" : updated, "file_last_update_time" : last_update_time_logged }

def __merged_csv_files_save_gspread_impl__(*, workbook, merge_res=None):
	max_last_mod_time = merge_res['max_last_mod_time']
	combined_df = merge_res['df']
	fname = merge_res['fname']
	last_update_time_logged = get_gsheet_last_update_time(workbook,fname)
	# --
	# -- check if the data is newer than last export
	# --
	updated = False
	if(max_last_mod_time and last_update_time_logged):
		if max_last_mod_time > last_update_time_logged:
			updated = True
	elif(max_last_mod_time and not last_update_time_logged):
		updated = True
	# --
	# -- update only when the file is newer
	# --
	if(updated):
		sheet = gsu.get_or_create_worksheet(workbook,fname,create_if_missing=True,clear_ws=True)
		write_values=[combined_df.columns.values.tolist()] + combined_df.values.tolist()
		range_LR = gsu.to_a1( np.asarray(write_values).shape )
		sheet.update(range_name=f"A1:{range_LR}",values=write_values,value_input_option="USER_ENTERED")
		# --
		# -- Update the "maint" worksheet
		# --
		last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		update_maint_sheet(workbook, fname, last_update_time, max_last_mod_time.isoformat())
		gsu.move_worksheet_to_second_position(workbook, fname)
	# --
	return { "gspread_updated" : updated, "file_last_update_time" : last_update_time_logged }

def merged_csv_files_save_gspread(*, workbook=None, merge_res=None):
	return retry(
		lambda : __merged_csv_files_save_gspread_impl__(workbook=workbook,merge_res=merge_res),
		retry=5, exceptTypes=(BaseException,Exception),cooldown=90,rtnEx=False,silent=False
	)

def merge_csv_files_save(*, directories, fname, workbook=None, outdir=None, return_result=False, silent=False):
	result = { 'merge_res':None, 'export_res':None, 'save_db_res':None }
	# --
	result['merge_res'] = merge_csv_files_as_df(directories=directories, fname=fname)
	merge_res = result['merge_res']
	if(workbook is not None):
		result['export_res'] = merged_csv_files_save_gspread(workbook=workbook, merge_res=merge_res)
	if(outdir is not None):
		result['save_db_res'] = merged_csv_files_save_db(destination=outdir, merge_res=merge_res)
	if(not silent):
		print("merge :", result['merge_res']['fname'], result['merge_res']['max_last_mod_time'])
		print("export :", result['export_res'])
		print("save_db :", result['save_db_res'])
	if(return_result):
		return result

# -- ----------------------------------------------------
# -- no_chk ---------------------------------------------
# -- ----------------------------------------------------
def merged_csv_files_save_db_no_chk(*, destination, merge_res=None):
	combined_df = merge_res['df']
	fname = merge_res['fname']
	destination_fname = f'{destination}/{fname}'
	# --
	last_update_time_logged = datetime.fromtimestamp(1)
	if(os.path.exists(destination_fname)):
		last_update_time_logged = datetime.fromtimestamp(os.path.getmtime(destination_fname))
	# --
	# -- check if the data is newer than last export
	# --
	updated = True
	# --
	# -- update only when the file is newer
	# --
	if(updated):
		combined_df.to_csv(destination_fname,index=False,float_format="%0.0f")
	return { "dest" : destination_fname, "file_updated" : updated, "file_last_update_time" : last_update_time_logged }

def __merged_csv_files_save_gspread_impl___no_chk(*, workbook, merge_res=None):
	combined_df = merge_res['df']
	fname = merge_res['fname']
	last_update_time_logged = get_gsheet_last_update_time(workbook,fname)
	# --
	# -- check if the data is newer than last export
	# --
	updated = True
	# --
	# -- update only when the file is newer
	# --
	if(updated):
		sheet = gsu.get_or_create_worksheet(workbook,fname,create_if_missing=True,clear_ws=True)
		write_values=[combined_df.columns.values.tolist()] + combined_df.values.tolist()
		range_LR = gsu.to_a1( np.asarray(write_values).shape )
		sheet.update(range_name=f"A1:{range_LR}",values=write_values,value_input_option="USER_ENTERED")
		# --
		# -- Update the "maint" worksheet
		# --
		last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		gsu.move_worksheet_to_second_position(workbook, fname)
	# --
	return { "gspread_updated" : updated, "file_last_update_time" : last_update_time_logged }

def merged_csv_files_save_gspread_no_chk(*, workbook=None, merge_res=None):
#	return __merged_csv_files_save_gspread_impl___no_chk(workbook=workbook,merge_res=merge_res)
	return retry(
		lambda : __merged_csv_files_save_gspread_impl___no_chk(workbook=workbook,merge_res=merge_res),
		retry=5, exceptTypes=(BaseException,Exception),cooldown=90,rtnEx=False,silent=False
	)

def merge_csv_files_save_no_chk(*, directories, fname, workbook=None, outdir=None, return_result=False, silent=False):
	result = { 'merge_res':None, 'export_res':None, 'save_db_res':None }
	# --
	result['merge_res'] = merge_csv_files_as_df(directories=directories, fname=fname)
	merge_res = result['merge_res']
	if(workbook is not None):
		result['export_res'] = merged_csv_files_save_gspread_no_chk(workbook=workbook, merge_res=merge_res)
	if(outdir is not None):
		result['save_db_res'] = merged_csv_files_save_db_no_chk(destination=outdir, merge_res=merge_res)
	if(not silent):
		print("merge :", result['merge_res']['fname'], result['merge_res']['max_last_mod_time'])
		print("export :", result['export_res'])
		print("save_db :", result['save_db_res'])
	if(return_result):
		return result
