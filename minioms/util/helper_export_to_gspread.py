import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF8')
# --
import pandas as pd
import numpy as np
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
from jackutil.microfunc import retry
from jackutil import containerutil as cutil
# --
from pprint import pprint
import gspread_util as gsu
from ..obj.PairedTxns import io_utility as pairedtxns_u_io
from ..obj.PortfPositions import io_utility as portfpos_u_io
from ..obj.PortfDividendTxns import io_utility as portfdtxns_u_io
from ..obj.PortfDailyOrders import io_utility as portfdord_u_io
from ..obj.Portfolios import io_utility as portfs_u_io
from ..obj.AcctPositions import io_utility as acctpos_u_io
import os 
from datetime import datetime

# --
# -- ref internally
# --
def open_workbook(svc_cred_fname,wb_name):
	return gsu.authenticate_and_open_tradebook(svc_cred_fname, wb_name)

# --
# -- ref externally
# --
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
	txns = pairedtxns_u_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.copy()
	if(drop_cash_txn):
		txns = txns[ (txns['type']=='BUY') + (txns['type']=='SEL') ]
	txns = txns.iloc[:,1:].reset_index(drop=True)
	if(details_only):
		return txns 
	balance = txns['cost'].sum()
	return txns,balance


# --
# -- strategy = books.xml:/*/wb_name
# -- book_name = books.xml:/*/sh_name
# --
def load_open_positions(*,db_folder,strategy,book_name):
	openpos = portfpos_u_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name)
	return openpos.df.reset_index(drop=True)

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
	# -- not sure what the 'line#' column for, remove it for now
	# !! might need to fix the source
	div_txn = portfdtxns_u_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name)
	txns = div_txn.df.reset_index(drop=True)
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
	aggregate['avg_prc'] = aggregate['cost'] / aggregate['unit']
	aggregate['current'] = '=lookup("'+aggregate['symbol']+'",pricer!$A$2:$A$600,pricer!$H$2:$H$600)'
	return aggregate

def aggregate_all_openpos_by_idx_sym(all_openpos):
	aggregate = all_openpos.groupby(by=['strategy','symbol']).agg({'unit':'sum','cost':'sum',})
	aggregate = aggregate.reset_index(drop=False)
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
def export_books_to_gspread(*,db_folder,books,svc_cred_fname):
	export_wb_name = books.portfolios[0]['wb_name2']
	workbook = open_workbook(svc_cred_fname,export_wb_name)
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
	return portfdord_u_io.load(db_dir=db_folder,strategy=book,portfolio=portf).df.copy()

# --
# -- copied from bookkeeper_report.py
# --
def local__load_tbsys_portfs(*,db_folder):
	return portfs_u_io.load(db_dir=db_folder).df.copy()

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

def export_orders_to_gspread(*,db_folder,dtstr=None,svc_cred_fname=None):
	workbook = open_workbook(svc_cred_fname,"tb2_tradebot")
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
	return acctpos_u_io.load(db_dir=db_folder,account=account).df.copy()
	
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

def export_blotters_to_gspread(*,db_folder,am_pm="AM",svc_cred_fname=None):
	workbook = open_workbook(svc_cred_fname,"tb2_tradebot")
	all_blotters = load_all_blotters(db_folder=db_folder)
	write_blotters_page(workbook, all_blotters,am_pm)

# --
# --
# --
def write_symbol_to_market_pricer(*,inPos=None,tradeLst=None,index_n_ETF=None,miscSym=None,svc_cred_fname=None):
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
	workbook = open_workbook(svc_cred_fname,"Market_Pricer_Sink")
	worksheet = workbook.worksheet("symbols")
	retry(
		lambda : worksheet.batch_update(upd_batch),
		cooldown = 60,
		silent = False,
	)



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


# --
# -- merge multiple csv files (with same columns spec, enforced)
# -- into a single csv files with the same columns
# -- over write the old single csv if any of the source files is newer
# --
def convert_columns_to_string(df):
	for col in df.columns:
		if not pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_datetime64_any_dtype(df[col]):
			df[col] = df[col].astype(str).replace({'nan': '', 'NaN': ''})
		else:
			df[col] = df[col].fillna('').replace({'nan': '', 'NaN': ''})
	return df

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
					# -- (HUM) REVIEWED;pending_rm -- 				# (CLU) NEED_REVIEW: pd.read_csv called directly, bypassing oms_db abstraction.
					# -- (HUM) REVIEWED;pending_rm -- 				# (CLU) NEED_REVIEW: Fix: use oms_db.datafile.DataFile (or a thin subclass) to
					# -- (HUM) REVIEWED;pending_rm -- 				# (CLU) NEED_REVIEW: wrap the read, passing full_path=file_path. This keeps all
					# -- (HUM) REVIEWED;pending_rm -- 				# (CLU) NEED_REVIEW: CSV I/O inside the oms_db layer.
					# (HUM) in this case, I am export the files as csv (concate them and write to 
					# (HUM) google spreadsheet) In a way, I am not treating them as data, but raw
					# (HUM) files. Let me know if this is a strong enough reason to by pass the 
					# (HUM) object I/O interface.
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

# -- (HUM) REVIEWED;pending_rm -- # -- ----------------------------------------------------
# -- (HUM) REVIEWED;pending_rm -- # -- with_chk -------------------------------------------
# -- (HUM) REVIEWED;pending_rm -- # -- ----------------------------------------------------
# -- (HUM) REVIEWED;pending_rm -- # (HUM) *** NO *** external ref
# -- (HUM) REVIEWED;pending_rm -- # (HUM) local ref
# -- (HUM) REVIEWED;pending_rm -- def merged_csv_files_save_db(*, destination, merge_res=None):
# -- (HUM) REVIEWED;pending_rm -- 	max_last_mod_time = merge_res['max_last_mod_time']
# -- (HUM) REVIEWED;pending_rm -- 	combined_df = merge_res['df']
# -- (HUM) REVIEWED;pending_rm -- 	fname = merge_res['fname']
# -- (HUM) REVIEWED;pending_rm -- 	destination_fname = f'{destination}/{fname}'
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	last_update_time_logged = datetime.fromtimestamp(1)
# -- (HUM) REVIEWED;pending_rm -- 	if(os.path.exists(destination_fname)):
# -- (HUM) REVIEWED;pending_rm -- 		last_update_time_logged = datetime.fromtimestamp(os.path.getmtime(destination_fname))
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	# -- check if the data is newer than last export
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	updated = False
# -- (HUM) REVIEWED;pending_rm -- 	if(max_last_mod_time and last_update_time_logged):
# -- (HUM) REVIEWED;pending_rm -- 		if(max_last_mod_time > last_update_time_logged):
# -- (HUM) REVIEWED;pending_rm -- 			updated = True
# -- (HUM) REVIEWED;pending_rm -- 	elif(max_last_mod_time and not last_update_time_logged):
# -- (HUM) REVIEWED;pending_rm -- 		updated = True
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	# -- update only when the file is newer
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	if(updated):
# -- (HUM) REVIEWED;pending_rm -- 		# (CLU) NEED_REVIEW: to_csv called directly, bypassing oms_db abstraction.
# -- (HUM) REVIEWED;pending_rm -- 		# (CLU) NEED_REVIEW: Fix: use oms_db.datafile.DataFile (or a thin subclass) to
# -- (HUM) REVIEWED;pending_rm -- 		# (CLU) NEED_REVIEW: wrap the write, passing full_path=destination_fname and df0=combined_df.
# -- (HUM) REVIEWED;pending_rm -- 		# (CLU) NEED_REVIEW: This keeps all CSV I/O inside the oms_db layer.
# -- (HUM) REVIEWED;pending_rm -- 		combined_df.to_csv(destination_fname,index=False,float_format="%0.0f")
# -- (HUM) REVIEWED;pending_rm -- 	return { "dest" : destination_fname, "file_updated" : updated, "file_last_update_time" : last_update_time_logged }

# -- (HUM) REVIEWED;pending_rm -- # (HUM) *** NO *** external ref
# -- (HUM) REVIEWED;pending_rm -- # (HUM) local ref
# -- (HUM) REVIEWED;pending_rm -- def __merged_csv_files_save_gspread_impl__(*, workbook, merge_res=None):
# -- (HUM) REVIEWED;pending_rm -- 	max_last_mod_time = merge_res['max_last_mod_time']
# -- (HUM) REVIEWED;pending_rm -- 	combined_df = merge_res['df']
# -- (HUM) REVIEWED;pending_rm -- 	fname = merge_res['fname']
# -- (HUM) REVIEWED;pending_rm -- 	last_update_time_logged = get_gsheet_last_update_time(workbook,fname)
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	# -- check if the data is newer than last export
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	updated = False
# -- (HUM) REVIEWED;pending_rm -- 	if(max_last_mod_time and last_update_time_logged):
# -- (HUM) REVIEWED;pending_rm -- 		if max_last_mod_time > last_update_time_logged:
# -- (HUM) REVIEWED;pending_rm -- 			updated = True
# -- (HUM) REVIEWED;pending_rm -- 	elif(max_last_mod_time and not last_update_time_logged):
# -- (HUM) REVIEWED;pending_rm -- 		updated = True
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	# -- update only when the file is newer
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	if(updated):
# -- (HUM) REVIEWED;pending_rm -- 		sheet = gsu.get_or_create_worksheet(workbook,fname,create_if_missing=True,clear_ws=True)
# -- (HUM) REVIEWED;pending_rm -- 		write_values=[combined_df.columns.values.tolist()] + combined_df.values.tolist()
# -- (HUM) REVIEWED;pending_rm -- 		range_LR = gsu.to_a1( np.asarray(write_values).shape )
# -- (HUM) REVIEWED;pending_rm -- 		sheet.update(range_name=f"A1:{range_LR}",values=write_values,value_input_option="USER_ENTERED")
# -- (HUM) REVIEWED;pending_rm -- 		# --
# -- (HUM) REVIEWED;pending_rm -- 		# -- Update the "maint" worksheet
# -- (HUM) REVIEWED;pending_rm -- 		# --
# -- (HUM) REVIEWED;pending_rm -- 		last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# -- (HUM) REVIEWED;pending_rm -- 		update_maint_sheet(workbook, fname, last_update_time, max_last_mod_time.isoformat())
# -- (HUM) REVIEWED;pending_rm -- 		gsu.move_worksheet_to_second_position(workbook, fname)
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	return { "gspread_updated" : updated, "file_last_update_time" : last_update_time_logged }

# -- (HUM) REVIEWED;pending_rm -- # (HUM) ***NO*** external ref
# -- (HUM) REVIEWED;pending_rm -- # (HUM) local ref
# -- (HUM) REVIEWED;pending_rm -- def merged_csv_files_save_gspread(*, workbook=None, merge_res=None):
# -- (HUM) REVIEWED;pending_rm -- 	return retry(
# -- (HUM) REVIEWED;pending_rm -- 		lambda : __merged_csv_files_save_gspread_impl__(workbook=workbook,merge_res=merge_res),
# -- (HUM) REVIEWED;pending_rm -- 		retry=5, exceptTypes=(BaseException,Exception),cooldown=90,rtnEx=False,silent=False
# -- (HUM) REVIEWED;pending_rm -- 	)

# -- (HUM) REVIEWED;pending_rm -- # (HUM) ***NO*** external ref
# -- (HUM) REVIEWED;pending_rm -- # (HUM) ***NO*** local ref
# -- (HUM) REVIEWED;pending_rm -- def merge_csv_files_save(*, directories, fname, workbook=None, outdir=None, return_result=False, silent=False):
# -- (HUM) REVIEWED;pending_rm -- 	result = { 'merge_res':None, 'export_res':None, 'save_db_res':None }
# -- (HUM) REVIEWED;pending_rm -- 	# --
# -- (HUM) REVIEWED;pending_rm -- 	result['merge_res'] = merge_csv_files_as_df(directories=directories, fname=fname)
# -- (HUM) REVIEWED;pending_rm -- 	merge_res = result['merge_res']
# -- (HUM) REVIEWED;pending_rm -- 	if(workbook is not None):
# -- (HUM) REVIEWED;pending_rm -- 		result['export_res'] = merged_csv_files_save_gspread(workbook=workbook, merge_res=merge_res)
# -- (HUM) REVIEWED;pending_rm -- 	if(outdir is not None):
# -- (HUM) REVIEWED;pending_rm -- 		result['save_db_res'] = merged_csv_files_save_db(destination=outdir, merge_res=merge_res)
# -- (HUM) REVIEWED;pending_rm -- 	if(not silent):
# -- (HUM) REVIEWED;pending_rm -- 		print("merge :", result['merge_res']['fname'], result['merge_res']['max_last_mod_time'])
# -- (HUM) REVIEWED;pending_rm -- 		print("export :", result['export_res'])
# -- (HUM) REVIEWED;pending_rm -- 		print("save_db :", result['save_db_res'])
# -- (HUM) REVIEWED;pending_rm -- 	if(return_result):
# -- (HUM) REVIEWED;pending_rm -- 		return result

# -- ----------------------------------------------------
# -- no_chk ---------------------------------------------
# -- ----------------------------------------------------
# (HUM) TODO: if timestamp check is added back in the future:
# (HUM) TODO:   - rely on local OS filesystem mtime as the source of truth (not gsheet maint tab)
# (HUM) TODO:   - write to file first; gsheet update is secondary and only triggered if file write happened
# (HUM) TODO:   - gsheet timestamp should reflect file save time, not gsheet update time
# (HUM) TODO:   - this avoids maint-tab drift and keeps a single consistent timeline
def merged_csv_files_save_db_no_chk(*, destination, merge_res=None):
	# --
	# -- destination must contain _export_
	# --
	if('_export_' not in destination):
		raise ValueError(f"only allow to write to _export_: destination={destination}")
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
# -- (HUM) REVIEWED;pending_rm -- 		# (CLU) NEED_REVIEW: to_csv called directly, bypassing oms_db abstraction.
# -- (HUM) REVIEWED;pending_rm -- 		# (CLU) NEED_REVIEW: Fix: same as merged_csv_files_save_db above — use DataFile to wrap
# -- (HUM) REVIEWED;pending_rm -- 		# (CLU) NEED_REVIEW: the write. Both functions share the same pattern and could be unified.
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
	return retry(
		lambda : __merged_csv_files_save_gspread_impl___no_chk(workbook=workbook,merge_res=merge_res),
		retry=5, exceptTypes=(BaseException,Exception),cooldown=90,rtnEx=False,silent=False
	)

# -- 
# -- is_API: TRUE
# -- 
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
