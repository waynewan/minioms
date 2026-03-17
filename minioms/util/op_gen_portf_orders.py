from ..obj.ExitConds import io_utility as exitconds_io
from ..obj.Buylist import io_utility as buylist_io
from ..obj.PortfPositions import io_utility as portfpos_io
from ..obj.PortfDividendTxns import io_utility as portfdtxns_io
from ..obj.PairedTxns import io_utility as pairedtxns_io
from ..obj.PortfSetting import io_utility as portfset_io
from ..obj.PortfSetting import br_utility as portfset_br
from ..obj.PortfDailyOrders import io_utility as portfdord_u_io
from ..oms_db.classes_io import PortfDailyOrders_IO
from jackutil.microfunc import types_validate
from jackutil.microfunc import dt_to_str
from .external_interface import load_market_price
import datetime
import pandas as pd
import numpy as np
from pprint import pprint

class op_gen_portf_orders:
	# (HUM)
	# (HUM) TODO: keep this, and investigate
	# (HUM) TODO: There must be an explaination for it to be here
	# (HUM) TODO: ?? Maybe part of an incompleted task ??
	# (HUM)
	def load_required_objects(*,db_dir,strategy,portfolio):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(strategy,msg="strategy",types=[ type("") ],allow_none=False)
		types_validate(portfolio,msg="portfolio",types=[ type("") ],allow_none=False)
		# --
		portf_settings = portfset_io.load(db_dir=db_dir,strategy=strategy,portfolio=portfolio)
		open_pos = portfpos_io.load(db_dir=db_dir,strategy=strategy,portfolio=portfolio)
		exitconds = exitconds_io.load(db_dir=db_dir,strategy=strategy,portfolio=portfolio)
		buylist = buylist_io.load(db_dir=db_dir,strategy=strategy,portfolio=portfolio)
		dtxns = portfdtxns_io.load(db_dir=db_dir,strategy=strategy,portfolio=portfolio)
		# --
		return portf_settings,open_pos,exitconds,buylist,dtxns

	def gen_book_orders(db_dir,book,version=None):
		orders = generate_orders_for_book(db_folder=db_dir,book=book,version=version)
		return orders

# -- ----------------------------------------------------------------------------
# -- code copy from bookkeeper_daily_orders.py (no longer exist)
# -- ----------------------------------------------------------------------------
def get_portf_attr(portf):
	portf_attr = portf.get('portf_attr',[])
	if(type(portf_attr)==type([])):
		return portf_attr
	return portf_attr.split(",")

def generate_orders_for_book(*,db_folder,book,version=None):
	if(version is not None):
		check_version(book.version, version)
	# --
	# -- pre-load all portfolio data once to avoid duplicate I/O with pre_fetch_market_price
	# --
	all_portf_data = {}
	for portf in book.portfolios:
		wb_name = portf['wb_name']
		sh_name = portf['sh_name']
		all_portf_data[(wb_name,sh_name)] = load_portf_data(db_folder=db_folder,strategy=wb_name,book_name=sh_name)
	pre_fetch_market_price(all_portf_data=all_portf_data)
	# --
	portf_orders = {}
	for portf in book.portfolios:
		wb_name = portf['wb_name']
		sh_name = portf['sh_name']
		name = portf['name']
		portf_attr = get_portf_attr(portf)
		# --
		portf_orders[name] = generate_orders_for_portf(db_folder=db_folder,strategy=wb_name,book_name=sh_name,portf_attr=portf_attr,d_portf_data=all_portf_data[(wb_name,sh_name)])
		print(f"{wb_name}/{sh_name} orders generated")
	return portf_orders

def generate_orders_for_portf(*,db_folder,strategy,book_name,portf_attr,d_portf_data=None):
	if(d_portf_data is None):
		d_portf_data = load_portf_data(db_folder=db_folder,strategy=strategy,book_name=book_name)
	portf_settings = d_portf_data['portf_settings']
	orders = build_orders_table(
		portf_attr=portf_attr,
		portf_basic_info=portfset_br.basic_info_from(portf_setting=portf_settings,optional={"benchmark":portfset_br.get_def_benchmark(book=strategy)}),
		portf_summary=portf_financial_summary(db_folder=db_folder,strategy=strategy,book_name=book_name),
		**d_portf_data
	)
	# --
	daily_orders = orders["all_orders"]
	# --
	PortfDailyOrders_IO(db_dir=db_folder,strategy=strategy,portfolio=book_name,df0=daily_orders).write()
	return orders

def load_portf_data(*,db_folder,strategy,book_name):
	return {
		"portf_settings" : portfset_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name),
		"openpos" : load_openpos(db_folder=db_folder,strategy=strategy,book_name=book_name),
		"exitcond" : load_exitcond(db_folder=db_folder,strategy=strategy,book_name=book_name),
		"buylist" : load_buylist(db_folder=db_folder,strategy=strategy,book_name=book_name),
		"dividend_txn" : load_dividend(db_folder=db_folder,strategy=strategy,book_name=book_name),
	}

def check_version(book_version,version):
	if(version>book_version):
		raise Exception(f"book version is {book_version}; require version is {version} or above")
	print(f"book version is {book_version}; require version is {version} or above")

def pre_fetch_market_price(*,all_portf_data):
	symbols_list = set()
	for (wb_name,sh_name),portf_data in all_portf_data.items():
		symbols_list.update(portf_data['openpos']['symbol'].tolist())
		symbols_list.update(portf_data['exitcond']['symbol'].tolist())
		symbols_list.update(portf_data['buylist']['symbol'].tolist())
	output = load_market_price(pd.DataFrame(data=symbols_list,columns=["symbol"]))
	pprint(output)

def build_orders_table(*,portf_attr,portf_basic_info,portf_summary,exitcond,buylist,**kargs):
	exitorders = load_market_price(exitcond)
	exitorders = exitorders.loc[:,['symbol','price','unit','pkey']]
	exitorders['unit'] = -1 * exitorders['unit'].astype(int)
	exitorders['action'] = 'SEL'
	exitorders['linked_buy_pkey'] = exitorders['pkey']
	exitorders['pkey'] = ""
	# --
	n_exit_order = exitorders.shape[0]
	maxpos = portf_basic_info['maxpos']
	n_open_pos = portf_summary['#openpos']
	ttl_div = portf_summary['dividend_val']
	ttl_cost = portf_summary['total_cost']
	exitorders_mkt_val = -(exitorders['unit']*exitorders['price']).sum()
	# -- 
	print(" # 	exitorders_mkt_val", exitorders_mkt_val)
	# --
	# -- for "cash_for_trade" calc
	# !! assumption: ttl_cost (from paired_txn) include the 
	# !!             principle (all DEPOSIT/WITHDRAW entry)
	# ?? Should the cash txn put in a separate table ??
	# --
	cash_for_trade = ttl_cost + ttl_div + exitorders_mkt_val
	n_empty_slot = maxpos - n_open_pos + n_exit_order
	cash_per_slot = 0
	if(n_empty_slot>0):
		cash_per_slot = cash_for_trade / n_empty_slot
	if("no_buy" in portf_attr):
		maxpos = 0
		n_empty_slot = 0
		cash_for_trade = 0
		cash_per_slot = 0
	print(" # 	portf_attr", portf_attr)
	print(" # 	maxpos", maxpos)
	print(" # 	ttl_mkt_val", portf_summary['market_value'] )
	print(" # 	cash_for_trade", cash_for_trade)
	print(" # 	n_empty_slot", n_empty_slot)
	print(" # 	cash_per_slot", cash_per_slot)
	# --
	buyorders = load_market_price(buylist)
	buyorders = buyorders[:n_empty_slot]
	buyorders['unit'] = np.round(cash_per_slot / buyorders['price'], 0)
	buyorders['action'] = 'BUY'
	buyorders['linked_buy_pkey'] = '--'
	# --
	daily_orders = pd.concat([buyorders,exitorders]).reset_index(drop=True)
	daily_orders['portfolio'] = portf_basic_info['portf']
	daily_orders['book'] = portf_basic_info['book']
	daily_orders = daily_orders[daily_orders['unit']!=0]
	daily_orders['date'] = dt_to_str(datetime.datetime.today(),delimiter="/")
	daily_orders['unit'] = daily_orders['unit'].astype(int)
	if(daily_orders.shape[0]>0):
		fd_pkey = daily_orders[["date","action","unit","symbol"]]
		# --
		# !! (HUM) this might trigger SettingWithCopyWarning, but refactor/rewrite will fix it eventually
		# !! (HUM) take no action for now
		# --
		fd_pkey.loc[:,'unit'] = np.abs(fd_pkey['unit'])
		daily_orders.loc[:,'pkey'] = fd_pkey[["date","symbol","action","unit"]].astype(str).agg("|".join,axis=1)
	daily_orders = daily_orders["book,portfolio,date,symbol,action,unit,price,linked_buy_pkey,pkey".split(",")]
	# --
	instructions = daily_orders.loc[:,['symbol','unit']].to_csv(index=False,header=None,float_format="%0.0f")
	# --
	return {
		"neworders" : buyorders,
		"closeorders" : exitorders,
		"all_orders" : daily_orders,
		"instructions" : instructions,
	}


def portf_financial_summary(*,db_folder,strategy,book_name):
	# --
	paired_txn = load_paired_txn(db_folder=db_folder, strategy=strategy,book_name=book_name)
	openpos = load_openpos(db_folder=db_folder, strategy=strategy,book_name=book_name)
	dividend_txn = load_dividend(db_folder=db_folder, strategy=strategy,book_name=book_name)
	if('price' not in openpos.columns):
		# !!
		# !! load_market_price change pasted in obj
		# !!
		openpos = load_market_price(pd.DataFrame(openpos))
	# --
	total_cost = paired_txn['cost'].sum()
	market_val = ( openpos['unit'] * openpos['price'] ).sum()
	n_openpos = openpos.shape[0]
	# --
	dividend_val = dividend_txn['amount'].sum()
	# --
	return {
		"total_cost" : total_cost,
		"dividend_val" : dividend_val,
		"market_value" : market_val,
		"#openpos" : n_openpos,
	}

def load_openpos(*,db_folder,strategy,book_name):
	return portfpos_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.copy()

def load_exitcond(*,db_folder,strategy,book_name,trig_only=True):
	exitcond = exitconds_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.copy()
	exitcond['unit'] = exitcond['unit'].astype(int)
	if(trig_only):
		if('exit_cond' in exitcond.columns):
			exitcond = exitcond[ exitcond['exit_cond']==True ]
		elif('exit_trigger' in exitcond.columns):
			exitcond = exitcond[ exitcond['exit_trigger'].str.len()>0 ]
			exitcond = exitcond[ exitcond['exit_trigger'] !="--" ]
		else:
			raise ValueError("Do not know how to filter exitcond")
	return exitcond

def load_buylist(*,db_folder,strategy,book_name):
	return buylist_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.copy()


def load_dividend(*,db_folder,strategy,book_name):
	div_txn = portfdtxns_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.copy()
	return div_txn[ div_txn['type']=='DIV' ]

def load_paired_txn(*,db_folder,strategy,book_name):
	return pairedtxns_io.load(db_dir=db_folder,strategy=strategy,portfolio=book_name).df.copy()

