from ..obj.PortfDailyOrders import io_utility as portfdord_io
from ..obj.Accounts import io_utility as acct_io
from ..obj.Portfolios import io_utility as portf_io
from ..obj.Portfolios import br_utility as portf_br
from ..oms_db.classes_io import AcctDailyOrders_IO
from ..oms_db.classes_io import AccountOrders_IO
from jackutil.microfunc import types_validate
import pandas as pd

class op_gen_account_orders:
	def load_required_objects(*,db_dir,account=None):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(account,msg="account",types=[ type("") ],allow_none=True)
		# --
		# -- get PairedTxns for all portfolios using "account"
		# --
		portfs_dords_map = {}
		all_accounts = [ account ]
		if(account is None):
			all_accounts = acct_io.load(db_dir).df['key'].tolist()
		for acct in all_accounts:
			all_portfolios = portf_io.load(db_dir=db_dir)
			related_portfs = portf_br.filter_by_account(all_portfolios, acct)
			portfs_dords  = portfdord_io.load_bulk(db_dir, portf_br.portfolio_list(related_portfs))
			portfs_dords_map[acct] = portfs_dords
		return portfs_dords_map

	def gen_account_orders(portfs_dords_map):
		return generate_orders_for_all_accounts(portfs_dords_map)

	def commit_result(results):
		for acct,portfs_dords in results.items():
			if(portfs_dords !=(None,None)):
				portfs_dords[0].write()
				portfs_dords[1].write()

# -- ----------------------------------------------------------------------------
# -- new code, but some base on old code from bookkeeper_daily_orders
# -- ----------------------------------------------------------------------------
def generate_orders_for_all_accounts(portfs_dords_map):
	account_order_map = {}
	for acct,portfs_dords in portfs_dords_map.items():
		orders,consolidated = generate_orders_for_account(acct,portfs_dords)
		account_order_map[acct] = (orders,consolidated)
	return account_order_map

def generate_orders_for_account(acct,portfs_dords):
	all_orders = []
	db_dir = None
	for strat_portf,portf_dords in portfs_dords.items():
		if(len(portf_dords.df) >0):
			db_dir = portf_dords.db_dir
			all_orders.append(portf_dords.df)
	if(len(all_orders)==0):
		return None,None
	daily_orders = pd.concat(all_orders).iloc[:,:]
	daily_orders.sort_values(by=['symbol','unit'],inplace=True)
	daily_orders.reset_index(drop=True,inplace=True)
	daily_orders = daily_orders["book,portfolio,date,symbol,action,unit,price,linked_buy_pkey,pkey".split(",")]
	# --
	consolidated = daily_orders[['symbol','action','unit']]
	consolidated = consolidated[['symbol','unit']].groupby(by='symbol').sum()
	# --
	daily_orders = AcctDailyOrders_IO(db_dir=db_dir,account=acct,df0=daily_orders)
	consolidated = AccountOrders_IO(db_dir=db_dir,account=acct,df0=consolidated)
	return daily_orders,consolidated

