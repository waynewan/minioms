from ..oms_db.classes_io import Executions_IO
from ..obj.Executions import io_utility as executions_io
from ..obj.Executions import br_utility as executions_br
from ..oms_db.classes_io import Allocations_IO
from ..obj.Allocations import io_utility as allocations_io
from ..obj.Allocations import br_utility as allocations_br
from ..oms_db.classes_io import PairedTxns_IO
from ..obj.PairedTxns import io_utility as ptxns_io
from ..obj.PairedTxns import br_utility as ptxns_br
from ..oms_db.classes_io import Portfolios_IO
from ..obj.Portfolios import io_utility as portf_io
from ..obj.Portfolios import br_utility as portf_br
from ..oms_db.classes_io import PortfPositions_IO
from ..obj.PortfPositions import io_utility as portfpos_io
from ..obj.PortfPositions import br_utility as portfpos_br
from jackutil.microfunc import types_validate
import pandas as pd
import numpy as np

class op_alloc_exec:
	# --
	def load_required_objects(*,db_dir,account):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(account,msg="account",types=[ type("") ],allow_none=False)
		# --
		allocations = allocations_io.load(db_dir=db_dir,account=account)
		# --
		# -- get PairedTxns for all portfolios using "account"
		# --
		all_portfolios = portf_io.load(db_dir=db_dir)
		related_portfs = portf_br.filter_by_account(all_portfolios, account)
		portfs_ptxns = ptxns_io.load_bulk(db_dir, portf_br.portfolio_list(related_portfs))
		# --
		return ( allocations,portfs_ptxns )

	def alloc_exec(allocations,portfs_ptxns):
		return impl__post_process_account(allocations,portfs_ptxns)

	def validate(alloc_results,raise_on_err=True):
		return True

	def commit_result(alloc_results):
		op_alloc_exec.validate(alloc_results,raise_on_err=True)
		for strat_portf,alloc_result in alloc_results.items():
			portfpos,ptxns,errors = alloc_result
			portfpos.write()
			ptxns.write()

# -- --------------------------------------------------------------------------------
# -- helpers
# -- --------------------------------------------------------------------------------
def impl__post_process_account(allocations,portfs_ptxns):
	allocations_df = allocations.df
	book_list = allocations_df[['book','portfolio']].drop_duplicates()
	results = {}
	db_dir = allocations.db_dir
	for _,row in book_list.iterrows():
		strat_portf = (row['book'],row['portfolio'])
		allocations_for_portfolio = allocations_df[
			(allocations_df['book']==strat_portf[0]) * 
			(allocations_df['portfolio']==strat_portf[1])
		]
		allocations_for_portfolio = replace_portf_alloc_pkey(allocations_for_portfolio)
		allocations_for_portfolio = allocations_for_portfolio.sort_values(['action','symbol'],ascending=[False,True])
		(open_pos, paired_txn, error) = post_allocation_to_portf(
			allocations_for_portfolio,
			portfs_ptxns[strat_portf].df.copy()
		)
		# --
		open_pos.index.name = 'line#'
		results[strat_portf] = (
			PortfPositions_IO(db_dir=db_dir,strategy=strat_portf[0],portfolio=strat_portf[1],df0=open_pos),
			PairedTxns_IO(db_dir=db_dir,strategy=strat_portf[0],portfolio=strat_portf[1],df0=paired_txn),
			error,
		)
	return results

def post_allocation_to_portf(allocations,paired_txn):
	# --
	# -- loading current paired txns
	# --
	for nn,sell_allo in allocations[allocations['action']=='SEL'].iterrows():
		paired_txn.loc[paired_txn['pkey']==sell_allo['linked_buy_pkey'],'linked_sell_txn'] = sell_allo['pkey']
	# --
	# -- format allocation to match paired_txn
	# --
	allocations.loc[:,'linked_sell_txn'] = "--"
	allocations.rename(inplace=True,columns={
		'action':'type',
		'exec_price':'entry price'
	})
	# --
	# -- aggregate new txns first to reduce # of entries
	# --
	fmt_alloc = allocations.groupby("pkey").aggregate(func={
		'date':'first',
		'symbol':'first',
		'type':'first',
		'linked_sell_txn':'first',
		'entry price':'first',
		'unit':'sum',
		'cost':'sum',
	}).reset_index(drop=False)
	fmt_alloc['entry price'] = np.round(np.abs(fmt_alloc['cost'] / fmt_alloc['unit']),4)
	# --
	# -- merge [old, new] paired txns
	# --
	# ?? side_by_side = pd.merge(paired_txn,fmt_alloc,how="outer",on="pkey",suffixes=['','_fmt_alloc'])
	# ?? display(side_by_side.tail())
	paired_txn = pd.concat([paired_txn,fmt_alloc],axis=0)
	is_valid,error = paired_txn_validation(paired_txn)
	if(not is_valid):
		return (None, paired_txn, error)
	else:
		# -- write_paired_txn(db_folder=db_folder,strategy=book,book_name=portfolio,paired_txn=paired_txn)
		# --
		# -- write open pos based on the combined paired_txn list
		# --
		open_pos = extract_openpos(paired_txn)
		# -- write_open_pos(db_folder=db_folder,strategy=book,book_name=portfolio,open_pos=open_pos)
		# --
		# -- 
		# --
		return (open_pos, paired_txn, error)

def replace_portf_alloc_pkey(alloc):
	working = alloc.groupby(['symbol','action'],as_index=False).aggregate(func={'unit':'sum','date':'first'})
	working['aggr_pkey'] = working.apply(lambda rr: '|'.join([
		rr['date'],
		rr['symbol'],
		rr['action'],
		f"{abs(rr['unit'])}"
	]),axis=1)
	working = pd.merge(alloc,working,how='outer',on=['date','symbol','action'],suffixes=['','_working'])
	working.rename(inplace=True,columns={
		'pkey':'ord_pkey',
		'aggr_pkey':'pkey'
	})
	return working

def paired_txn_validation(txn):
	return True,{}

def extract_openpos(paired_txn):
	open_pos = paired_txn[np.logical_and(paired_txn["type"]=="BUY",paired_txn["linked_sell_txn"]=="--")]
	open_pos = open_pos.drop(columns=["linked_sell_txn"])
	return open_pos

