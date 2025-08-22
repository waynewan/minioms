from ..oms_db.classes_io import AcctDividendTxns_IO
from ..oms_db.classes_io import PortfDividendTxns_IO
from ..obj.Portfolios import io_utility as portf_io
from ..obj.Portfolios import br_utility as portf_br
from ..obj.PairedTxns import io_utility as ptxns_io
from ..obj.AcctDividendTxns import io_utility as acctdiv_io
from ..obj.AcctDividendTxns import br_utility as acctdiv_br
from ..obj.PortfDividendTxns import io_utility as portfdiv_io
from ..obj.PortfDividendTxns import br_utility as portfdiv_br
from ..obj.PairedTxns import br_utility as ptxns_br
from jackutil.microfunc import types_validate
from jackutil.microfunc import dt_to_str,str_to_dt
from datetime import datetime
from copy import copy
import pandas as pd
import numpy as np

class op_alloc_div:
	# --
	def load_required_objects(*,db_dir,account,acctdiv=None):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(account,msg="account",types=[ type("") ],allow_none=False)
		types_validate(acctdiv,msg="acctdiv",types=[ AcctDividendTxns_IO ],allow_none=True)
		# --
		if(acctdiv is None):
			acctdiv = acctdiv_io.load(db_dir=db_dir,account=account)
		# --
		# -- get PairedTxns for all portfolios using "account"
		# --
		all_portfolios = portf_io.load(db_dir=db_dir)
		related_portfs = portf_br.filter_by_account(all_portfolios, account)
		portfs_ptxns = ptxns_io.load_bulk(db_dir, portf_br.portfolio_list(related_portfs))
		portfs_dtxns = portfdiv_io.load_bulk(db_dir, portf_br.portfolio_list(related_portfs))
		# --
		return acctdiv,portfs_ptxns,portfs_dtxns

	def merge(acctdiv,portfs_ptxns,portfs_dtxns):
		types_validate(acctdiv,msg="acctdiv",types=[ AcctDividendTxns_IO ],allow_none=False)
		types_validate(portfs_ptxns,msg="portfs_ptxns",types=[ dict ],allow_none=False)
		types_validate(portfs_dtxns,msg="portfs_dtxns",types=[ dict ],allow_none=False)
		# --
		# -- alloc dollar div to each related portofolios
		# --
		dollar_alloc_by_portf = alloc_div_by_portf(acctdiv,portfs_ptxns)
		# !!
		# !! alloc_div_by_portfupdated acctdiv, and need to be stored !!
		# !!
		# --
		# -- merge alloc to portofolios' div txns table
		# --
		return acctdiv,merge_div_by_legacy_key_side_by_side(portfs_dtxns,dollar_alloc_by_portf)

	def validate(merge_results, raise_on_err=True):
		acctdiv,merge_results = merge_results
		for key,result in merge_results.items():
			errors,merged,side_by_side = result
			if(len(errors)>0):
				raise ValueError(errors)

	# --
	# -- TODO: fix this
	# !! write is not atomic, so if something failed here, 
	# !! it will need a lot of manual fixes on data file
	# --
	def commit_result(merge_results, auto_commit=True):
		op_alloc_div.validate(merge_results,raise_on_err=True)
		acctdiv,merge_results = merge_results
		if(auto_commit):
			acctdiv.write()
		for key,result in merge_results.items():
			errors,merged,side_by_side = result
			if(auto_commit):
				merged.write()

# -- ----------------------------------------------------------------------------
# -- old code from bookkeeper_dividend
# -- ----------------------------------------------------------------------------
# --
# -- acctdiv : acct dividends
# -- portf_ptxns : { (strategy,portfolio) : ptxns }
# --
def alloc_div_by_portf(acctdiv,portf_ptxns):
	types_validate(acctdiv,msg="acctdiv",types=[ AcctDividendTxns_IO ],allow_none=False)
	types_validate(portf_ptxns,msg="portf_ptxns",types=[ dict ],allow_none=False)
	allocations = []
	d_acct = acctdiv.account
	for nn,dtxn in acctdiv.df.iterrows():
		if(dtxn['status'] !="LOADED"):
			continue
		postab_for_dtxn = build_pos_table(d_acct,dtxn, portf_ptxns)
		if(postab_for_dtxn.empty):
			acctdiv.df.at[nn,'status'] = "IGNORED"
			continue
		compute_dollar_div(dtxn,postab_for_dtxn)
		acctdiv.df.at[nn,'status'] = "PROCESSED"
		allocations.append(postab_for_dtxn)
	# --
	# -- trivial case, no allocation
	# --
	if(len(allocations)==0):
		return {}
	# --
	# -- rearrange all alloc into (strat,portf)-->alloc_df
	# --
	result = {}
	allocations = pd.concat(allocations,axis=0)
	unique_portf = allocations.loc[:,['strat','portf']].drop_duplicates().values
	for strat,portf in unique_portf:
		an_alloc = allocations[
			(allocations['strat']==strat) * (allocations['portf']==portf)
		]
		# --
		# -- remove strat,book columns
		# --
		result[(strat,portf)] = an_alloc.iloc[:,2:]
	# --
	return result 

# --
# -- I will need to know both objects well to build PortfDividendTxns
# !! Based on the principle, builder should know what it is building
# !! that's why pos__ is here, but isolated br_utility (bussiness rule)
# -- obj_spec: "line#,account,pay_date,enter_date,type,symbol,amount,dtxn_pkey,unit,note1" )
# --
def build_pos_table(d_acct,dtxn,portf_ptxns):
	pos_table = []
	d_sym = dtxn['Symbol']
	d_date = str_to_dt(dtxn['Date'])
	d_pkey = dtxn['pkey']
	for key,a_ptxn in portf_ptxns.items():
		ptxns_for_sym = ptxns_br.filter_by_symbol(a_ptxn,d_sym)
		rollback_ptxns = ptxns_br.rollback_to_date(ptxns_for_sym,d_date)
		rollback_pos = ptxns_br.extract_openpos(rollback_ptxns)
		if(not rollback_pos.df.empty):
			rollback_pos = rollback_pos.df.groupby("symbol",as_index=False).agg({"unit":"sum"})
			rollback_pos = rollback_pos.iloc[0]
			df0 = {
				# -- only for div computation
				'strat' : key[0],
				'portf' : key[1],
				# -- 
				'account' : d_acct,
				'pay_date' : dt_to_str(d_date,delimiter='/'),
				'enter_date' : dt_to_str(datetime.today(),delimiter='/'),
				'type' : 'DIV',
				'symbol' : rollback_pos['symbol'],
				'amount' : 0.00, # !! placeholder, need total # of units
				'dtxn_pkey' : d_pkey,
				'unit' : rollback_pos['unit'],
				'note1' : None,
			}
			pos_table.append( df0 )
	return pd.DataFrame(pos_table)

def compute_dollar_div(dtxn,pos):
	row,col = pos.shape
	if(row==0):
		return
	if(row==1):
		pos['amount'] = dtxn['Amount']
		return
	# --
	total_unit = pos['unit'].sum()
	pos['amount'] = np.round(pos['unit'] / total_unit * dtxn['Amount'],2)
	last_div = round(dtxn['Amount'] - pos['amount'].iloc[0:-1].sum(),2)
	pos.iloc[-1,pos.columns.get_loc('amount')] = last_div

# --
# -- merge dollar div alloc to portfolio div txn table
# -- portfs_dtxns : { (start,portf) : PortfDividendTxns_IO } : div txn from database
# -- dollar_div_alloc : { (start,portf) : [DataFrame]dollar_div_alloc } : new div txn from broker
# --
def merge_div_by_legacy_key_side_by_side(portfs_dtxns,dollar_div_alloc):
	results = {}
	for strat_portf in dollar_div_alloc.keys():
		p_dtxns = portfs_dtxns[strat_portf]
		d_alloc = dollar_div_alloc[strat_portf]
		errors,merged,side_by_side = merge_div_by_legacy_key_side_by_side_1_v2(p_dtxns, d_alloc)
		if(merged is not None):
			merged.index.name = "line#"
		results[strat_portf] = (errors,portfdiv_io.create(p_dtxns,merged),side_by_side)
	return results

def merge_div_by_legacy_key_side_by_side_1_v2(old_div,new_div):
	from_file = portfdiv_io.upgrade_v0(old_div.df.copy())
	pf_div_txn = new_div.copy()
	# --
	pf_div_txn = pf_div_txn.copy()
	pf_div_txn['legacy_key'] = pf_div_txn[["type","pay_date","symbol"]].astype(str).agg("|".join,axis=1)
	# --
	side_by_side = pd.merge(pf_div_txn,from_file,how='outer',on='legacy_key', suffixes=['_new','_file'])
	side_by_side = side_by_side.sort_values(["legacy_key"])
	side_by_side = side_by_side.reset_index(drop=True)
	side_by_side = side_by_side.fillna("")
	# --
	errors = div_alloc_validate_merge_plan(old_div,side_by_side)
	# --
	accepted = None
	if(not errors):
		accepted = div_alloc_accept_merge_plan(side_by_side)
	# --
	return errors,accepted,side_by_side

# --
# -- report any possible errors
# --
def div_alloc_validate_merge_plan(old_div,side_by_side):
	errors = []
	# --
	cond1 = (side_by_side['type_new'] !="") * (side_by_side['type_file'] !="")
	if(cond1.any()):
		keys = ','.join( side_by_side[cond1]['dtxn_pkey_new'].tolist() )
		errors.append({
			'loc' : str(old_div),
			'message' : "trying to overwrite old div entry",
			'evidence' : keys,
		})
	# --
	# all_file_dtxns = side_by_side[ side_by_side['type_file'] !="" ]
	last_pay_date_on_file = side_by_side['pay_date_file'].max()
	cond2 = (side_by_side['type_new'] !="") * (side_by_side['pay_date_new'] < last_pay_date_on_file) * (side_by_side['type_file']=="")
	# print(last_pay_date_on_file)
	# print(cond2)
	if(cond2.any()):
		keys = ','.join( side_by_side[cond2]['dtxn_pkey_new'].tolist() )
		errors.append({
			'loc' : str(old_div),
			'message' : "old div entry found without a match on file",
			'evidence' : keys
		})
	return errors

# --
# --
# --
def div_alloc_accept_merge_plan(side_by_side):
	file_dtxn = side_by_side[side_by_side['type_file'] !=""].copy()
	file_dtxn = file_dtxn["account_file,pay_date_file,enter_date_file,type_file,symbol_file,amount_file,dtxn_pkey_file,unit_file,note1_file".split(",")]
	file_dtxn.columns = "account,pay_date,enter_date,type,symbol,amount,dtxn_pkey,unit,note1".split(",")
	new_dtxn  = side_by_side[side_by_side['type_new'] !=""].copy()
	new_dtxn = new_dtxn["account_new,pay_date_new,enter_date_new,type_new,symbol_new,amount_new,dtxn_pkey_new,unit_new,note1_new".split(",")]
	new_dtxn.columns = "account,pay_date,enter_date,type,symbol,amount,dtxn_pkey,unit,note1".split(",")
	accepted = pd.concat([file_dtxn,new_dtxn],axis=0)
	return accepted

