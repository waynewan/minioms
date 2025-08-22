from ..oms_db.classes_io import DividendTxnsStaging_IO
from ..oms_db.classes_io import DividendTxnsAdj_IO
from ..oms_db.classes_io import AcctDividendTxns_IO
from ..obj.DividendTxnsStaging import io_utility as divstg_io
from ..obj.DividendTxnsStaging import br_utility as divstg_br
from ..obj.AcctDividendTxns import io_utility as acctdiv_io
from ..obj.AcctDividendTxns import br_utility as acctdiv_br
from ..obj.DividendTxnsAdj import io_utility as divadj_io
from jackutil.microfunc import types_validate
import pandas as pd
import numpy as np

class op_merge_div_staging:
	# --
	def load_required_objects(*,db_dir,account):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(account,msg="account",types=[ type("") ],allow_none=False)
		loaded = acctdiv_io.load(db_dir=db_dir,account=account)
		staging = divstg_io.load(db_dir=db_dir,account=account)
		manual_dtxn_adj = divadj_io.load(db_dir=db_dir,account=account)
		divstg_br.apply_adj_to(manual_dtxn_adj,staging)
		return loaded,staging

	def merge(acctdtxns,staging):
		types_validate(acctdtxns,msg="acctdtxns",types=[ AcctDividendTxns_IO ],allow_none=False)
		types_validate(staging,msg="staging",types=[ DividendTxnsStaging_IO ],allow_none=False)
		# --
		if(len(acctdtxns.df)==0):
			# --
			# -- special case, no staging entries
			# --
			return [],None,None
		else:
			side_by_side = merge_div_by_pkey_side_by_side(staging.df,acctdtxns.df)
			merge_errors,merged = accept_div_txns_merge(side_by_side)
			merged.index.name = 'line#'
			return merge_errors,acctdiv_io.create(acctdtxns,merged),side_by_side

	def validate(merge_result,raise_on_err=True):
		errors,merged,side_by_side = merge_result
		if( len(errors)>0 ):
			raise ValueError(errors)

	def commit_result(merge_result):
		errors,merged,side_by_side = merge_result
		op_merge_div_staging.validate(merge_result,raise_on_err=True)
		merged.write()

# -- ------------------------------------------------------------------------------------------------------------------------
# -- ------------------------------------------------------------------------------------------------------------------------
# -- ------------------------------------------------------------------------------------------------------------------------
# --
# -- merge new, and old dividend transactions
# -- report any inconsistency after merge
# -- if any inconsistency reported, further action might fail
# -- user should manually fix the inconsistency before retry
# --
def merge_div_by_pkey_side_by_side(staging,current):
	df0 = pd.merge(staging, current, how='outer',on='pkey', suffixes=['_new','_file'])
	df0.loc[df0['Date_new'].isnull(),'Date_new'] = df0["Date_file"]
	df0 = df0.fillna("")
	df0 = df0.sort_values(["Date_new","pkey"])
	df0 = df0.reset_index(drop=True)
	return df0

# --
# -- accept the side-by-side merge result
# -- combine the two data set into one
# --
def accept_div_txns_merge(side_by_side):
	errors = merged_dividend_txns_validation(side_by_side)
	if(len(errors)>0):
		for err in errors:
			print(err)
		raise ValueError(errors)
		# return errors,None
	# --
	# -- merge *_new fold into *_file, then remove suffix
	# --
	accepted = side_by_side.copy()
	last_local_txn_date = np.max(accepted[accepted['Symbol_file'] !='']['Date_file'])
	cond = ( accepted['Date_file']=="" ) * ( accepted['Date_new']>=last_local_txn_date )
	if(cond.any()):
		accepted.loc[cond,'Date_file'] = accepted.loc[cond,'Date_new']
		accepted.loc[cond,'Symbol_file'] = accepted.loc[cond,'Symbol_new']
		accepted.loc[cond,'Amount_file'] = accepted.loc[cond,'Amount_new']
		accepted.loc[cond,'status_file'] = accepted.loc[cond,'status_new']
	accepted = accepted[['Date_file','Symbol_file','Amount_file','status_file','pkey']]
	accepted.columns = ['Date','Symbol','Amount','status','pkey']
	return errors,accepted

# --
# -- merged: side-by-side merge, left side is latest downloaded txns; 
# --         the right side is existing div txns from local file
# !! must run the 2nd step accept_div_txns_merge to finalize the merge
# --
def merged_dividend_txns_validation(merged):
	errors = []
	# --
	last_local_txn_date = np.max(merged[merged['Symbol_file'] !='']['Date_file'])
	missing_local = merged[
		(merged['Date_new'] < last_local_txn_date) *
		(merged['Symbol_file']=="")
	]
	if(len(missing_local)>0):
		errors.append({
			'message' : "transactions found online, but not in db",
			'evidence' : missing_local
		})
	# --
	first_remote_txn_date = np.min(merged[merged['Symbol_new'] !='']['Date_new'])
	missing_remote = merged[
		(merged['Date_new'] > first_remote_txn_date) *
		(merged['Symbol_new']=='') * 
		(merged['Symbol_file'] !='')
	]
	if(len(missing_remote)>0):
		errors.append({
			'message' : "transactions found in db, but not online",
			'evidence' : missing_remote
		})
	return errors


