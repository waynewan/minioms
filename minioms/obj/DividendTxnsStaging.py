from ..oms_db.classes_io import DividendTxnsStaging_IO
from ..oms_db.classes_io import DividendTxnsAdj_IO
from ..oms_db.classes_io import AcctDividendTxns_IO
from jackutil.microfunc import types_validate
import pandas as pd
import numpy as np

class io_utility:
	def load(db_dir,account):
		return DividendTxnsStaging_IO(load=True, **locals() )

class br_utility:
	# --
	# --
	# --
	def apply_adj_to(divadj, divtxn):
		types_validate(divadj,msg="divadj",types=[ DividendTxnsAdj_IO  ],allow_none=False)
		types_validate(divtxn,msg="divtxn",types=[ DividendTxnsStaging_IO ],allow_none=False)
		## --
		df0, df1 = divtxn.df, divadj.df
		for nn,rr in df1.iterrows():
			affected_txn_pkey = df0['pkey']==rr['pkey']
			affected_txn = df0[affected_txn_pkey]
			if(len(affected_txn)==0):
				print(ValueError(f"WARN: cannot find original div txn {rr}, maybe new value"))
			if(len(affected_txn)>1):
				raise ValueError(f"ERR: too many row affected {rr}, bad data")
			df0.loc[affected_txn_pkey,'Amount'] = rr['adj_Amount']
		return divtxn

# -- rm -- 	# --
# -- rm -- 	# --
# -- rm -- 	# --
# -- rm -- 	def merge_staging_into(staging,current):
# -- rm -- 		DividendTxnsStaging_IO._type_validate_(staging)
# -- rm -- 		AcctDividendTxns_IO._type_validate_(current)
# -- rm -- 		# --
# -- rm -- 		if(len(current.df)==0):
# -- rm -- 			# --
# -- rm -- 			# -- special case, local file is empty
# -- rm -- 			# --
# -- rm -- 			return [],staging.copy(),None
# -- rm -- 		else:
# -- rm -- 			side_by_side = merge_div_by_pkey_side_by_side(staging.df,current.df)
# -- rm -- 			merge_errors,merged = accept_div_txns_merge(side_by_side)
# -- rm -- 			return merge_errors,merged,side_by_side
# -- rm -- 
# -- rm -- # -- ------------------------------------------------------------------------------------------------------------------------
# -- rm -- # -- ------------------------------------------------------------------------------------------------------------------------
# -- rm -- # -- ------------------------------------------------------------------------------------------------------------------------
# -- rm -- # --
# -- rm -- # -- merge new, and old dividend transactions
# -- rm -- # -- report any inconsistency after merge
# -- rm -- # -- if any inconsistency reported, further action might fail
# -- rm -- # -- user should manually fix the inconsistency before retry
# -- rm -- # --
# -- rm -- def merge_div_by_pkey_side_by_side(staging,current):
# -- rm -- 	df0 = pd.merge(staging, current, how='outer',on='pkey', suffixes=['_new','_file'])
# -- rm -- 	df0.loc[df0['Date_new'].isnull(),'Date_new'] = df0["Date_file"]
# -- rm -- 	df0 = df0.fillna("")
# -- rm -- 	df0 = df0.sort_values(["Date_new","pkey"])
# -- rm -- 	df0 = df0.reset_index(drop=True)
# -- rm -- 	return df0
# -- rm -- 
# -- rm -- # --
# -- rm -- # -- accept the side-by-side merge result
# -- rm -- # -- combine the two data set into one
# -- rm -- # --
# -- rm -- def accept_div_txns_merge(side_by_side):
# -- rm -- 	errors = merged_dividend_txns_validation(side_by_side)
# -- rm -- 	if(len(errors)>0):
# -- rm -- 		return errors,None
# -- rm -- 	# --
# -- rm -- 	# -- merge *_new fold into *_file, then remove suffix
# -- rm -- 	# --
# -- rm -- 	accepted = side_by_side.copy()
# -- rm -- 	last_local_txn_date = np.max(accepted[accepted['Symbol_file'] !='']['Date_file'])
# -- rm -- 	cond = ( accepted['Date_file']=="" ) * ( accepted['Date_new']>=last_local_txn_date )
# -- rm -- 	if(cond.any()):
# -- rm -- 		accepted.loc[cond,'Date_file'] = accepted.loc[cond,'Date_new']
# -- rm -- 		accepted.loc[cond,'Symbol_file'] = accepted.loc[cond,'Symbol_new']
# -- rm -- 		accepted.loc[cond,'Amount_file'] = accepted.loc[cond,'Amount_new']
# -- rm -- 		accepted.loc[cond,'status_file'] = accepted.loc[cond,'status_new']
# -- rm -- 	accepted = accepted[['Date_file','Symbol_file','Amount_file','status_file','pkey']]
# -- rm -- 	accepted.columns = ['Date','Symbol','Amount','status','pkey']
# -- rm -- 	return errors,accepted
# -- rm -- 
# -- rm -- # --
# -- rm -- # -- merged: side-by-side merge, left side is latest downloaded txns; 
# -- rm -- # --         the right side is existing div txns from local file
# -- rm -- # !! must run the 2nd step accept_div_txns_merge to finalize the merge
# -- rm -- # --
# -- rm -- def merged_dividend_txns_validation(merged):
# -- rm -- 	errors = []
# -- rm -- 	# --
# -- rm -- 	last_local_txn_date = np.max(merged[merged['Symbol_file'] !='']['Date_file'])
# -- rm -- 	missing_local = merged[
# -- rm -- 		(merged['Date_new'] < last_local_txn_date) *
# -- rm -- 		(merged['Symbol_file']=="")
# -- rm -- 	]
# -- rm -- 	if(len(missing_local)>0):
# -- rm -- 		errors.append({
# -- rm -- 			'message' : "transactions found online, but not in db",
# -- rm -- 			'evidence' : missing_local
# -- rm -- 		})
# -- rm -- 	# --
# -- rm -- 	first_remote_txn_date = np.min(merged[merged['Symbol_new'] !='']['Date_new'])
# -- rm -- 	missing_remote = merged[
# -- rm -- 		(merged['Date_new'] > first_remote_txn_date) *
# -- rm -- 		(merged['Symbol_new']=='') * 
# -- rm -- 		(merged['Symbol_file'] !='')
# -- rm -- 	]
# -- rm -- 	if(len(missing_remote)>0):
# -- rm -- 		errors.append({
# -- rm -- 			'message' : "transactions found in db, but not online",
# -- rm -- 			'evidence' : missing_remote
# -- rm -- 		})
# -- rm -- 	return errors
# -- rm -- 
