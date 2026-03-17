from .op_merge_div_staging import op_merge_div_staging
from .op_alloc_div import op_alloc_div

# --
# -- General steps
# -- -- part A
# -- 1) load new div txn downloaded from bank (this step is done as part of daily execution capture step)
# -- 2) load local copy of div txn (from database)
# -- 3) merge "1)" and "2)"
# -- 4) check for error and commit the change (write to file)
# -- -- part B
# -- 5) load portfolio related to the account
# -- 6) alloc dollar div based on unit distribution on pay date (!! should use ex-div date for accuracy !!)
# -- 7) apply the dollar div alloc
# -- 8) check for error, commit all changes (write to files)
#--
def process_account_div(db_dir, target_account, auto_commit=True):
	# --
	# -- DIV STAGING MERGE
	# --
	merge_div_staging_objects = op_merge_div_staging.load_required_objects(db_dir=db_dir,account=target_account)
	div_staging_merge_result = op_merge_div_staging.merge(*merge_div_staging_objects)
	op_merge_div_staging.validate(div_staging_merge_result,raise_on_err=True)
	if(auto_commit):
		op_merge_div_staging.commit_result(div_staging_merge_result)
	# --
	# -- PORTFOLIO DIV ALLOC MERGE
	# --
	print("#"*120)
	print("# div staging merge ")
	print("#"*120)
	print(div_staging_merge_result[1], "[tail]")
	print(div_staging_merge_result[1].df.tail(10).to_string())
	# --
	div_alloc_objects = op_alloc_div.load_required_objects(db_dir=db_dir, account=target_account)
	div_alloc_merge_results = op_alloc_div.merge(*div_alloc_objects)
	op_alloc_div.validate(div_alloc_merge_results,raise_on_err=True)
	if(auto_commit):
		op_alloc_div.commit_result(div_alloc_merge_results)
	# --
	print("#"*120)
	print("# portf div merge ")
	print("#"*120)
	acctdiv,div_alloc_merge_results = div_alloc_merge_results
	if(div_alloc_merge_results):
		print("#"*120)
		print("# div staging merge (after posting to portfolio)")
		print("#"*120)
		print(acctdiv, "[tail]")
		print(acctdiv.df.tail(10).to_string())
		# --
		for strat_portf,div_alloc_merge in div_alloc_merge_results.items():
			_,portf_dtxn,_ = div_alloc_merge
			print(portf_dtxn, "[tail]")
			print(portf_dtxn.df.tail().to_string())
	else:
		print("*** NO_NEW_DIV ***")
		print("*** NO_NEW_DIV ***")
		print("*** NO_NEW_DIV ***")
	return div_staging_merge_result,div_alloc_merge_results

