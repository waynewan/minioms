from .op_exec_match import op_exec_match
from .op_alloc_exec import op_alloc_exec

def post_process_account(db_dir,account,auto_commit=True):
	# --
	# -- match and alloc per account executions
	# --
	objects_for_match = op_exec_match.load_required_objects(db_dir=db_dir,account=account)
	matching_results = op_exec_match.exec_match(*objects_for_match)
	try:
		op_exec_match.validate(matching_results)
	except:
		display(objects_for_match)
		raise
	if(auto_commit):
		op_exec_match.commit_result(matching_results)

	# --
	# -- post account alloc to per portfolio
	# --
	objects_for_post = op_alloc_exec.load_required_objects(db_dir=db_dir,account=account)
	alloc_results = op_alloc_exec.alloc_exec(*objects_for_post)
	try:
		op_alloc_exec.validate(alloc_results)
	except:
		display(objects_for_post)
		raise
	if(auto_commit):
		op_alloc_exec.commit_result(alloc_results)
		
	return matching_results, alloc_results

def display_results(results):
	matching_results, alloc_results = results
	print("#"*120)
	print("# matching")
	display(matching_results[2].df)
	print("#"*120)
	print("# allocations")
	display(matching_results[3].df)
	print("#"*120)
	# --
	print("#"*120)
	for strat_portf,alloc_result in alloc_results.items():
		print("#"*120)
		print(strat_portf)
		print("*"*120)
		print("**** error ****", alloc_result[2])
		print("*"*120)
		print("paired_txns [tail]")
		display(alloc_result[1].df.tail())
		print("open_pos [tail]")
		display(alloc_result[0].df.tail())
