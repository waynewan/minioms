from .op_gen_account_orders import op_gen_account_orders
from .op_gen_portf_orders import op_gen_portf_orders

# --
# -- portf orders
# --
def gen_orders_for_book_v5(*,db_dir,book):
	book_orders = op_gen_portf_orders.gen_book_orders(db_dir,book,version=5)
	return book_orders

def pprint_book_orders(book_orders):
	for k,v in book_orders.items():
		print("#",k,"#"*80)
		print(v['instructions'])

# --
# -- account orders
# --
def gen_orders_for_all_accounts(*,db_dir):
	loaded_objects = op_gen_account_orders.load_required_objects(db_dir=db_dir,account=None)
	results = op_gen_account_orders.gen_account_orders(loaded_objects)
	op_gen_account_orders.commit_result(results)
	return results

def pprint_all_accounts_orders(results):
	for acct,portfs_dords in results.items():
		print("#"*120)
		print(acct)
		print("#"*120)
		if(portfs_dords !=(None,None)):
			print(portfs_dords[0].df.to_string())
			print(portfs_dords[1].df.to_string())
		else:
			print(portfs_dords)

