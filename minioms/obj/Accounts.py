from ..oms_db.classes_io import Accounts_IO

class io_utility:
	def load(db_dir):
		return Accounts_IO(load=True, **locals() )

class br_utility:
	pass

