from ..oms_db.classes_io import AcctPositions_IO

class io_utility:
	def load(db_dir):
		return AcctPositions_IO(load=True, **locals() )

class br_utility:
	pass

