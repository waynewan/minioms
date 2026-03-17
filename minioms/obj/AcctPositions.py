from ..oms_db.classes_io import AcctPositions_IO
from jackutil.microfunc import types_validate

class io_utility:
	def load(db_dir, account):
		types_validate(account,msg="account",types=[ type("") ],allow_none=False)
		return AcctPositions_IO(load=True, **locals() )

class br_utility:
	pass

