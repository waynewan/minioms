from ..oms_db.classes_io import AcctPositionReport_IO
from jackutil.microfunc import types_validate

class io_utility:
	def load(db_dir, account):
		types_validate(account, msg="account", types=[type("")], allow_none=False)
		# create=False: skip column enforcement to allow dynamic portfolio columns
		return AcctPositionReport_IO(db_dir=db_dir, account=account, load=True, create=False)

	def save(db_dir, account, report_df):
		types_validate(account, msg="account", types=[type("")], allow_none=False)
		obj = AcctPositionReport_IO(db_dir=db_dir, account=account, df0=report_df)
		obj.write()

class br_utility:
	pass
