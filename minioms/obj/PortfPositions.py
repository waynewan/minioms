from ..oms_db.classes_io import PortfPositions_IO
from ..oms_db.classes_io import PairedTxns_IO
from jackutil.microfunc import types_validate
from . import PairedTxns
from copy import copy
import pandas as pd

class io_utility:
	def load(db_dir,strategy,portfolio):
		return PortfPositions_IO(load=True, **locals() )

	def create(base,df0):
		types_validate(base,msg="base",types=[ PortfPositions_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	pass

