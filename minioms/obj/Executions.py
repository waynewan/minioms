from ..oms_db.classes_io import Executions_IO
from jackutil.microfunc import types_validate
from jackutil.microfunc import dt_to_str,str_to_dt
from datetime import datetime
import pandas as pd
import numpy as np

# --
# -- obj_spec: ( [ "account" ], None, DEF_IDX,  "Executions", "executions.csv", "line#,Symbol,Shares,Price,Amount" ), # account
# --
class io_utility:
	def load(db_dir,account):
		return Executions_IO(load=True, **locals() )

	def create(base,df0):
		types_validate(base,msg="base",types=[ Executions_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	# --
	def prepare_executions_for_alloc(executions):
		executions = br_utility.prepare_executions_for_matching(executions)
		executions = executions[['symbol','price','amount','pkey']]
		executions.columns = ['symbol','exec_price','amount','pkey']
		return executions

	def prepare_executions_for_matching(executions):
		# --
		# -- column names are first-letter-cap
		# --
		executions = executions[['Symbol','Shares','Price','Amount']]
		executions['Price'] = np.round(executions['Price'],4)
		executions['date'] = dt_to_str(datetime.today(),delimiter="/")
		executions['type'] = 'BUY'
		executions['abs_unit'] = np.abs(executions['Shares'])
		executions.loc[executions['Shares']<0,'type'] = 'SEL'
		executions['pkey'] = ""
		if(len(executions)>0):
			executions['pkey'] = executions.apply(lambda rr: '|'.join([
				'EXEC',
				rr['date'],
				rr['Symbol'],
				rr['type'],
				f"{rr['abs_unit']:0.0f}"
			]), axis=1)
		executions.columns = ['symbol','unit','price','amount', 'date', 'type', 'abs_unit', 'pkey']
		executions = executions[['date','symbol','unit','price','amount','pkey']]
		return executions 

