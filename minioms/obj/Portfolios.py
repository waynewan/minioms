from ..oms_db.classes_io import Portfolios_IO
from jackutil.microfunc import types_validate
from copy import copy
import pandas as pd

class io_utility:
	def load(db_dir):
		return Portfolios_IO(load=True, **locals() )

	def create(base,df0):
		types_validate(base,msg="base",types=[ Portfolios_IO],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	def filter_by_account(portfolios,account):
		types_validate(portfolios,msg="portfolios",types=[ Portfolios_IO ],allow_none=False)
		types_validate(account,msg="account",types=[ str ])
		df0 = portfolios.df
		df1 = df0[df0['trade_acct']==account]
		return io_utility.create(portfolios,df1)

	def portfolio_list(portfolios):
		types_validate(portfolios,msg="portfolios",types=[ Portfolios_IO ],allow_none=False)
		df0 = portfolios.df
		df1 = df0.loc[:,['book','portfolio']]
		va1 = df1.sort_values(by=['book','portfolio']).values
		return va1

