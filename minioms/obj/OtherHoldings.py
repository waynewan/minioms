from ..oms_db.classes_io import OtherHoldings_IO

class io_utility:
	def bookkeeper_report_load_wrapper(df0):
		holding = df0.loc[:,["symbol","quantity","note"]]
		holding.columns = ['symbol','other_holding','note']
		return holding.iloc[:,:2]

