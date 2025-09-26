from ..oms_db.classes_io import PortfSetting_IO
from jackutil.microfunc import types_validate
from jackutil import containerutil as cutil
import pandas as pd
import ast
import re

# --
# -- obj_spec: ( [ "strategy", "portfolio" ], None, DEF_IDX0, "PortfSetting", "portf_setting.csv", "value,dtype" ), # portfolio
# --
class io_utility:
	def load(*,db_dir,strategy,portfolio):
		types_validate(db_dir,msg="db_dir",types=[ type("") ],allow_none=False)
		types_validate(strategy,msg="strategy",types=[ type("") ],allow_none=False)
		types_validate(portfolio,msg="portfolio",types=[ type("") ],allow_none=False)
		portf_setting = PortfSetting_IO(load=True, **locals() )
		portf_setting._df = br_utility.eval_loaded_value(portf_setting._df)
		return portf_setting 

	def load_bulk(db_dir,strat_portf_pairs):
		result = {}
		for strat,portf in strat_portf_pairs:
			result[(strat,portf)] = io_utility.load(db_dir=db_dir,strategy=strat,portfolio=portf)
		return result

	def create(base,df0):
		types_validate(base,msg="base",types=[ PortfSetting_IO ],allow_none=False)
		types_validate(df0,msg="df0",types=[ pd.DataFrame ],allow_none=True)
		newcopy = copy(base)
		newcopy._df = df0
		return newcopy

class br_utility:
	# --
	default_key_set = {
		"name":"name",
		".*wb_name":"book",
		".*sh_name":"portf",
		".*portf_benchmark":"benchmark",
		".*portf_start_date":"start_date",
		".*portf_start_principle":"principle",
		".*maxpos":"maxpos",
	}
	def basic_info_from(*,portf_setting=None,flattened=None,optional={},keys=default_key_set,):
		if(flattened is None):
			portf_settings_dict = portf_setting._df['value'].to_dict()
			flattened = cutil.flattenContainer(portf_settings_dict)
		basic_info = {}
		for key,val in keys.items():
			if(val in optional):
				defval = optional[val]
				basic_info[val] = extractValue( dict_flattened=flattened, regex_key=key, rtn_first=True, optional=True, defval=defval,)
			else:
				basic_info[val] = extractValue( dict_flattened=flattened, regex_key=key, rtn_first=True,)
		return basic_info

	def get_def_benchmark(*,book):
		def_benchmark = book
		if(book.endswith("n100")):
			def_benchmark = "QQQ"
		elif(book.endswith("s500")):
			def_benchmark = "SPY"
		elif(book.endswith("r1000")):
			def_benchmark = "IWB"
		return def_benchmark

	def porft_setting_to_df(portf_setting,flattened=None):
		if(flattened is None):
			flattened = cutil.flattenContainer(portf_setting)
		df0 = pd.DataFrame(data=[flattened]).transpose().sort_index()
		df0.columns = ['value']
		df0['dtype'] = df0['value'].apply(lambda vv: type(vv))
		return df0

	def eval_loaded_value(df0):
		# --
		def safe_literal_eval(rr):
			try:
				xx = ast.literal_eval(rr)
				return xx
			except (ValueError, SyntaxError, TypeError):
				return rr
		# --
		df0['value'] = df0['value'].apply(safe_literal_eval)
		return df0

def extractValue(dict_flattened=None,partial_key=None,regex_key=None,exact_key=None,rtn_first=True,optional=False,defval=None):
	matched_keys = None
	if(regex_key is not None):
		search_regex = re.compile(regex_key)
		matched_keys = filter(lambda kk : search_regex.match(kk), dict_flattened)
	elif(partial_key is not None):
		matched_keys = filter(lambda kk : partial_key in kk, dict_flattened)
	else:
		matched_keys = filter(lambda kk : exact_key==kk, dict_flattened)
	if(rtn_first):
		try:
			matched_key = next(matched_keys)
			return dict_flattened[matched_key]
		except StopIteration as iex:
			if(optional):
				return defval
			raise Exception(f"no such key: exact_key={exact_key};partial_key={partial_key};regex_key={regex_key}")
	else:
		return { kk : dict_flattened[kk] for kk in matched_keys }

