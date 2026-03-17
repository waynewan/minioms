def print_oms_io_objects(obj):
	if(isinstance(obj, dict)):
		for key, val in obj.items():
			print(f"[{key}] {val.full_path}")
			print(val.df.to_string())
			print()
	else:
		print(obj.full_path)
		print(obj.df.to_string())
		print()

def display_objects(objects):
	try:
		get_ipython()
		for obj in objects:
			display(obj)
	except NameError:
		for obj in objects:
			print_oms_io_objects(obj)
