import pandas as pd
import os
from typing import Optional, Union, Sequence

class DataFile:
	"""
	A class to handle reading, processing, and writing data from/to a CSV file using pandas.

	Attributes:
		full_path (str): The full path to the CSV file.
		_df (pd.DataFrame):  Stores the content of the CSV file as a pandas DataFrame.
							This attribute is intended to be read-only.

	Methods:
		read(): Reads the CSV file into a pandas DataFrame.
		write(): Writes the internal pandas DataFrame to the CSV file.
		df (property): A read-only property to access the DataFrame.
	"""
	def __init__(self, directory, filename, columns: Optional[Union[str, Sequence[str]]] = None, df0: Optional[pd.DataFrame] = None):
		"""
		Initializes the DataFile object with the directory path, filename, and optional columns or initial DataFrame.

		Args:
			directory (str): The directory where the file is located.
			filename (str): The name of the CSV file.
			columns (Optional[Union[str, Sequence[str]]]): A comma-separated string or list/tuple of column names.
														If provided, an empty DataFrame with these columns is created.
			df0 (Optional[pd.DataFrame]): An initial DataFrame to use. If provided, the file is not read until the read()
										  method is called.
		"""
		self.full_path = os.path.join(directory, filename)
		if (df0 is not None):
			if (not isinstance(df0, pd.DataFrame)):
				raise TypeError("df0 must be a pandas DataFrame")
			self._df = df0
		elif (columns):
			if (isinstance(columns, str)):
				columns = [col.strip() for col in columns.split(',')]
			self._df = pd.DataFrame(columns=columns)
		else:
			self._df = None  # Initialize _df to None

	def read(self, drop=False, columns=None, idx_col=None):
		"""
		Reads the CSV file into a pandas DataFrame.

		Args:
			drop (bool, optional): If True, the current DataFrame is dropped before reading
								  the file. If False (default), raises an exception if a DataFrame
								  is already loaded.
		"""
		if (self._df is not None and not drop):
			raise ValueError("DataFrame already loaded. Use drop=True to override.")
		try:
			self._df = pd.read_csv(self.full_path, index_col=idx_col)
		except FileNotFoundError:
			if(columns is not None):
				self._df = pd.DataFrame(columns=columns)
			else:
				raise FileNotFoundError(f"Error: File not found at {self.full_path}")
		except Exception as e:
			raise Exception(f"An error occurred while reading the file: {e}")

	def write(self):
		"""
		Writes the internal pandas DataFrame to the CSV file.
		"""
		if (self._df is None):
			raise ValueError("No DataFrame to write.  Read the file or provide a DataFrame first.")
		try:
			self._df.to_csv(self.full_path, index=True)
		except Exception as e:
			raise Exception(f"An error occurred while writing to the file: {e}")

	@property
	def df(self):
		"""
		Read-only property that returns the pandas DataFrame.

		Returns:
			pd.DataFrame: The pandas DataFrame containing the data from the CSV file.
					  Returns None if the file has not been read yet.
		"""
		return self._df

	def __repr__(self):
		"""
		Returns a string representation of the DataFile object.
		"""
		return f"DataFile(full_path='{self.full_path}')"

