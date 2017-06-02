from util import map_camel_to_snake
import pandas as pd
import re
from os import system

def read_csv(file, parse_numeric_strings=None, normalize=False, **kwargs):
	'''
	open a csv into dataFrame.
	

	options include built in pandas options (see documentation use **kwargs)
	'''
	### parse dates ... 
	# if we have zipcodes or other administrative numbers that we want to use as strings
	# parse them.  future question 'object' or 'categorical' ?
	if isinstance(parse_numeric_strings, list):
		dtype = {zc: 'str' for zc in parse_numeric_strings}
		if kwargs.get("dtype", False):
			kwargs['dtype'] = {**kwargs['dtype'],**dtype}
		else:
			kwargs['dtype'] = dtype

	df = pd.read_csv(file, **kwargs)
	if normalize:
		df.columns = map_camel_to_snake(df.columns)
	return df



def remove_header(files):
	'''Given a list of files, remove the first line from the files. '''
	for file in files:
		try:
			system("""sed '1d' {file} > tmpfile; mv tmpfile {file}""".format(file=file))
		except:
			print("could not find file {file}".format(file=file))

