from db_config import * 
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from clean_csv import load, create_table, make_schema
from os import system



def edit_all():
	''' 
	Deal with raw financial, cohort, and test data that must be manipulated to "wide" format,
	i.e. each row is a single school
	'''
	for i in ['11', '12', '13']:
		edit_financials(i)
		

def edit_financials(year):
	'''
	Pull raw table for a single year of financial data from db into a dataframe, 
	transform df into wide format (one school per row), and upload back into db

	Inputs
		year: (str) representing the suffix of financial table name, e.g. "11"
	'''

	# get initial df
	query = """
		SELECT "CDSCode", "ObjectCode", "Description", total
		FROM "Alternate_Form_Data_{year}" 
		JOIN "Objects_full" 
		ON "Alternate_Form_Data_{year}"."ObjectCode" = "Objects_full"."Object" 
	    ;""".format(year=year)
	df = db_selection(query)

	# modify df
	modified_df = pd.pivot_table(df, values='total', index='CDSCode', columns=['ObjectCode'], aggfunc=np.sum)
	
	# print modified df to csv
	modified_df.to_csv('../Data/financials_{year}_wide.csv'.format(year=year))
	
	# load csv to database
	system_str = '''csvsql --db "postgresql://{}:{}@{}:{}/{}"  --insert ~/charters/Data/financials_{}_wide.csv --no-inference'''.format(USER, PASSWORD, HOST, PORT, DATABASE, year)
	try:
		system(system_str)
	except:
		print('did not create table!')

def db_selection(query):
	'''
	Utility function to run SQL query and return results in a dataframe
	'''
	db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
	engine = create_engine(db_string)
	df = pd.read_sql_query(query, engine)
	return df

def create_table(table_group, year, schema_specifics):
	'''
	Insert completed table (wide version) into db

	Inputs
		table_group: (str) "financial", "cohort", etc
		year: (str) "11", "12", etc
		schema_specifics: (str) table creation specifications

	Outputs
		None. Inserts new table into db.
	'''

	query = '''
		DROP TABLE IF EXISTS {table_group}_{year}; 
		CREATE TABLE {table_group}_{year} 
		({schema_specifics}); 
		COPY {table_group}_{year} FROM '../Data/{table_group}_{year}.csv' DELIMITER ',' CSV;
		'''.format(table_group=table_group, year=year, schema_specifics=schema_specifics)

	db_selection(query)
	

if __name__=="__main__":
	edit_all()
	#modified_df = edit_financials('11')


