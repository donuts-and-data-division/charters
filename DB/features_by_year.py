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

	# update data types and create CDSCode column
	alteration = """
		ALTER TABLE "Alternate_Form_Data_{year}"
			ALTER COLUMN restricted TYPE NUMERIC USING (restricted::numeric),
			ALTER COLUMN unrestricted TYPE NUMERIC USING (unrestricted::numeric),
			ALTER COLUMN total TYPE NUMERIC USING (total::numeric);
		
		ALTER TABLE "Alternate_Form_Data_{year}"
			ADD COLUMN "CDSCode" VARCHAR(20);

		UPDATE "Alternate_Form_Data_{year}"
		SET "CDSCode" = concat("Ccode","Dcode","SchoolID");
		""".format(year=year)
	db_action(alteration, action_type='alter')

	# get initial df
	select = """
		SELECT "CDSCode", "ObjectCode", "Description", total
		FROM "Alternate_Form_Data_{year}" 
		JOIN "Objects_full" 
		ON "Alternate_Form_Data_{year}"."ObjectCode" = "Objects_full"."Object" 
	    ;""".format(year=year)
	df = db_action(select, action_type='select')

	# modify df
	modified_df = pd.pivot_table(df, values='total', index='CDSCode', columns=['ObjectCode'], aggfunc=np.sum)
	
	# print modified df to csv
	modified_df.to_csv('../Data/financials_{year}_wide.csv'.format(year=year))
	
	# load csv to database
	system_str = '''csvsql --db "postgresql://{}:{}@{}:{}/{}"  --insert ~/charters/Data/financials_{}_wide.csv'''.format(USER, PASSWORD, HOST, PORT, DATABASE, year)
	try:
		system(system_str)
	except:
		print('did not create table!')

def db_action(query, action_type):
	'''
	Utility function to run SQL query and return results in a dataframe
	'''
	db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
	engine = create_engine(db_string)
	if action_type == 'select': 
		df = pd.read_sql_query(query, engine)
		return df
	elif action_type == 'alter':
		system("psql -d capp30254_project1 -U capp30254_project1_user -h pg.rcc.uchicago.edu -c '{}'".format(query))
	else:
		print('invalid action type')
	

if __name__=="__main__":
	edit_all()
	#modified_df = edit_financials('11')


