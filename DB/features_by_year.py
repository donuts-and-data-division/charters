from db_config import * 
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from os import system
from clean_csv import load, TYPE_DICT, VERBOSE, TIMER


def edit_all():
	''' 
	Deal with raw financial data that must be manipulated to "wide" format,
	i.e. each row is a single school
	'''
	for i in ['04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15']:
		#edit_financials(i)
		edit_enrollment(i)
	
def edit_enrollment(year):
	# get initial df
	select = """
		SELECT "enrollment{year}".*
		FROM "enrollment{year}"
		JOIN ca_pubschls_new ON "enrollment{year}"."cds_code" = ca_pubschls_new."cdscode"
		WHERE charter = TRUE  
		;""".format(year=year)
	df = db_action(select, action_type='select')
	ethnic09 = df[(df['ethnic'] == 9) | (df['ethnic'] == 0)]
	ethnic09 = ethnic09.groupby(["cds_code","gender"]).sum()
	ethnic09 = ethnic09.reset_index(level=[0,1]) 
	ethnic09['ethnic'] = 8

	df = df[~((df['ethnic'] == 9) | (df['ethnic'] == 0))]
	new_df = pd.concat([ethnic09, df], axis=0)
	new_df['ethnic'] = new_df['ethnic'].astype(str)
	print(df.shape)
	# modify df
	values = [
		'kdgn', 
		'gr_1', 
		'gr_2', 
		'gr_3', 
		'gr_4', 
		'gr_5', 
		'gr_6', 
		'gr_7', 
		'gr_8', 
		'ungr_elm', 
		'gr_9', 
		'gr_10', 
		'gr_11', 
		'gr_12', 
		'ungr_sec', 
		'enr_total', 
		'adult']
	print(new_df.columns)
	modified_df = pd.pivot_table(new_df, values=values, index='cds_code', columns=['ethnic','gender'])
	modified_df.columns = modified_df.columns.map('_'.join)
	modified_df.reset_index(inplace=True)
	modified_df.to_csv('../Data/enrollment{year}_wide.csv'.format(year=year))

	# load csv to database
	
	data = 'enrollment{}_wide.csv'.format(year)
	table = 'enrollment{}_wide.csv'.format(year)
	#try:
	load(filepaths=[data], outnames=[data], make_id_cols=None, db = "postgresql://{}:{}@{}:{}/{}".format(USER, PASSWORD, HOST, PORT, DATABASE), clean=False)
	#except:
	#	print('did not create table!')
	
	

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
	if year in ['04', '05', '06']:
		print('need to alter year')
		alteration = """
			ALTER TABLE "Alternate_Form_Data_{year}"
				RENAME COLUMN objectcode TO "ObjectCode";
			ALTER TABLE "Alternate_Form_Data_{year}"
				RENAME COLUMN ccode TO "Dcode";
			ALTER TABLE "Alternate_Form_Data_{year}"
				RENAME COLUMN dcode TO "Ccode";
			ALTER TABLE "Alternate_Form_Data_{year}"
				RENAME COLUMN schoolid TO "SchoolID";

			""".format(year=year) + alteration
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
	
	# load csv to database; CHANGE TO ARI'S LOAD FUNCTION?
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
	

