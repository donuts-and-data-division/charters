from db_config import * 
from sqlalchemy import create_engine
import pandas as pd
from clean_csv import load, create_table, make_schema

def edit_all():
	for i in ['1011', '1112', '1213']:
		edit_financials(i)
		#load("../Data/financials_{}.csv".format(i), ending="")




def edit_financials(year):
	'''
	Pull raw table for a single year of financial data from db into a dataframe, 
	transform df into wide format (one school per row), and upload back into db

	Inputs
		year: (str) representing the suffix of financial table name, e.g. "11"
	'''
	db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
	engine = create_engine(db_string)

	string = """
		SELECT "CDSCode", "ObjectCode", "Description", total
		FROM "Alternate_Form_Data_{yr}" 
		JOIN "Objects_full" 
		ON "Alternate_Form_Data_{yr}"."ObjectCode" = "Objects_full"."Object" 
	    ;""".format(yr=year)
	df = pd.read_sql_query(string, engine)
	df.to_sql('financials_'+year, engine)
	#df.to_csv('../Data/financials_{yr}.csv'.format(yr=year))
	

	

if __name__=="__main__":
	edit_all()