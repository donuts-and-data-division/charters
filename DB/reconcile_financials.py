from sqlalchemy import create_engine
import sys
import pandas as pd
import numpy as np
from db_config import *
from features_by_year import db_action

def get_financial_columns(string, obj_header_name):
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)
    df = pd.read_sql_query(string, engine)

    return df[obj_header_name].tolist()

def reconcile_financial_columns():
	# Purpose: determine mismatching columns between years of financial data
	financial_columns = get_financial_columns(' SELECT "Object" FROM "Objects_full" ', "Object")
	year_range = range(2004, 2016)
	missing_cols = {}
	for yr in year_range:
		year_string = str(yr)[-2:]
		string = ' SELECT DISTINCT "ObjectCode" FROM "Alternate_Form_Data_{year_string}" '.format(year_string=year_string) 
		unique_obj = get_financial_columns(string, "ObjectCode")
		missing_cols = [i for i in financial_columns if i not in unique_obj]
		for col in missing_cols:
			alteration = """
				ALTER TABLE "financials_{year_string}_wide"
					ALTER COLUMN "{col}" TYPE NUMERIC USING ("{col}"::numeric);
				""".format(year_string=year_string, col=col)
			db_action(alteration, action_type='alter')

if __name__=="__main__":
 	reconcile_financial_columns()