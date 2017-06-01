from sqlalchemy import create_engine
import sys
import pandas as pd
import numpy as np
from db_config import *
from features_by_year import db_action
import psycopg2

feature_years = {
    'tests': [x for x in range(2003, 2016) if x != 2014],
    'dropouts': [x for x in range(2010, 2016)],
    'enrollment': [x for x in range(2004, 2016)],
    'financial': [x for x in range(2004, 2016)]
    }

def get_feature_group_columns(table_name):
    '''
    Returns a list of column names for given table
    '''
    conn = psycopg2.connect("dbname={} user={} host={} password={}".format(DATABASE, USER, HOST, PASSWORD))
    cur = conn.cursor()
    string = """
        SELECT column_name FROM information_schema.columns WHERE table_name = '{}'
        ;""".format(table_name)
    cur.execute(string)
    ls = []
    for record in cur:
        ls.append(record[0])
    return ls

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


def reconcile_test_columns():
    test_columns = get_feature_group_columns('catests_2013_wide')
    columns_2015 = get_feature_group_columns('catests_2015_wide2')

    for year in feature_years['tests']:

        year_columns = get_feature_group_columns('catests_{}_wide'.format(year))

        if year == 2015:
            missing_cols = [i for i in test_columns if i not in columns_2016]

            for col in missing_cols:
            
                alteration = """
                    ALTER TABLE 'catests_2015_wide2'
                        ADD COLUMN "{col}" numeric;""".format(col=col)
                
                db_action(alteration, action_type='alter')
        else:
            missing_cols = [i for i in columns_2015 if i not in test_columns]

            for col in missing_cols:
                alteration = """
                    ALTER TABLE 'catests_{year}_wide'
                        ADD COLUMN "{col}" numeric;""".format(year=year, col=col)

                db_action   (alteration, action_type='alter')

        #for i, column in enumerate(test_columns):
            #alteration = """ALTER TABLE "catests_{year}_wide"
                            #RENAME year_columns[i] TO test_columns[i];




if __name__=="__main__":
 	#reconcile_financial_columns()
    reconcile_test_columns()