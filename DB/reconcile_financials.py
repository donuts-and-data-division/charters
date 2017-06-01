from sqlalchemy import create_engine
import sys
import pandas as pd
import numpy as np
from db_config import *
from features_by_year import db_action
import psycopg2

feature_years = {
    'tests': [x for x in range(2003, 2016) if x != 2014],
    'dropouts': [x for x in range(10, 16)],
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
    #test_columns = get_feature_group_columns('catests_2013_wide')
    #columns_2015 = get_feature_group_columns('catests_2015_wide')

    master_list = []
    grades = [x for x in range(2,12)]
    subgroups = [3,4,31,74, 76, 78, 80, 120, 128]
    values = ['percent_tested', 'percentage_standard_exceeded', 'percentage_standard_met', 'percentage_standard_met_and_above',\
        'percentage_standard_nearly_met', 'percentage_standard_not_met', 'standard_exceeded_students', \
        'standard_met_students', 'standard_met_and_above_students',\
        'standard_nearly_met_students', 'standard_not_met_students']


    for grade in grades:
        for subgroup in subgroups:
            for value in values: 
                master_list.append(value + "_" + str(subgroup) + ".0" + str(grade) + ".0")
    print (len(master_list))

    #for year in feature_years['tests']:
    for year in [2015]:

        year_columns = get_feature_group_columns('catests_{}_wide'.format(year))
        missing_cols = [i for i in master_list if i not in year_columns]


        #missing_cols = [i for i in test_columns if i not in columns_2015]

        for col in missing_cols:
            
            alteration = """
            ALTER TABLE 'catests_{year}_wide'
            ADD COLUMN "{col}" numeric;""".format(year=year, col=col)
                    
            db_action(alteration, action_type='alter')


        if year == 2015:
            extra_cols = [i for i in year_columns if i not in master_list and i != 'cdscode']

            for col in extra_cols:

                alteration = """
                ALTER TABLE 'catests_{year}_wide'
                DROP COLUMN "{col}";""".format(year=year, col=col)
                    
                db_action(alteration, action_type='alter')        


def reconcile_dropout_columns():
    #test_columns = get_feature_group_columns('catests_2013_wide')
    #columns_2015 = get_feature_group_columns('catests_2015_wide')

    master_list = []
    subgroups = ['All', 'SE', 'MAL', 'MIG', 'SD', 'EL', 'FEM']
    subgroupstypes = [0,1,2,3,4,5,6,7,8,9,'All']
    values = ['percent_tested', 'percentage_standard_exceeded', 'percentage_standard_met', 'percentage_standard_met_and_above',\
        'percentage_standard_nearly_met', 'percentage_standard_not_met', 'standard_exceeded_students', \
        'standard_met_students', 'standard_met_and_above_students',\
        'standard_nearly_met_students', 'standard_not_met_students']


    for subgroup in subgroups:
        for subgrouptype in subgrouptypes:
            for value in values: 
                master_list.append(value + "_" + str(subgroup) + ".0" + str(grade) + ".0")
    print (len(master_list))

    #for year in feature_years['tests']:
    for year in [2015]:

        year_columns = get_feature_group_columns('catests_{}_wide'.format(year))
        missing_cols = [i for i in master_list if i not in year_columns]


        #missing_cols = [i for i in test_columns if i not in columns_2015]

        for col in missing_cols:
            
            alteration = """
            ALTER TABLE 'catests_{year}_wide'
            ADD COLUMN "{col}" numeric;""".format(year=year, col=col)
                    
            db_action(alteration, action_type='alter')


        if year == 2015:
            extra_cols = [i for i in year_columns if i not in master_list and i != 'cdscode']

            for col in extra_cols:

                alteration = """
                ALTER TABLE 'catests_{year}_wide'
                DROP COLUMN "{col}";""".format(year=year, col=col)
                    
                db_action(alteration, action_type='alter')        


        '''
        else:
            missing_cols = [i for i in columns_2015 if i not in test_columns]

            for col in missing_cols:
                alteration = """
                    ALTER TABLE 'catests_{year}_wide_testing'
                        ADD COLUMN "{col}" numeric;""".format(year=year, col=col)

                db_action(alteration, action_type='alter')
        '''

        #for i, column in enumerate(test_columns):
            #alteration = """ALTER TABLE "catests_{year}_wide"
                            #RENAME year_columns[i] TO test_columns[i];

def rename_dropout():

    for year in feature_years['dropouts']:

        year_columns = get_feature_group_columns('dropout_{}_wide'.format(year))
        
        for col in year_columns:
            alteration = """
            ALTER TABLE 'catests_{year}_wide'
            RENAME COLUMN "{col}" "{newname}";""".format(year=year, col=col, newname = col[:9] + col[11:])
                    
            db_action(alteration, action_type='alter')    






if __name__=="__main__":
 	#reconcile_financial_columns()
    #reconcile_test_columns()
    rename_dropout()