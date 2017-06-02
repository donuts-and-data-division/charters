from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from db_config import *
import psycopg2
import datetime as dt
from util import get_feature_group_columns

def select_function(year_list):
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)
    asnull = ''
    final_string = ''

    for yr in year_list: 

        open_cutoff = dt.datetime(int(yr)-1+2000, 7, 1).date()
        
        joins = """
            FROM ca_pubschls_new
            LEFT JOIN financials_{yr}_wide ON financials_{yr}_wide."CDSCode" = ca_pubschls_new."cdscode" 
            LEFT JOIN "2015-16_AllCACharterSchools_new" ON "2015-16_AllCACharterSchools_new"."cds_code" = ca_pubschls_new."cdscode" 
            LEFT JOIN "enrollment{yr}_wide" ON "enrollment{yr}_wide".cds_code = ca_pubschls_new."cdscode" 
            """.format(yr=yr)

        if yr != '14':
            asnull = 'Null as'
            join = 'LEFT JOIN "catests_20{yr}_wide" on catests_20{yr}_wide."cdscode" = ca_pubschls_new."cdscode"'.format(yr=yr)
            test_string = 'catests_20{yr}_wide.*'.format(yr=yr)
            joins = joins + ' ' + join

        if yr == '14':
            test_cols = get_feature_group_columns('catests_2015_wide')
            test_cols = ['"' + x + '"' for x in test_cols]
            test_string = "Null as " + ", Null as ".join(test_cols)

        dropout_select = """
                        , "GED Rate{yr}_AllAll" as ged_rate, "Special Ed Completers Rate{yr}_AllAll" as special_ed_compl_rate, 
                        "Cohort Graduation Rate{yr}_AllAll" as cohort_grad_rate, "Cohort Dropout Rate{yr}_AllAll" as cohort_dropout_rate
                        """.format(yr=yr)

        if yr not in ['10', '11', '12', '13', '14', '15']:
            cols = ['ged_rate', 'special_ed_compl_rate', 'cohort_grad_rate', 'cohort_dropout_rate']
            cols = ['"' + x + '"' for x in cols]
            test_string += ", Null as " + ", Null as ".join(cols)

        select = """
                SELECT ca_pubschls_new."cdscode" as cds_c, {yr} AS year, closeddate, district, zip, fundingtype, charter_authorizer, afilliated_organization, site_type, start_type, 
                financials_{yr}_wide.*, enrollment{yr}_wide.*, {testcolumns} 
                """.format(yr = yr, testcolumns = test_string)

        if yr in ['10', '11', '12', '13', '14', '15']:
            select = select + ' ' + dropout_select
            join = 'LEFT JOIN "dropout_{yr}_wide" ON LEFT("CDS{yr}", 13) = ca_pubschls_new."cdscode"'.format(yr=yr)
            joins = joins + ' ' + join

        string = select + joins + " WHERE charter = TRUE AND opendate <= '{open_cutoff}'".format(open_cutoff=open_cutoff)
 
        if final_string == '':
            final_string = string
        else:
            final_string = final_string + " UNION ALL " + string

    final_string += ';'

    df = pd.read_sql_query(final_string, engine)

    return df

def select_acs():
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    string = """
            SELECT cdscode as cds_c, total_below_poverty, under_5_below_poverty, age_5_below_poverty, age_6_to_11_below_poverty, \
            age_12_to_17_below_poverty, age_18_65_below_poverty, total_above_poverty, under_5_above_poverty, \
            age_5_above_poverty, age_6_to_11_above_poverty, age_12_to_17_above_poverty, age_18_65_above_poverty, year
            FROM acs_complete
            WHERE year != 2000
            ;
            """

    df = pd.read_sql_query(string, engine)
    return df

def select_statement():
    df1 = select_function(['09', '10', '11', '12', '13', '14', '15'])
    df2 = select_function(['04', '05', '06', '07', '08'])

    df3 = pd.concat([df1,df2], ignore_index=True)

    df4 = select_acs()
    #9 rows didn't get filled
    df4.fillna(0, inplace = True)
    df4['year'] = df4['year'] - 2000  
    df4['year'] = df4['year'].astype('int') 

    df4['cds_c'] = df4['cds_c'].astype('int')
    df4['cds_c'] = df4['cds_c'].astype('str')

    df5 = df3.join(df4, how = 'inner', lsuffix = '_idk')
    df5.drop(['cds_c_idk', 'year_idk'], axis = 1, inplace= True)


    return df5
    #return df3



