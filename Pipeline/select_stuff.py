from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from db_config import *
import psycopg2
import datetime as dt

feature_years = {
    'tests': [x for x in range(2004, 2016) if x != 2014],
    'dropouts': [x for x in range(10, 16)],
    'enrollment': [x for x in range(2004, 2016)],
    'financial': [x for x in range(2004, 2016)]
    }


def select_function(year_list):
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)
    #year_list = []
    asnull = ''

    #for yr in range(2004, 2016):
        #year_list.append(str(yr)[-2:])

    final_string = ''
    #for yr in year_list:
    #for yr in ['09', '10', '11', '12', '13', '14', '15']:
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

        #select = """
              #  SELECT ca_pubschls_new."cdscode" as cds_code, {yr} AS year, closeddate, district, zip, fundingtype, charter_authorizer, afilliated_organization, site_type, start_type, 
               # financials_{yr}_wide.*, {testcolumns}
               # """.format(yr = yr, testcolumns = test_string)

        dropout_select = """
                        , "GED Rate{yr}_AllAll" as ged_rate, "Special Ed Completers Rate{yr}_AllAll" as special_ed_compl_rate, 
                        "Cohort Graduation Rate{yr}_AllAll" as cohort_grad_rate, "Cohort Dropout Rate{yr}_AllAll" as cohort_dropout_rate
                        """.format(yr=yr)

        if yr not in ['10', '11', '12', '13', '14', '15']:
            #dropout_select = ', Null as ged_rate, Null as special_ed_compl_rate, Null as cohort_grad_rate, Null as cohort_dropout_rate'
            #dropout_select = ', 0 as ged_rate, 0 as special_ed_compl_rate, 0 as cohort_grad_rate, 0 as cohort_dropout_rate'
            cols = ['ged_rate', 'special_ed_compl_rate', 'cohort_grad_rate', 'cohort_dropout_rate']
            cols = ['"' + x + '"' for x in cols]
            test_string += ", Null as " + ", Null as ".join(cols)

            #select = select + ' ' + dropout_select

        select = """
                SELECT ca_pubschls_new."cdscode" as cds_c, {yr} AS year, closeddate, district, zip, fundingtype, charter_authorizer, afilliated_organization, site_type, start_type, 
                financials_{yr}_wide.*, enrollment{yr}_wide.*, {testcolumns} 
                """.format(yr = yr, testcolumns = test_string)


        if yr in ['10', '11', '12', '13', '14', '15']:
            select = select + ' ' + dropout_select
            join = 'LEFT JOIN "dropout_{yr}_wide" ON dropout_{yr}_wide."CDS{yr}" = ca_pubschls_new."cdscode"'.format(yr=yr)
            joins = joins + ' ' + join

        string = select + joins + " WHERE charter = TRUE AND opendate <= '{open_cutoff}'".format(open_cutoff=open_cutoff)

        #print(string)
        #return None
        df = pd.read_sql_query(string, engine)
        print(df.shape)
        #return df


        if final_string == '':
            final_string = string
        else:
            final_string = final_string + " UNION ALL " + string

        #print(yr)

    final_string += ';'
    print(final_string)
    df = pd.read_sql_query(final_string, engine)

    return df

def select_statement():
    df1 = select_function(['09', '10', '11', '12', '13', '14', '15'])
    df2 = select_function(['04', '05', '06', '07', '08'])

    df3 = pd.concat([df1,df2])
    return df3

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
'''
# this works too; it wasn't a pandas vs psycopg2 issue
def get_feature_group_columns(table_name):
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)
    string = """
        SELECT column_name FROM information_schema.columns WHERE table_name = {table_name}
        """.format(table_name=table_name)
    df = pd.read_sql_query(string, engine)

    return df["column_name"].tolist()
'''


'''
def select_statement(start_date, end_date, feature_groups):
    
    Inputs
        start_date: start date of period
        end_date: end date of period
        reature_groups: list of feature groups
    Outputs
        df: dataframe with columns relevant to dates and feature_groups
    
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    start_year = start_date.year
    end_year = end_date.year
    year_range = range(start_year+1, end_year+1)
    year_list = []
    for yr in year_range:
        year_list.append(str(yr)[-2:])

    final_string = ''
    for yr in year_list:
        # set up substrings depending on what feature groups need to be selected
        fin_select = ' '
        cohort_select = ' '
        dem_select = ' '
        schinfo_select = ' '
        spat_select = ' '
        acad_select = ' '

        fin_join = ' '
        cohort_join = ' '
        dem_join = ' '
        schinfo_join = ' '
        spat_join = ' '
        acad_join = ' '

        if 'financial' in feature_groups:
            
            fin_select += ', financials_{yr}_wide.*'.format(yr=yr) 
            fin_join += ' LEFT JOIN financials_{yr}_wide ON financials_{yr}_wide."CDSCode" = ca_pubschls_new."cdscode" '.format(yr=yr)
        
        if 'cohort' in feature_groups:
            cohort_select = ' COL, COL, COL '
            cohort_join = """ LEFT JOIN ...
                ON 
                 """
        if 'demographic' in feature_groups:
            dem_select = ' COL, COL, COL '
            dem_join = """ LEFT JOIN ...
                ON
                 """
        if 'school_info' in feature_groups:
            schinfo_select = """ "opendate", "zip", "fundingtype", "soctype", "eilname", "gsoffered", "latitude", "longitude" """  
            schinfo_join = """ LEFT JOIN "2015-16_AllCACharterSchools_new" 
                ON "2015-16_AllCACharterSchools_new"."cds_code" = "ca_pubschls_new"."cdscode" 
                """
        if 'spatial' in feature_groups:
            spat_select = ' COL, COL, COL '
            spat_join = """ LEFT JOIN ...
                ON 
                 """
        if 'academic' in feature_groups:
            acad_select = ' COL, COL, COL '
            acad_join = """ LEFT JOIN ...
                ON
                 """
        

        # construct overall query      
        string = (
        ' SELECT "cdscode" '
        + fin_select
        + cohort_select
        + dem_select
        + schinfo_select
        + spat_select
        + acad_select 
        + ' FROM "ca_pubschls_new" '
        + fin_join
        + cohort_join
        + dem_join
        + schinfo_join
        + spat_join
        + acad_join
        )

        if final_string == '':
            final_string = string
        else:
            final_string = final_string + " UNION ALL " + string
    
    return final_string + ';'
    #df = pd.read_sql_query(final_string, engine)

    #return df, y
    

# Not using select statement below because no longer using NCES data source due to too many non-matched schools

def select_statement():
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    string = """

        SELECT ca_to_nces."CDSCode", ca_to_nces."NCESCode", "closeddate", "opendate", "zip", "fundingtype", "soctype", "eilname", "gsoffered", "latitude", "longitude",
               
            "school_level_code_public_school_2012-13", "school_level_code_public_school_2011-12", "school_level_code_public_school_2010-11",
            "total_students_all_grades_excludes_ae_public_school_2012-13", "total_students_all_grades_excludes_ae_public_school_2011-12",
            "total_students_all_grades_excludes_ae_public_school_2010-11", "free_and_reduced_lunch_students_public_school_2012-13",
            "free_and_reduced_lunch_students_public_school_2011-12", "free_and_reduced_lunch_students_public_school_2010-11",
            "male_students_public_school_2012-13", "male_students_public_school_2011-12", "male_students_public_school_2010-11",
            "female_students_public_school_2012-13", "female_students_public_school_2011-12", "female_students_public_school_2010-11",
            "hispanic_students_public_school_2012-13", "hispanic_students_public_school_2011-12", "hispanic_students_public_school_2010-11",
            "black_students_public_school_2012-13", "black_students_public_school_2011-12", "black_students_public_school_2010-11",
            "white_students_public_school_2012-13", "white_students_public_school_2011-12", "white_students_public_school_2010-11",
            "asian_or_pacific_islander_students_public_school_2012-13", "asian_or_pacific_islander_students_public_school_2011-12",
            "asian_or_pacific_islander_students_public_school_2010-11", "full-time_equivalent_fte_teachers_public_school_2012-13", 
            "full-time_equivalent_fte_teachers_public_school_2011-12", "full-time_equivalent_fte_teachers_public_school_2010-11", 
            "pupil_teacher_ratio_2012-13", "pupil_teacher_ratio_2011-12",
            
            "charter_authorizer", "afilliated_organization", "site_type", "start_type", "located_within_district"

        FROM "ca_to_nces" 
        LEFT JOIN "ca_pubschls_new" ON "ca_pubschls_new"."cdscode" = "ca_to_nces"."CDSCode"
        LEFT JOIN "nces_complete" ON "nces_complete"."nces_id" = "ca_to_nces"."NCESCode"
        LEFT JOIN "2015-16_AllCACharterSchools_new" ON "2015-16_AllCACharterSchools_new"."cds_code" = "ca_to_nces"."CDSCode"
        ;"""

    df = pd.read_sql_query(string, engine)
    return df
'''


