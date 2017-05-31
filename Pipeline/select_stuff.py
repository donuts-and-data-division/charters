from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from db_config import *



def select_statement(start_date, end_date, feature_groups):
    '''
    Inputs
        start_date: start date of period
        end_date: end date of period
        reature_groups: list of feature groups
    Outputs
        df: dataframe with columns relevant to dates and feature_groups
    '''
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
            
                fin_select = ', financials_{yr}_wide.*'.format(yr=yr) 
                fin_join += ' LEFT JOIN financials_{yr}_wide ON financials_{yr}_wide."CDSCode" = ca_pubschls_new."cdscode" '.format(yr=yr)
        '''
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
            schinfo_select = """ "zip", "fundingtype", "soctype", "eilname", "gsoffered", "latitude", "longitude" """  
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
        '''

        # construct overall query      
        string = (
        ' SELECT "closeddate", "opendate" '
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
        + ';'
        )

        final_string = final_string + " UNION ALL " + string
    
    print(final_string)
    df = pd.read_sql_query(final_string, engine)

    return df, y
    

# Not using select statement below because no longer using NCES data source due to too many non-matched schools
'''
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


