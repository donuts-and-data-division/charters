from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from db_config import *


def select_statement():
    #need to rename "pupil/teacher_ratio_public_school_2012-13" column in order to select it i dont think it likes that slash

    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    string = """

        SELECT ca_to_nces."CDSCode", ca_to_nces."NCESCode", "closeddate", "zip", "fundingtype", "soctype", "eilname", "gsoffered", "latitude", "longitude",
               
            "school_level_code_public_school_2012-13", "school_level_code_public_school_2011-12", "school_level_code_public_school_2010-11",
            "total_students_all_grades_excludes_ae_public_school_2012-13", "total_students_all_grades_excludes_ae_public_school_2011-12",
            "total_students_all_grades_excludes_ae_public_school_2010-11", "free_and_reduced_lunch_students_public_school_2012-13",
            "free_and_reduced_lunch_students_public_school_2011-12", "free_and_reduced_lunch_students_public_school_2010-11",
            "male_students_public_school_2012-13", "male_students_public_school_2011-12", "male_students_public_school_2010-11",
            "female_students_public_school_2012-13", "female_students_public_school_2011-12", "female_students_public_school_2010-11",
            "hispanic_students_public_school_2012-13", "hispanic_students_public_school_2011-12", "hispanic_students_public_school_2010-11",
            "black_students_public_school_2012-13", "black_students_public_school_2011-12", "black_students_public_school_2010-11",
            "white_students_public_school_2012-13", "white_students_public_school_2011-12", "white_students_public_school_2010-11",
            "full-time_equivalent_fte_teachers_public_school_2012-13", "full-time_equivalent_fte_teachers_public_school_2011-12",
            "full-time_equivalent_fte_teachers_public_school_2010-11", 
            
            "charter_authorizer", "afilliated_organization", "site_type", "start_type"

        FROM "ca_pubschls_new"
        JOIN "ca_to_nces" ON "ca_pubschls_new"."cdscode" = "ca_to_nces"."CDSCode"
        JOIN "nces_complete" ON "nces_complete"."nces_id" = "ca_to_nces"."NCESCode"
        JOIN "2015-16_AllCACharterSchools_new" ON "2015-16_AllCACharterSchools_new"."cds_code" = "ca_to_nces"."CDSCode"
        ;"""

    df = pd.read_sql_query(string, engine)
    return df



