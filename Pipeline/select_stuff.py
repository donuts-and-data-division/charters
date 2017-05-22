from sqlalchemy import create_engine
import sys
import pandas as pd
import numpy as np
from config import *
from cleaning import *
from model import *
from features import *
from explore import *
from sklearn.cross_validation import train_test_split

def select_statement():
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
            "asian_or_pacific_islander_students_public_school_2012-13", "asian_or_pacific_islander_students_public_school_2011-12",
            "asian_or_pacific_islander_students_public_school_2010-11", "full-time_equivalent_fte_teachers_public_school_2012-13", 
            "full-time_equivalent_fte_teachers_public_school_2011-12", "full-time_equivalent_fte_teachers_public_school_2010-11", 
            "pupil_teacher_ratio_2012-13", "pupil_teacher_ratio_2011-12", "pupil_teacher_ratio_2010-11",
            
            "charter_authorizer", "afilliated_organization", "site_type", "start_type"

        FROM "ca_pubschls_new"
        JOIN "ca_to_nces" ON "ca_pubschls_new"."cdscode" = "ca_to_nces"."CDSCode"
        JOIN "nces_complete" ON "nces_complete"."nces_id" = "ca_to_nces"."NCESCode"
        JOIN "2015-16_AllCACharterSchools_new" ON "2015-16_AllCACharterSchools_new"."cds_code" = "ca_to_nces"."CDSCode"
        ;"""

    df = pd.read_sql_query(string, engine)
    return df

def create_features():

    percentage_features = {'percent_female_2012-13': ['female_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_female_2011-12': ['female_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_female_2010-11': ['female_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13'],

                           'percent_white_2012-13': ['white_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_white_2011-12': ['white_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_white_2010-11': ['white_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13'],

                           'percent_freelunch_2012-13': ['free_and_reduced_lunch_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_freelunch_2011-12': ['free_and_reduced_lunch_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_freelunch_2010-11': ['free_and_reduced_lunch_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13']
                           }

    for feature, columns in percentage_features.items():
        df[feature] = df.apply(lambda row: percentages(row, columns[0], columns[1]), axis=1)


def percentages(row, category_column, total_column):
    if row[category_column] == None:
        return 0
    
    else:
        percent = float(row[category_column]) / float(row[total_column])
        return percent


def pipeline(df):
    explore(df)
    
    df = clean(df)
    X_train, X_test, y_train, y_test = train_test_split(df[FEATURE_COLS], df[OUTCOME_VAR], test_size=TEST_SIZE, random_state=0)
    X_train = feature_eng(X_train)
    X_test = feature_eng(X_test)
    results = classifiers_loop(X_train, X_test, y_train, y_test)
    results.to_csv('results.csv')
    
    return results, y_test


if __name__=="__main__":
    
    df = select_statement()
    create_features()
    results, y_test = pipeline(df)
