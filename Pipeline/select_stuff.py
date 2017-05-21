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
        SELECT ca_to_nces.CDSCode, closeddate, "Zip", "FundingType", "SOCType", "EILName", "GSoffered", "Latitude", "Longitude"
        FROM ca_pubschls_new
        JOIN ca_to_nces
        ;"""
    df = pd.read_sql_query(string, engine)
    return df

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
    results, y_test = pipeline(df)