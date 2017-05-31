from sqlalchemy import create_engine
import sys
import pandas as pd
import numpy as np
from config import *
from select_stuff import *
from explore import *
from cleaning import *
from features import *
from model import *
from sklearn.cross_validation import train_test_split
import datetime as dt
from dateutil.relativedelta import relativedelta


def get_model_opts():
    '''
    Returns dictionary where keys are tuples pairs: (test window # of years, closed within # of years)
    and values are dictionaries whose keys are train_start, train_end, test_start, test_end
    '''
    d = {}
    for test_window in [1, 2]: 
        for closed_within in [1, 2, 3]:
            train_start = dt.datetime(2003, 7, 1, 0, 0)
            end_of_time = dt.datetime(2016, 7, 1, 0, 0)
            test_end = end_of_time - relativedelta(years=closed_within)
            test_start = test_end - relativedelta(years=test_window)
            train_end = test_start
            d[(test_window, closed_within)] = {'train_start': train_start, 'train_end': train_end, 'test_start': test_start, 'test_end': test_end}
    return d

def get_feature_opts():
    '''
    Returns list of lists of feature groups
    '''
    feature_groups =['financial', 'cohort', 'demographic', 'school_info', 'spatial', 'academic']
    feature_opts = [feature_groups]
    for group in feature_groups:
        feature_opts.append([group]) 
        feature_opts.append([i for i in feature_groups if i != group])
    return feature_opts

def get_financial_columns():
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    string = ' SELECT "Object" FROM "Objects_full" '
    df = pd.read_sql_query(string, engine)

    return df['Object'].tolist()

if __name__=="__main__":
    
    model_opts = get_model_opts()
    feature_opts = get_feature_opts()
    financial_columns = get_financial_columns()
    
    for key, val in model_opts.items():
        for f in feature_opts:
            train_start = val['train_start']
            train_end = val['train_end']
            test_start = val['test_start']
            test_end = val['test_end']
            
            df = select_statement(train_start, train_end, f)
            #X_train, y_train = select_statement(train_start, train_end, f)
            #X_test, y_test = select_statement(test_start, test_end, f)

            #X_train = clean(X_train)
            #X_test = clean(X_test)

            #X_train = feature_eng(X_train)
            #X_test = feature_eng(X_test)

            #results = classifiers_loop(X_train, X_test, y_train, y_test)
            #results.to_csv('results.csv')
    
   