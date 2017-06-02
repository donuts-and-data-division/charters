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

def loop_through_models(df):
    pass
    # move everything from below to here

if __name__=="__main__":

    model_opts = get_model_opts()
    feature_opts = get_feature_opts()

    model_opts = {(1,1): {'test_end': dt.datetime(2015, 7, 1, 0, 0),
                  'test_start': dt.datetime(2014, 7, 1, 0, 0),
                  'train_end': dt.datetime(2014, 7, 1, 0, 0),
                  'train_start': dt.datetime(2003, 7, 1, 0, 0)}}
    feature_opts = [['financial', 'cohort', 'school_info', 'spatial', 'academic', 'demographic']]

    try:
        df=pd.read_csv(sys.argv[1], dtype={'cdscode':object,'cds_c':object,'CDSCode':object})
        df['closeddate'] = pd.to_datetime(df['closeddate'])
        df['pit'] = pd.to_datetime(df['pit'])
        print('using csv')
        print('closeddate type ', df.closeddate.dtype)
        print('pit type ', df.pit.dtype)
    except:
        print('building sql query')
        df = select_statement()
        df['key'] = list(zip(df['cds_c'],df['year']))
        df['pit'] = pd.to_datetime(['200'+str(i)+'-07-01' if len(str(i)) == 1 else '20'+str(i)+'-07-01' for i in df['year']])
        df['closeddate'] = pd.to_datetime(df['closeddate'])
        df['closeddate'].fillna(inplace=True, value=dt.datetime(2200,7,1))
        df.to_csv('queryresults.csv')    

   
    results_list = []
    for key, val in model_opts.items():
        for feat in feature_opts:
            base = ['year', 'pit', 'closeddate', 'district']
            financial = []
            cohort = []
            demographic = []
            school_info = []
            academic = []
            spatial = []
            for i in feat:
                if i == 'financial': 
                    financial = FINANCIAL_COLS
                if i == 'cohort':
                    cohort = COHORT_COLS
                if i == 'school_info':
                    school_info = SCHOOL_INFO_COLS
                if i == 'spatial':
                    spatial = []
                if i == 'academic':
                    academic = ACADEMIC_COLS
                if i == 'demographic':
                    demographic = DEMO_COLS

            relevant_cols = base + financial + cohort + school_info + spatial + demographic + academic

            train_start = val['train_start']
            train_end = val['train_end']
            test_start = val['test_start']
            test_end = val['test_end']
            closed_within = key[1]

            train_start_yr = int(str(train_start.year)[-2:])
            train_end_yr = int(str(train_end.year)[-2:])
            test_start_yr = int(str(test_start.year)[-2:])
            test_end_yr = int(str(test_end.year)[-2:])

            outcome_header = "closed_within_{}_years".format(str(closed_within))
            X_train = df[relevant_cols].loc[(df['year'] > train_start_yr) & (df['year'] <= train_end_yr)]
            y_train = (X_train['closeddate'] - X_train['pit'] <= np.timedelta64(closed_within, 'Y')) & (X_train['closeddate'] - X_train['pit'] >= np.timedelta64(0, 'D'))
            
            X_test = df[relevant_cols].loc[(df['year'] > test_start_yr) & (df['year'] <= test_end_yr)]
            y_test = (X_test['closeddate'] - X_test['pit'] <= np.timedelta64(closed_within, 'Y')) & (X_test['closeddate'] - X_test['pit'] >= np.timedelta64(0, 'D'))
            
            
            print('\n\n NEXT MODEL')
            print('test_start: ', test_start_yr, 'train_end: ', train_end_yr, 'train_start: ', train_start_yr, 'test_end: ', test_end_yr)
            print(key, feat)
            X_train = clean(X_train, feat)
            X_test = clean(X_test, feat, X_train.columns)

            #X_train = feature_eng(X_train, feat)
            #X_test = feature_eng(X_test, feat)
            print("X_test null columns, ", X_test.columns[X_test.isnull().any()].tolist()) 
            print("X_train null columns, ", X_train.columns[X_train.isnull().any()].tolist())
            print("Y_test null columns, ", y_train.isnull().sum()) 
            print("Y_train null columns, ", y_test.isnull().sum())

            baseline = float(y_test[y_test == 1].value_counts()/ y_test.shape[0])
            results = classifiers_loop(X_train, X_test, y_train, y_test, val, feat, baseline)
            results_list.append(results)
            
            
    final_results = pd.concat(results_list, axis=0)
    final_results.to_csv('results.csv')            
    
    #final_results.to_csv(f, header=False)