import pandas as pd
import numpy as np
from config import *
from select_stuff import *
from pipeline import *

def clean(df, features, train_cols = None):
    '''
    Clean df to prepare for modeling
    '''
    #df = convert_types(df)
    #df = setup_outcome(df)
    #df = replace_none(df)

    
    if 'financial' in features:
        df = financial_features(df)
    if 'school_info' in features:
        #print('working on school info')
        df = school_info_features(df)
    if 'demographic' in features:
        df = demographic_features(df)
    if 'cohort' in features:
        df = cohort_features(df)
    if 'spatial' in features:
        df = spatial_features(df)
    if 'academic' in features:
        df = academic_features(df)

    df = fill_missing(df)
    
    x=[]
    if train_cols is not None:
         x = [i for i in df.columns if i not in train_cols]
    df = df.drop(['year', 'pit', 'closeddate', 'district']+x, axis=1)
    
    return df

### Helper functions that feed into clean() ###


def financial_features(df):
    financial = FINANCIAL_COLS
    df = replace_none(df, REP_NONE = financial, fill = 0)
    df['tot_spend'] = df[financial].sum(axis=1)
    for i in financial: # to ignore CSDcode
        df[i].fillna(value=0.0, inplace=True)
        #df['perc_'+i] = 0.0 
        #df[df['tot_spend']!=0]['perc_'+i] = df[i]/df['tot_spend']
    #df = df.drop(['CDSCode'], axis=1)
    
    return df


def school_info_features(df):

    df = replace_none(df, REP_NONE=SCHOOL_INFO_COLS, fill="Unknown category") 

    df = pd.get_dummies(df, columns=SCHOOL_INFO_COLS)
    return df

def demographic_features(df):
    demographic = DEMO_COLS
    df[demographic].fillna(value=0.0, inplace=True)
    df['tot_enrollment'] = df[demographic].sum(axis=1)
    for i in demographic: # to ignore CSDcode and index
        df['perc_'+i] = 0.0 
        df.loc[df['tot_enrollment']!=0, 'perc_'+i] = df[i]/df['tot_enrollment']
    #df = df.drop(['a','cds_code'], axis=1)
    return df

def cohort_features(df):
    '''
    cohort_cols = ['ged_rate', 'special_ed_compl_rate', 'cohort_grad_rate', 'cohort_dropout_rate']
    for i in cohort_cols:
        df['deciles_'+i] = pd.qcut(df[i], q=10)
        avg = df[i].mean
        df['avg_compare_'+i] = ['Above avg' if df[i]> avg else 'Below avg' for i in df[i]]
        df = make_dummies(df[`'avg_compare_'+i], axis=1)
    '''

    #compare rates to district averages
    #create new columns - 1 if above average, 0 if below
    for col in COHORT_COLS:
        
        df[col].fillna(0, inplace=True) #fill missing values with category 0

        means = df.groupby('district')[col].mean()
        new_name = col + "_means_compare"
        means.name = new_name
        df = df.join(means, on='district')

        df['diff'] = df[col] - df[new_name]

        df.set_value(df['diff'] > 0, new_name, \
        value=1)
        df.set_value(df['diff'] < 0, new_name, \
        value=0)

    df = df.drop(['diff'], axis=1)
    return df

def spatial_features(df):
    return df

def academic_features(df):
   
    academic = ACADEMIC_COLS
    empty_columns = df[academic].columns[df[academic].isnull().all()]
    non_empty_columns = df[academic].columns[~df[academic].isnull().all()]
    df[empty_columns] = 0
    df[non_empty_columns].fillna(value=df[non_empty_columns].mean(axis=1), inplace=True)
    """
    for i in academic: 
        avg = df[i].mean
        df['avg_compare_'+i] = ['Above avg' if df[i] > avg else 'Below avg' for i in df[i]] # getting a key error 0!
        df = pd.get_dummies(df, ['avg_compare_'+i]) 
        df = df.drop([i],axis=1)
    """
    return df

def convert_types(df):
    '''
    Deal with columns requiring data type conversions
    '''
    for i in TO_INT:    
        df[i] = df[i].astype('float')
    
    for i in TO_FLOAT:    
        df[i] = df[i].astype('int')

    for i in TO_STR:    
        df[i] = df[i].astype('str')

    for i in TO_BOOL:
        df[i] = df[i].astype(np.bool)
    
    return df
'''
def setup_outcome(df):
    df["closedyear"] = pd.to_datetime(df.closeddate, errors = "raise").dt.year.astype(str)
    df = get_dummies(df["closedyear"],auxdf = df, prefix = "closed")

    return df
'''

def create_percentages(df):
    '''
    Convert counts to percentages
    '''
    for feature, columns in PERCENTAGE_FEATURES.items():
        df[feature] = df.apply(lambda row: percentages(row, columns[0], columns[1]), axis=1)
    return df

def percentages(row, category_column, total_column):
    '''
    Helper function for create_percentages
    '''
    if row[category_column] == None:
        return 0
    
    else:
        percent = float(row[category_column]) / float(row[total_column])
        return percent



def get_dummies(data,auxdf=None, prefix=None, prefix_sep='_', dummy_na=False, columns=None, sparse=False, drop_first=False):
    '''
    convert categorical values (set of k) to dummy variables (in k columns)

    inputs:
        data (array-like)
        auxdf (dataFrame)
        other args (built on top of pandas get_dummies see pandas docummentation for more detail)

    returns:
        array-like object
    '''
    dummies = pd.get_dummies(data, prefix=prefix, prefix_sep='_', dummy_na=False, columns=None, sparse=False, drop_first=False).astype(np.int8)
    if isinstance(auxdf, pd.DataFrame):    
        df = pd.concat([auxdf, dummies],axis=1)
        return df
    return dummies



def replace_none(df, REP_NONE=REP_NONE, fill="Unknown category"):
    for colname in REP_NONE:
        try:
            #print("Replacing None in ", colname)
            df[colname].fillna(value=fill, inplace=True)
        except:
            pass

       
    return df


def make_dummies(df, cols):
    '''
    Function to make dummy features from categorical variables and concatenate with df
    '''
    '''
    for c in cols:
        print('making dummies for ', c)
        dummies = pd.get_dummies(df[c], prefix = c)
        print(dummies)
        df = pd.concat([df, dummies], axis = 1)
    '''
    print(cols)
    dummies = pd.get_dummies(df, [cols])
    df = pd.concat([df, dummies], axis=1)
    return df


def fill_missing(df):
    '''
    Function to fill null values in df with:
        median (if integer)
        mean (if float)
        mode (if string)
    Do this imputation after training/test split occurs.
    '''   
    for colname in df:    
        if 'int' in str(df[colname].dtype):
            df[colname].fillna(value=df[colname].median(), inplace=True)
        elif 'float' in str(df[colname].dtype):
            df[colname].fillna(value=df[colname].mean(), inplace=True)
        elif df[colname].dtype == 'object':
            try: # see if mode exists
                mode = df[colname].mode()[0]
                df[colname].fillna(value=mode, inplace=True)
            except: # if no mode, fill with 'unknown'
                df[colname].fillna(value='Unknown', inplace=True)
        else:
            #sys.exit('check irregular data types')
            pass
    return df



