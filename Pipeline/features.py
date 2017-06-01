import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize
from sklearn.preprocessing import LabelEncoder as le
from config import *
from select_stuff import *
import sys

def feature_eng(df):
    #df = fill_missing(df)
    df = cap_extreme(df)
    df = discretize(df)
    df = normalize(df)
    df = label_encode(df)

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

def cap_extreme(df):
    '''
    Function to cap extreme columns at a certain percentile
    '''
    for c in EXTREME_COLS:
        ceiling = df[c].quantile(CAP)
        df[c] = df[c].apply(lambda x: set_ceiling(x, ceiling))
    return df

def set_ceiling(x, ceiling):
    '''
    Helper function for cap_extreme
    '''
    if x > ceiling:
        return ceiling
    else:
        return x

def discretize(df):
    '''
    Function for discretizing continuous variables into Q equally-sized buckets     
    '''

    #delete all of this - it's in cleaning now
    for col in ['ged_rate', 'special_ed_compl_rate', 'cohort_grad_rate', 'cohort_dropout_rate']:
        
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


def prev_year_difference(df):

    testing_cols = get_feature_group_columns('catests_2015_wide')


    #for year in [x for x in range(4,16)]:
    for year in [4]:

        for col in testing_cols:
            print(year, col)
            
            new_column_name = col + '_prev_year_difference'
            if year != 3:
                difference = df[df['year'] == year][col] - df[df['year'] == (year-1)][col]
                df.set_value(df['year'] == year, new_column_name, value = difference)

            else:
                print('year 3')
                df.set_value(df['year'] == year, new_column_name, value = 0)

    return df


def normalize(df):
    '''
    Function for normalizing dataframe
    '''
    if NORMALIZE:
        normalize(df, axis=1)
    return df

def make_dummies(df, cols):
    '''
    Function to make dummy features from categorical variables and concatenate with df
    '''
    for c in cols:
        dummies = pd.get_dummies(df[c], prefix = c)
        print(dummies)
        df = pd.concat([df, dummies], axis = 1)
    return df

def label_encode(df):
    for c in LABEL_ENCODE:
        le = preprocessing.LabelEncoder()
        le.fit(df[c])
        df[c] = le.transform(df[c])
    
    return df
    

