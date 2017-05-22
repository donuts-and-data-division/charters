import pandas as pd
import numpy as np
from config import *


def clean(df):
    '''
    Clean df to prepare for modeling
    '''
    df = convert_types(df)
    #df = setup_outcome(df)
    #create outcome feature column
    #df['closedyear'] = df['closeddate'].dt.year
    #df['closedyear'] = df['closedyear'].astype(str)
    df = create_percentages(df)
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

def setup_outcome(df):
    df['closedyear'] = 'Not closed'

    open_schools = df.ix[df.ix[:,'closeddate'].isnull()]
    df['closedyear'] = pd.to_datetime(df['closeddate'][open_schools], errors='raise').dt.year
        #df['closedyear'] = df['closedyear'].apply(lambda x: int(x))
    # NOT WORKING!!!

    return df

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
