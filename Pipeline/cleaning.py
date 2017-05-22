import pandas as pd
import numpy as np
from config import *


def clean(df):
    '''
    Clean df to prepare for modeling
    '''
    df = convert_types(df)

    #create outcome feature column
    df['closedyear'] = df['closeddate'].dt.year
    #df['closedyear'] = df['closedyear'].apply(lambda x: int(x) if np.isnan(x) is False else '')
    
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

    for i in TO_DATETIME:
        df[i] = pd.to_datetime(df[i], errors='ignore')

    return df


