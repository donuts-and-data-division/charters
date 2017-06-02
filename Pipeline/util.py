import pandas as pd
import re
from random import sample
import psycopg2
from db_config  import *

# Helper Functions
def check_nulls(df, col):
    '''
    returns df with NaN in specified column(s)
    '''
    return df.ix[df.ix[:,col].isnull()]

def get_notnulls(df, col):
    '''
    returns df without NaN in specified column(s)
    '''
    return df.ix[df.ix[:,col].notnull()]


def clean_data(df, cleaning_tuples):
    '''
    replace a string in a column (pat) with a clean string (repl):
    e.g. cleaning_tuples = [(col, pat, repl)]
    '''
    for col, pat, repl in cleaning_tuples:
        df.ix[:,col] = df.ix[:,col].str.replace(pat, repl)
        
def clean_grouped_data(grouped_df,col=0):
    '''
    returns df with counts that result from groupby
    '''
    counts = pd.DataFrame(grouped_df.count().ix[:,col])
    counts = counts.unstack()
    counts.columns = counts.columns.droplevel()
    counts.columns.name = None
    counts.index.name = None
    counts.fillna(0, inplace=True)
    return counts

def combine_cols(df, col, extra):
    '''
    Inputs:
        df (pd.DataFrame)
        col,extra (string) column names
    
    Combines columns with similar information into a single column and drops extra.
    '''
    df.ix[:,col] = df.ix[:,col].where(df.ix[:,col].notnull(), df.ix[:,extra])
    df.drop(extra, axis=1, inplace=True)



def get_subsample(x, n, method = "sample"):
    '''
    input:
        x (array-like)
        n (numeric) sample size
        method keywods that determine how to subsample ("sample", "head") 
    '''

    if n > len(x):
        return "ERROR: n > len(x)"
    #Look into ways of passing part of a function name to call the function?
    # e.g. pass sample, do pd.DataFrame.sample(df, n)
    if method == "sample":
        return x.sample(n)
    elif method == "head":
        return x.head(n)

def camel_to_snake(column_name):
    """
    converts a string that is camelCase into snake_case
    Example:
        print camel_to_snake("javaLovesCamelCase")
        > java_loves_camel_case
    See Also:
        http://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', column_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def map_camel_to_snake(s):
    '''
    Converts a series of strings in camelCase to snake_case
    '''
    return s.map(camel_to_snake)


def get_feature_group_columns(table_name):
    '''
    Returns a list of column names for given table
    '''
    conn = psycopg2.connect("dbname={} user={} host={} password={}".format(DATABASE, USER, HOST, PASSWORD))
    cur = conn.cursor()
    string = """
        SELECT column_name FROM information_schema.columns WHERE table_name = '{}'
        ;""".format(table_name)
    cur.execute(string)
    ls = []
    for record in cur:
        ls.append(record[0])
    return ls

def remove_item(lst, item):
    return [l for l in lst if l not in item]
