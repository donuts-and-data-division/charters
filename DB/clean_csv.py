import pandas as pd
import regex as re

def clean():
    '''Fixes column names and fills missing data with None'''

    filepaths = ['/home/student/charters/Data/nces_download1.csv', 
        '/home/student/charters/Data/nces_download2.csv', '/home/student/charters/Data/nces_download3.csv', 
        '/home/student/charters/Data/2015-16_AllCACharterSchools.csv']

    for filepath in filepaths:

        df = pd.read_csv(filepath, encoding = "ISO-8859-1") 

        #replace weird characters
        df.replace('ï¾\x86', np.nan, inplace=True)
        df.replace('ï¾\x96', np.nan, inplace=True)  

        #remove brackets/parentheses and replace spaces with underscores
        for column in df:
            df.columns = df.columns.str.replace('[', '')
            df.columns = df.columns.str.replace(']', '')
            df.columns = df.columns.str.replace('(', '')
            df.columns = df.columns.str.replace(')', '')
            df.columns = df.columns.str.strip().str.replace(' ', '_')
            df.columns = df.columns.str.lower()
        
        #fill nans with none for postgres
        df1 = df.where((pd.notnull(df)), None)

        reg = re.findall('Data/(.*).csv$', filepath)
        filename = reg[0] + '_new.csv'

        df1.to_csv(filename)