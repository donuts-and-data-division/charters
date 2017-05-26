import pandas as pd
import regex as re
import numpy as np
from os import system
import time
from Pipeline.util import camel_to_snake

DATABASE = "postgresql://capp30254_project1_user@pg.rcc.uchicago.edu:5432/capp30254_project1"
BASEDIR = None
VERBOSE = True
WEIRD_CHARS ='\*|ï¾\x86|ï¾\x96|\xf1'

def clean(filepaths, outnames="out.csv", ending = "_new.csv",basedir = BASEDIR):
    '''Fixes column names and removes non-standard characters (including *)'''
    if isinstance(filepaths, str):
        filepaths = [filepaths]
    if isinstance(outnames, str):
        outnames = [outnames]

    if len(outname)!=len(filepaths):
        raise "outnames and filepaths should be same length: generating outnames"
        outnames = [rename_csv(f, ending) for f in filepaths]
    
    for i, filepath in enumerate(filepaths):
        with open(outnames[i], "w") as outfile, open(filepath, 'r') as content:
            out =  re.sub('\)|\(|\[|\]','', content.readline().replace(' ','_').lower())
            outfile.write(out)
            for c in content.readlines():
                out = re.sub(WEIRD_CHARS,'', c)
                outfile.write(out)
        content.close()
        outfile.close()
    return True
    '''
        try:
            df = pd.read_csv(filepath, encoding = "ISO-8859-1", low_memory=False) 
        except:
            df = pd.read_csv(filepath, low_memory=False) 

        if verbose:
            print("cleaning the characters")

        try: 
            df.replace('ï¾\x86', [None], inplace=True) 
        except: 
            pass
        try: 
            df.replace('ï¾\x96', [None], inplace=True) 
        except: 
            pass
        try: 
            df.replace(u'\xf1', 'n', inplace=True) 
        except: 
            pass    
        df.replace(['*'], [None], inplace=True) 
        #remove brackets/parentheses and replace spaces with underscores
        
        if verbose:
            print("changing column names")
        for column in df:
            df.columns = df.columns.str.replace('[', '')
            df.columns = df.columns.str.replace(']', '')
            df.columns = df.columns.str.replace('(', '')
            df.columns = df.columns.str.replace(')', '')
            df.columns = df.columns.str.strip().str.replace(' ', '_')
            df.columns = df.columns.str.lower()


        if verbose:
            print("filling nas")
       
        #fill nans with none for postgres
        df = df.where((pd.notnull(df)), None)
        
        filename = rename_csv(filepath,ending)

        df.to_csv(filename, index=False)
    '''
    #ISSUE FILE IN CURRENT DIRECTORY. 


def load(filepaths, alter_cols=[], ending="_new.csv",
            db = DATABASE ):
    if verbose:
        start = time.time()
        print("start time: ", start)
    
    clean(filepaths, ending, basedir)

    if verbose:
        clean_time = time.time() 
        print("clean time: ", clean_time - start)

    if isinstance(filepaths, str):
        filepaths = [filepaths]
    
    for f in filepaths:

        f = rename_csv(f)
        schema = make_schema(f)
        print(schema)

        try:
            system("""psql --db {} -c {}""".format(db, schema))
            return "table up"
        except:
            return "oh no the table was not created" 

        if verbose:
            print("initiate run sequence: inserting {}".format(f))
        
        system("""psql --db {}
                 -c '\copy {} from {} with csv header' """.format(db, f))
         
        if verbose:
            write_time = time.time() 
            print("write time: ", write_time - clean_time)


def make_schema(filepath, type_dict = None):
    schema = "DROP TABLE IF EXISTS {}; \n".format(filepath[:-4])
    system("""head -n 1000 {} | csvsql -postgresql > temp.txt""".format(filepath))
    f = open("temp.txt")
    for line in f.readlines():
        col = re.findall('(\w*) VARCHAR', line)
        try:
            datatype = type_dict[col]
            schema += "\t{} {}, \n".format(col, datatype)
        except:
            schema += line
    schema = schema[:-4] + ");"
    return schema

def create_table(schema, db = DATABASE):
    try:
        system('''psql --db {} -f {}'''.schema(db, schema))
        return True
    except:
        return False

def rename_csv(f, new_ending=None):
    '''Takes filepath strips the path and ending and adds a new_ending' (e.g. "_new.csv"'''
    reg = re.findall('/?(.*).csv$', f)
    f = reg[0] + new_ending
    return f



if __name__=="__main__":
    # RUN FROM charters director ... ipython3 -i DB/clean_csv
    emmas_filepaths = ['/home/student/charters/Data/nces_download1.csv', 
        '/home/student/charters/Data/nces_download2.csv', '/home/student/charters/Data/nces_download3.csv', 
        '/home/student/charters/Data/2015-16_AllCACharterSchools.csv']
    tests_filepaths = ["Data/CATestData/ca2011_all.csv",
                "Data/CATestData/ca2012_all.csv",
                "Data/CATestData/ca2013_all.csv"]
    filepath = "ca2012_all.csv"
    alter_cols = ["county_code","district_code","school_code"]
    #clean(filepath)
    #Schema = make_schema(rename_csv(filepath, "_new.csv"))

    Schema = make_schema("out.csv")



    