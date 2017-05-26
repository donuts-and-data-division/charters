import pandas as pd
import re
import numpy as np
from os import system
import time
import sys, tempfile, os
from subprocess import call

#EDITOR = os.environ.get('EDITOR','vim') 
DATABASE = "postgresql://capp30254_project1_user@pg.rcc.uchicago.edu:5432/capp30254_project1"
BASEDIR = None
VERBOSE = True
WEIRD_CHARS ='\*|ï¾\x86|ï¾\x96|\xf1'
# For columns with critical typing force the type (note the camel case headings are generated in cleaning):
# csvsql automatically types columns and will fail frequently.  
TYPE_DICT = {"county_code": "VARCHAR","district_code":"VARCHAR","charter_number":"VARCHAR"}
FILEPATHS = "/Users/arianisfeld/MachineLearning/CATestData/ca2012_all.csv"
# OPTIONAL: Each file in filepath will be cleaned and a new file will be created. 
# The outname (minus the ".csv" will become the table name in the DB)
OUTNAMES = "out.csv" 
# OTHERWISE: each new file will use the filepath name with an ending appended
ENDING = "_new.csv"

def load(filepaths=FILEPATHS, outnames=OUTNAMES, ending=ENDING, db = DATABASE):

    # Notice outnames may be specified or generated with an ending
    filepaths, outnames = standardize_paths(filepaths, outnames, ending)
    
    if VERBOSE:
        start = time.time()
        print("start time: ", start)
    
    clean(filepaths, outnames)

    if VERBOSE:
        clean_time = time.time() 
        print("clean time: ", clean_time - start)
    
    for f in outnames:

        schema = make_schema(f)
        print(schema)

        create_table(schema, db)
    
        try:    
            if VERBOSE:
                print("initiate copy: {}".format(f))
            
            system("""psql --db {} -c '\copy {} from {} with csv header' """.format(db, f))
        
        except psycopg2.Error as e:
            print(e.diag.message_primary)


        if VERBOSE:
            write_time = time.time() 
            print("write time: ", write_time - clean_time)


def clean(filepaths=FILEPATHS, outnames=OUTNAMES, ending=ENDING):
    '''Fixes column names and removes non-standard characters (including *)'''
    filepaths, outnames = standardize_paths(filepaths, outnames, ending)
    for i, filepath in enumerate(filepaths):
        with open(outnames[i], "w") as outfile, open(filepath, 'r') as content:
            out =  re.sub('\)|\(|\[|\]','', content.readline().replace('/','_').replace(' ','_').lower())
            outfile.write(out)
            for c in content.readlines():
                out = re.sub(WEIRD_CHARS,'', c)
                outfile.write(out)
        content.close()
        outfile.close()
    return True

def make_schema(outname, instructions = "-i postgresql --no-constraints", type_dict = TYPE_DICT):
    
    table_name = outname[:-4]
    schema = 'DROP TABLE IF EXISTS {}; \n CREATE TABLE {} ('.format(table_name,table_name)
    system('''head -n 1000 {} | csvsql {} > temp.txt'''.format(filepath,instructions))
    f = open("temp.txt")
    f.readline() # throw away the first line
    for line in f.readlines():
    
        col = re.findall('(\w*) [A-Z]', line)
    
        if VERBOSE:
            print('line:', line)
            print(col)
    
        try:
            datatype = type_dict[col[0]]
            schema += "\t{} {}, \n".format(col[0], datatype)
        except:
            schema += line
    
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

def standardize_paths(filepaths=FILEPATHS, outnames=OUTNAMES, ending=ENDING):
    if isinstance(filepaths, str):
        filepaths = [filepaths]
    
    if outnames:
        if isinstance(outnames, str):
            outnames = [outnames]
        if len(outnames)!=len(filepaths):  
            raise "outnames and filepaths should be same length: generating outnames"
    
    elif ending:
        outnames = [rename_csv(f, ending) for f in filepaths]
    
    return filepaths, outnames






if __name__=="__main__":
    # RUN FROM charters director ... ipython3 -i DB/clean_csv
    emmas_filepaths = ['/home/student/charters/Data/nces_download1.csv', 
        '/home/student/charters/Data/nces_download2.csv', '/home/student/charters/Data/nces_download3.csv', 
        '/home/student/charters/Data/2015-16_AllCACharterSchools.csv']
    tests_filepaths = ["Data/CATestData/ca2011_all.csv",
                "Data/CATestData/ca2012_all.csv",
                "Data/CATestData/ca2013_all.csv"]
    filepath = "~/MachineLearning/CATestData/ca2012_all.csv"
    
    clean(filepath)
    Schema = make_schema("out.csv")



    
