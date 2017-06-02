import pandas as pd
import re
import numpy as np
from os import system
from datetime import datetime

VERBOSE = True
TIMER = True
CLEAN = True


DATABASE = "postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1"
WEIRD_CHARS ='\"\*\"|ï¾\x86|ï¾\x96|\xf1|\"\"'
# For columns with critical typing force the type (note the camel case headings are generated in cleaning):
# csvsql automatically types columns and will fail frequently.  


FILEPATHS = ['../Data/ACS_Child_data/acs_data.csv'] #["../Data/enrollment04.txt","../Data/enrollment05.txt", "../Data/enrollment06.txt", "../Data/enrollment07.txt",\
#"../Data/enrollment08.txt", "../Data/enrollment09.txt", "../Data/enrollment10.txt", "../Data/enrollment11.txt", \
#"../Data/enrollment12.txt", "../Data/enrollment13.txt", "../Data/enrollment14.txt", "../Data/enrollment15.txt", \
#"../Data/enrollment16.txt"]
# OPTIONAL: Each file in filepath will be cleaned and a new file will be created. 
# The outname (minus the ".csv" will become the table name in the DB)
OUTNAMES = ['acs_data.csv']#["enrollment04.csv", "enrollment05.csv", "enrollment06.csv", "enrollment07.csv", "enrollment08.csv", "enrollment09.csv",\
#"enrollment10.csv", "enrollment11.csv", "enrollment12.csv", "enrollment13.csv", "enrollment14.csv", "enrollment14.csv", \
#"enrollment16.csv"] 

# OTHERWISE: each new file will use the filepath name with an ending appended
ENDING = "_new.csv"
MAKE_ID_COLS = None#["county_code","district_code","school_code"]
TYPE_DICT = {"cds_code": "VARCHAR","district_code":"VARCHAR","school_code":"VARCHAR"}


# FUTURE set directory for output
BASEDIR = None
# FUTURE editable schemas 
#import sys, tempfile, os
#from subprocess import call
#EDITOR = os.environ.get('EDITOR','vim') 


def main():
    load()

def load(filepaths=FILEPATHS, outnames=OUTNAMES, ending=ENDING, make_id_cols= MAKE_ID_COLS, db = DATABASE):

    # Notice outnames may be specified or generated with an ending
    filepaths, outnames = standardize_paths(filepaths, outnames, ending)
    
    if CLEAN:
        if TIMER:
            start = datetime.now()
            print("Starting clean")
        
        clean(filepaths, outnames)

        if TIMER:
            clean_time = datetime.now() 
            print("clean time: ", clean_time - start)
    else: 
        if TIMER:
            clean_time = datetime.now()


    for f in outnames:
        table =f[:-4]
        
        if TIMER:
            print("Starting sql for {}".format(f))
        
        schema = make_schema(f)
        
        if TIMER:
            schema_time = datetime.now() 
            print("csvsql schema generate time: ", schema_time - clean_time)

        print(schema)
     
        try_sql("""psql -d {} -c '{}' """.format(db, schema))   
        try_sql("""psql -d {} -c '\copy {} from {} with csv header' """.format(db, table,f))

        if TIMER:
            write_time = datetime.now() 
            print("write time: ", write_time - schema_time)

        if make_id_cols:
           try_sql("""psql -d {} -c '{}' """.format(db, add_col(table)))
           try_sql("""psql -d {} -c '{}' """.format(db, make_unique_id(table, make_id_cols)))

        if TIMER:
            id_time = datetime.now() 
            print("make id time: ", id_time - write_time)
        

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
    schema = 'DROP TABLE IF EXISTS {}; \n CREATE TABLE {}(\n'.format(table_name,table_name)

    if VERBOSE:
        print("Inferring schema from {}".format(outname))
    system('''head -n 1000 {} | csvsql {} > temp.txt'''.format(outname,instructions))
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
        make_schema = '''psql --db {} -f {}'''.schema(db, schema)
        if VERBOSE:
            print('try: {}'.format(make_schema))
        system(make_schema)

        return True
    except:
        return False


def add_col(tbl,id="cdscode"):
    return 'ALTER TABLE {} ADD COLUMN {} VARCHAR;'.format(tbl,id)

def make_unique_id(tbl, cols = MAKE_ID_COLS, id="cdscode"):
    '''Generate the SQL statement to make a unique id from multiple columns'''
    out = """UPDATE {} SET {} = concat(""".format(tbl,id)
    for col in cols:
        out += """{},""".format(col)
    return out[:-1] + ");"


# HELPER FUNCTIONS
def try_sql(command):
    try:
        if VERBOSE:
            print('try: {}'.format(command))
        system(command)
    except:
        print("{}: FAILED".format(command))

def rename_csv(f, new_ending=None):
    '''Takes filepath strips the path and ending and adds a new_ending' (e.g. "_new.csv"'''
    reg = re.findall('/?(.*).csv$', f)
    f = reg[0] + new_ending
    return f

def standardize_paths(filepaths=FILEPATHS, outnames=OUTNAMES, ending=ENDING):
    '''
    take out
    '''
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
main()