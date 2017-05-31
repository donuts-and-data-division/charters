import pandas as pd
import re
import numpy as np
from os import system
from datetime import datetime

VERBOSE = True
TIMER = True
CLEAN = False
DATABASE = "postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1"
WEIRD_CHARS ='\"\*\"|ï¾\x86|ï¾\x96|\xf1|\"\"'
# For columns with critical typing force the type (note the camel case headings are generated in cleaning):
# csvsql automatically types columns and will fail frequently.  


FILEPATHS = ["catests_2015_wide2"]#["catests_2003_wide","catests_2004_wide", "catests_2005_wide", "catests_2006_wide","catests_2007_wide", \
#"catests_2008_wide", "catests_2009_wide", "catests_2010_wide", "catests_2011_wide", "catests_2012_wide", \
#"catests_2013_wide", "catests_2015_wide2"]
# OPTIONAL: Each file in filepath will be cleaned and a new file will be created. 
# The outname (minus the ".csv" will become the table name in the DB)
OUTNAMES = ["catests_2015_wide2.csv"]#["catests_2003_wide.csv","catests_2004_wide.csv", "catests_2005_wide.csv", "catests_2006_wide.csv","catests_2007_wide.csv", \
#"catests_2008_wide.csv", "catests_2009_wide.csv", "catests_2010_wide.csv", "catests_2011_wide.csv", "catests_2012_wide.csv", \
#"catests_2013_wide.csv", "catests_15_wide2.csv"] #FILEPATHS
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

TESTING = False


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
        
    print('''head -n 1000 {} | csvsql {} > temp.txt'''.format(outname,instructions))
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
    
    
