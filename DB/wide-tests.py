from db_config import *
from sqlalchemy import create_engine
import pandas as pd 
from os import system
from clean_csv import load


def new_table():
    """
    For test tables. 
    """
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)


    subgroups = {'2003': 'subgroup', '2004': 'subgroup', '2005': 'subgroup', '2006': 'subgroup', '2007': 'subgroup', '2008': 'subgroup',\
                '2009': 'subgroup_id', '2010': 'subgroup_id', '2011': 'subgroup_id', '2012': 'subgroup_id', '2012': 'subgroup_id',\
                '2013': 'subgroup_id', '2015': 'subgroup_id'}

    tests = {'2003': 'testid', '2004': 'test_id', '2005': 'test_id', '2006': 'test_id', '2007': 'test_id', '2008': 'test_id', \
            '2009': 'test_id', '2010': 'test_id', '2011': 'test_id', '2012': 'test_id', '2013': 'test_id', '2015': 'test_id',}

    years = ['2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2015']

    oldlist = ['cst_capa_percentage_advanced', 'cst_capa_percentage_proficient', 'cst_capa_percentage_at_or_above_proficient', \
            'cst_capa_percentage_basic', 'cst_capa_percentage_below_basic', 'cst_capa_percentage_far_below_basic']

    newlist = ['percentage_advanced', 'percentage_proficient', 'percentage_at_or_above_proficient', 'percentage_basic', \
            'percentage_below_basic', 'percentage_far_below_basic']

    list2015 = ['percentage_standard_exceeded', 'percentage_standard_met', 'percentage_met_and_above', \
            'percentage_standard_nearly_met', 'percentage_standard_not_met']

    scores = {'2003': oldlist, '2004': oldlist, '2005': oldlist, '2006': oldlist, '2007': oldlist, '2008': oldlist,
            '2009': newlist, '2010': newlist, '2011': newlist,
            '2012': newlist, '2013': newlist, '2015': list2015}

    
    for year in years: 

        subgroup = subgroups[year]
        test = tests[year]

        if year != '2015':
            string = """SELECT cdscode, {}, grade, percent_tested, {}, {}, {}, {}, {}, {}
                FROM "catests_{}"
                where ({} = 3 or {}=4 or {}=31 or {}=74 
                    or {}=76 or {}=78 or {}=80 or {}=120 
                    or {}=128) 
                    and {}=7;""".format(subgroup, scores[year][0], scores[year][1], scores[year][2], scores[year][3], \
                        scores[year][4], scores[year][5],
                        year, subgroup, subgroup, subgroup, subgroup, subgroup, subgroup, subgroup, subgroup, subgroup, test)

        else:
            string = tring = """SELECT cdscode, {}, grade, percent_tested, {}, {}, {}, {}, {}
                FROM "catests_{}"
                where ({} = 3 or {}=4 or {}=31 or {}=74 
                    or {}=76 or {}=78 or {}=80 or {}=120 
                    or {}=128) 
                    and {}=7;""".format(subgroup, scores[year][0], scores[year][1], scores[year][2], scores[year][3], \
                        scores[year][4],
                        year, subgroup, subgroup, subgroup, subgroup, subgroup, subgroup, subgroup, subgroup, subgroup, test)

        df = pd.read_sql_query(string, engine)

        newcol = "Subgroup_concat_" + year
        df[newcol] = df[subgroup].apply(str) + df['grade'].apply(str)

        if year != '2015':

            df['percentage_standard_not_met'] = df[scores[year][4]] + df[scores[year][5]]

            df.rename(columns={scores[year][0]: 'percentage_standard_exceeded', scores[year][1]: 'percentage_standard_met', \
                scores[year][2]: 'percentage_standard_met_and_above', scores[year][3]: 'percentage_standard_nearly_met'}, \
                inplace=True)

            df = df.drop(scores[year][4], axis=1)
            df = df.drop(scores[year][5], axis=1)

        index = "cdscode"

        values = ['percent_tested', 'percentage_standard_exceeded', 'percentage_standard_met', \
        'percentage_standard_nearly_met', 'percentage_standard_not_met']

        
        df2 = pd.pivot_table(df, index = index, columns= newcol, values = values)

        #get rid of multi-index columns
        df2.columns = df2.columns.map('_'.join)
        df2.reset_index(inplace=True)
        ##need to join this to re-enter names##                                                 
        
        #csvs = []                   
        newcsv = "catests_" + year + "_wide"
        #csvs.append(newcsv)
        #print('appending' + year)
        df2.to_csv(newcsv, index = False)

    '''    
    VERBOSE = True
    TIMER = True
    CLEAN = True
    DATABASE = "postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1"
    WEIRD_CHARS ='\"\*\"|ï¾\x86|ï¾\x96|\xf1|\"\"'
    # For columns with critical typing force the type (note the camel case headings are generated in cleaning):
    # csvsql automatically types columns and will fail frequently.  


    FILEPATHS = csvs
    # OPTIONAL: Each file in filepath will be cleaned and a new file will be created. 
    # The outname (minus the ".csv" will become the table name in the DB)
    OUTNAMES = FILEPATHS#["enrollment04.csv", "enrollment05.csv", "enrollment06.csv", "enrollment07.csv", "enrollment08.csv", "enrollment09.csv",\
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

    load(filepaths=FILEPATHS, outnames=OUTNAMES, ending=ENDING, make_id_cols= MAKE_ID_COLS, db = DATABASE)

        #try: 
            #print ("im going to the db")
            #system("""csvsql --db "postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1"  --insert {} --overwrite""".format(newcsv))
       # except: 
            #("table didn't go to db")
    '''