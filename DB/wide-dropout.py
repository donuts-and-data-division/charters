#Create wide table for each year of dropout data! 
#adapted from https://stackoverflow.com/questions/41451199/long-to-wide-dataframe-in-pandas-with-pivot-column-name-in-new-columns
#https://stackoverflow.com/questions/24290297/pandas-dataframe-with-multiindex-column-merge-levels

from db_config import *
from sqlalchemy import create_engine
import pandas as pd 
from os import system


def db_table_query(query):
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    string = query
    df = pd.read_sql_query(string, engine)




def new_table():
    """
    For dropout tables. 
    """
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    years = ['10', '11', '12', '13', '14', '15', '16']

    years2 = ['10']

    for year in years: 

        string = """SELECT *
            FROM "newfilescohort{}"
            ;""".format(year)

        df = pd.read_sql_query(string, engine)

        newcol = "Subgroup_concat_" + year
        df[newcol] = df["Subgroup" + year] + df["Subgrouptype" + year]


        index = "CDS" + year
        values = ["NumCohort" + year, "NumGraduates" + year, "Cohort Graduation Rate" + year,\
        "NumDropouts" + year, "Cohort Dropout Rate" + year, "NumSpecialEducation" + year, \
        "Special Ed Completers Rate" + year, "NumStillEnrolled" + year, "Still Enrolled Rate" + year, \
        "NumGED" + year, "GED Rate" + year]

        
        df2 = pd.pivot_table(df, index = index, columns= newcol, values = values)
        #get rid of multi-index columns
        df2.columns = df2.columns.map('_'.join)
        df2.reset_index(inplace=True)
        ##need to join this to re-enter names##                                                 
                               

        newcsv = "dropout_" + str(year) + "_wide"
        df2.to_csv(newcsv, index = False)

        try: 
            print ("im going to the db")
            system("""csvsql --db "postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1"  --insert {} --overwrite""".format(newcsv))
        except: 
            ("table didn't go to db")





def new_enrollment_table():
    """
    For dropout tables. 
    """
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    years = ['04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16']
    years2 = ['04']

    for year in years: 

        string = """SELECT *
            FROM "enrollment{}"
            ;""".format(year)
        print (string)

        df = pd.read_sql_query(string, engine)
        print (df.head())

        newcol = "Subgroup_concat_" + year
        df[newcol] = df["ethnic"] + df["gender"]


        index = "cds_code"
        values = ["kdgn", "gr_1", "gr_2", "gr_3", "gr_4", "gr_5", "gr_6", "gr_7", "gr_8", "ungr_elm"\
        "gr_9", "gr_10", "gr_11", "gr_12", "ungr_sec", "enr_total", "adult"]
        #values = ["NumCohort" + year, "NumGraduates" + year, "Cohort Graduation Rate" + year,\
        #"NumDropouts" + year, "Cohort Dropout Rate" + year, "NumSpecialEducation" + year, \
        #"Special Ed Completers Rate" + year, "NumStillEnrolled" + year, "Still Enrolled Rate" + year, \
        #"NumGED" + year, "GED Rate" + year]

        
        df2 = pd.pivot_table(df, index = index, columns= newcol, values = values)
        #get rid of multi-index columns
        df2.columns = df2.columns.map('_'.join)
        df2.reset_index(inplace=True)
        ##need to join this to re-enter names##                                                 
                               

        newcsv = "enrollment_" + str(year) + "_wide"
        df2.to_csv(newcsv, index = False)

        try: 
            print ("im going to the db")
            system("""csvsql --db "postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1"  --insert {} --overwrite""".format(newcsv))
        except: 
            ("table didn't go to db")


        """

        #change from long to wide using UNSTACK
        df.reset_index(inplace = True)
        df2 = df.set_index(["CDS" + str(year), newcol]).unstack()
        df2.columns = df2.columns.map(lambda x: '{}_subgroup{}'.format(*x))
        df2.reset_index(inplace = True)
        #return df2
        print (df2.shape)

        to_drop = []
        for group in df["Subgroup" + str(year)].unique():
            for group2 in df["Subgrouptype" + str(year)].unique():
                if group != 'All' or group2 != 'All':

                        col_name = 'Name' + str(year) + "_subgroup" + group + group2
                        col_agg = 'AggLevel' + str(year) + "_subgroup" + group + group2
                        col_dfc = 'DFC' + str(year) + "_subgroup" + group + group2

                        if col_name in df2.columns: 
                            to_drop.append(col_name)
                        if col_agg in df2.columns: 
                            to_drop.append(col_agg)
                        if col_dfc in df2.columns: 
                            to_drop.append(col_dfc)
                        #makes sense to drop subgroups? 
        
        df2.drop(to_drop, axis = 1, inplace = True)
        print (df2.shape)

        """

    #test query
    #select "Cohort Dropout Rate10_subgroupAll2" from "dropout_10_wide" where "Name10_subgroupAll0" = 'Grant Union High';
        



