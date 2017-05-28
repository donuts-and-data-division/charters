from db_config import *
from sqlalchemy import create_engine
import pandas as pd 
from os import system


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
        #return df2
        ##need to join this to re-enter names##                                                 
                               

        newcsv = "dropout_" + str(year) + "_wide"
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
        



