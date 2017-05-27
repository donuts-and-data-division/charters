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

    years = [10, 11, 12, 13, 14, 15, 16]

    years2 = [10]

    for year in years2: 

    	string = """SELECT *
        	FROM "newfilescohort{}"
        	;""".format(year)

    	df = pd.read_sql_query(string, engine)
    	#print(df[:5])
    	#return df

    	newcol = "Subgroup_concat_" + str(year)
    	df[newcol] = df["Subgroup" + str(year)] + df["Subgrouptype" + str(year)]

    	#change from long to wide
    	df.reset_index(inplace = True)
    	df2 = df.set_index(["CDS" + str(year), newcol]).unstack()
    	df2.columns = df2.columns.map(lambda x: '{}_subgroup{}'.format(*x))
    	df2.reset_index(inplace = True)
    	#return df2
    	print (df2[:5])


    	to_drop = []
    	for group in df["Subgroup"].unique():
    		for group2 in df["Subgrouptype"].unique():
    			if group != 'All'
    				if group2 != 'All'
		    			to_drop.append('Name' + str(year) + "_subgroup" + group + group2)
		    			to_drop.append('AggLevel' + str(year) + "_subgroup" + group + group2)
		    			to_drop.append('DFC' + str(year) + "_subgroup" + group + group2)
		    			#makes sense to drop subgroups? 

    	df2.drop(to_drop, axis = 1, inplace = True)

    	newcsv = "dropout_" + str(year) + "_wide"
    	df2.to_csv(newcsv, index = False)
    	print ("im going to the db")

    	try: 
    		print ("im going to the db")
    		system("""csvsql --db "postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1"  --insert {} --overwrite""".format(newcsv))
    	except: 
    		("table didn't go to db")


    """

    year = str(year)
    index = "CDS" + year
    values = ["NumCohort" + year, "NumGraduates" + year, "Cohort Graduation Rate" + year,\
    "NumDropouts" + year, "CoHhort Dropout Rate" + year, "NumSpecialEducation" + year, \
    "Special Ed Completers Rate" + year, "NumStillEnrolled" + year, "Still Enrolled Rate" + year, \
    "NumGED" + year, "GED Rate" + year, "Year" + year]


    #creates multi-index df
    df.pivot_table(index = index, columns= "Subgroup", values = values)


    example: 
    df.pivot_table(index = "School", columns = "Subgroup", values = ["Dropout", "Other"])  


    df3 = df2.pivot_table(index = "Name10", columns = "Subgroup_concat", values = ["NumCohort10"])                                                
                           
    """

