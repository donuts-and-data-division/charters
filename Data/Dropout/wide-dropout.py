from db_config import *
from sqlalchemy import create_engine
import pandas as pd 


def new_table():
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)

    years = [10, 11, 12, 13, 14, 15, 16]

    for year in years: 

    	string = """
        
  			SELECT *
        	FROM "newfilescohort{}"
        	;""".format(year)

    	df = pd.read_sql_query(string, engine)
    	print(df[:5])
    
    return None

    """
    #change from long to wide
    index = "CDS" + year
    #need to create a new column to be the column? 
    df.pivot(index = index, columns= "Subgroup", values = every single column I want! )

    newcsv = "dropout_" + str(year) + "_wide"
    df.to_csv(newcsv, index = False)

    #run through cleaning code, add new csv to database
    """

