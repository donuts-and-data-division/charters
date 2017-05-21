import pandas as pd
from sqlalchemy import create_engine
from config import *


db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
engine = create_engine(db_string)
string = 'SELECT * FROM ca_to_nces;'
df = pd.read_sql_query(string, engine)