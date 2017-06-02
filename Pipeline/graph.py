import matplotlib.pyplot as plt
from select_stuff import *

DATABASE = "capp30254_project1"
HOST = "pg.rcc.uchicago.edu"
PORT = 5432
USER = "capp30254_project1_user"
PASSWORD = "bokMatofAtt."

def bar_graph():
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, HOST, PORT, DATABASE)
    engine = create_engine(db_string)
    string = """SELECT closeddate FROM ca_pubschls_new"""
    df = pd.read_sql_query(string, engine)

    df2 = df['closeddate'].dropna()
    df2 = df2.to_frame()

    years = []

    for i, row in df2.iterrows():

        year = row['closeddate'].year
        years.append(year)
    
    df2['year'] = years

    df2 = df2.loc[df2['year'].isin(range(2003, 2016))]



    counts = df2.groupby('year').count()
    counts.plot(kind='bar', color='#80002a', title='Charter Closures by Year')
    plt.show()

if __name__=="__main__":
   bar_graph()
