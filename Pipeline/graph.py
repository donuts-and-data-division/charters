import matplotlib.pyplot as plt
from select_stuff import *

def bar_graph():
    df = select_statement()
    df2 = df['closeddate'].dropna()

    years = []

    for row in df2.iteritems():
        year = row[1].year
        years.append(year)
    df2 = df2.to_frame()
    df2['year'] = years

    counts = df2.groupby('year').count()
    counts.plot(kind='bar')
    plt.show()

if __name__=="__main__":
    bar_graph()
