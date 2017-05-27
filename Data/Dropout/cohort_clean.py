#Hello
#Edit original CDE cohort data files
#Add new csv's to database


import pandas as pd 
from os import system


#csv_list = ['Cohort2009-10.csv', 'Cohort2010-11.csv', 'Cohort2011-12.csv', 'Cohort2012-13.csv', \
#'Cohort2013-14.csv', 'Cohort2014-15.csv', 'Cohort2015-16.csv']

csv_list = ['filescohort10.txt', 'filescohort11.txt', 'filescohort12.txt', 'filescohort13.txt', \
'filescohort14.txt', 'filescohort15.txt', 'filescohort16.txt']

csv_list2 = ['filescohort10.txt']


for csv in csv_list: 

	df = pd.read_csv(csv, sep = "	")
	df.replace(['*'], [None], inplace=True)

	#filter school-level data
	df = df[df['AggLevel'] == 'S']

	#add years to column names 
	"""
	newcols = [df.columns[0]]
	for col in df.columns[1:]:
		newcol = col + csv[-6:-4]
		newcols.append(newcol)
	"""
	newcols = []
	for col in df.columns:
		newcol = col + csv[-6:-4]
		newcols.append(newcol)

	df.columns = newcols

	newcsv = 'new' + csv 
	print (newcsv)

	df.to_csv(newcsv, index = False)

	system("""csvsql --db "postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1"  --insert {} --overwrite""".format(newcsv))



	