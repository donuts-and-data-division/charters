# charters
Machine Learning Project Predicting Charter Closure

https://drive.google.com/drive/folders/0B25-_v63EMbVWTEwemxybEFaZlU

# About

Nearly 550,000 students attend over 1,200 charter schools in California.  Over the course of the past three school years, over 120 of those schools have been closed for a variety of reasons.  With each closure, students and their families are left scrambling to find a different school – sometimes in the middle of a school year. The files in the repo organize school data from California and identify schools at-risk for closure using machine learning methods. 


# Overall Pipeline
<p>After setting up initial databse configurations, the general process for analysis is: 
<p>

<p>I. Load data into the database using DB/clean_csv.py <p>
  a. Most data sets are located in Data folder, but some of too large to be stored
<p>II. Organize tables and standardize format using DB/features_by_year_py, DB/wide-dropout.py, DB/wide-tests.py. <p> 
<p>III. Pull features from database using Pipeline/select_stuff.py, where a row in an individual (school, year) combination for    every year 2004-2015.   <p>
<p>IV. Generate features and impute data using Pipeline/cleaning.py <p>
<p>V. Run models using Pipeline/pipeline.py <p> 
  a. Analyze results using generated csv files. 

