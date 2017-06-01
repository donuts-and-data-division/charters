from sklearn import preprocessing, cross_validation, svm, metrics, tree, decomposition, svm
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression, Perceptron, SGDClassifier, OrthogonalMatchingPursuit, RandomizedLogisticRegression
from sklearn.neighbors.nearest_centroid import NearestCentroid
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier

# name of outcome feature
OUTCOME_VAR = 'closed_2014.0'

# list of features to include in model
FEATURE_COLS = ["charter_authorizer", 
    "afilliated_organization", 
    "site_type", 
    "start_type",
    "fundingtype"
    ]

# percent split for training vs teseting
TEST_SIZE = .20

# columns that need type conversions
TO_INT = []
TO_FLOAT = []
TO_STR = []
TO_BOOL = []

# columns with extreme values that need capping
EXTREME_COLS = []

# percentile value at which to cap EXTREME_COLS
CAP = 0.99

# continuous variables that need discretizing; not necessary for credit data
BUCKETING_COLS = ['percent_female_2012-13', 'percent_female_2011-12', 'percent_female_2010-11',
                  'percent_white_2012-13', 'percent_white_2011-12', 'percent_white_2010-12',
                  'percent_black_2012-13', 'percent_black_2011-12', 'percent_black_2010-11',
                  'percent_hispanic_2012-13', 'percent_hispanic_2011-12', 'percent_hispanic_2010-11',
                  'percent_asian_pi_2012-13', 'percent_asian_pi_2011-12', 'percent_asian_pi_2010-11',
                  'percent_freelunch_2012-13', 'percent_freelunch_2011-12', 'percent_freelunch_2010-11',
                  'total_students_all_grades_excludes_ae_public_school_2012-13',
                  'total_students_all_grades_excludes_ae_public_school_2011-12',
                  'total_students_all_grades_excludes_ae_public_school_2010-11',
                  'pupil_teacher_ratio_2012-13', 'pupil_teacher_ratio_2011-12',
                  'full-time_equivalent_fte_teachers_public_school_2012-13',
                  'full-time_equivalent_fte_teachers_public_school_2011-12'
                  ]

                  #maybe make a dictionary if we want varying numbers of buckets

# variables to bucket by district
DISTRICT_BUCKETING = ['percent_female_2012-13', 'percent_female_2011-12', 'percent_female_2010-11',
                  'percent_white_2012-13', 'percent_white_2011-12', 'percent_white_2010-12',
                  'percent_black_2012-13', 'percent_black_2011-12', 'percent_black_2010-11',
                  'percent_hispanic_2012-13', 'percent_hispanic_2011-12', 'percent_hispanic_2010-11',
                  'percent_asian_pi_2012-13', 'percent_asian_pi_2011-12', 'percent_asian_pi_2010-11',
                  'percent_freelunch_2012-13', 'percent_freelunch_2011-12', 'percent_freelunch_2010-11',
                  'total_students_all_grades_excludes_ae_public_school_2012-13',
                  'total_students_all_grades_excludes_ae_public_school_2011-12',
                  'total_students_all_grades_excludes_ae_public_school_2010-11',
                  'pupil_teacher_ratio_2012-13', 'pupil_teacher_ratio_2011-12',
                  'full-time_equivalent_fte_teachers_public_school_2012-13',
                  'full-time_equivalent_fte_teachers_public_school_2011-12'
                  ]

# number of buckets to cut the BUCKETING_COLS into
Q = 10

# categorical variables to be turned into dummies
CATEGORICAL = ['fundingtype', 'soctype', 'eilname', 'site_type', 'start_type']

# indicator for whether to normalize columns
NORMALIZE = False

# replace Nones in string feature with "Unknown"
REP_NONE = ["charter_authorizer", 
    "afilliated_organization", 
    "site_type", 
    "start_type",
    "fundingtype"]

# string variables to be encoded
LABEL_ENCODE = REP_NONE

# features that should be converted to percents
PERCENTAGE_FEATURES = {}
# Note: Not using percentage features below because not using NCES data anymore
'''
PERCENTAGE_FEATURES = {'percent_female_2012-13': ['female_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_female_2011-12': ['female_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_female_2010-11': ['female_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13'],

                           'percent_white_2012-13': ['white_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_white_2011-12': ['white_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_white_2010-11': ['white_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13'],

                           'percent_black_2012-13': ['black_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_black_2011-12': ['black_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_black_2010-11': ['black_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13'],

                           'percent_hispanic_2012-13': ['hispanic_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_hispanic_2011-12': ['hispanic_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_hispanic_2010-11': ['hispanic_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13'],

                           'percent_asian_pi_2012-13': ['asian_or_pacific_islander_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_asian_pi_2011-12': ['asian_or_pacific_islander_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_asian_pi_2010-11': ['asian_or_pacific_islander_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13'],

                           'percent_freelunch_2012-13': ['free_and_reduced_lunch_students_public_school_2012-13', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_freelunch_2011-12': ['free_and_reduced_lunch_students_public_school_2011-12', 'total_students_all_grades_excludes_ae_public_school_2012-13'],
                           'percent_freelunch_2010-11': ['free_and_reduced_lunch_students_public_school_2010-11', 'total_students_all_grades_excludes_ae_public_school_2012-13']
                           }
'''

# NOTE: the below is based on Rayid's magicloops code: https://github.com/rayidghani/magicloops/blob/master/magicloops.py
# all classifiers and their default params
CLASSIFIERS = {'RF': RandomForestClassifier(n_estimators=50, n_jobs=-1),
        'ET': ExtraTreesClassifier(n_estimators=10, n_jobs=-1, criterion='entropy'),
        'AB': AdaBoostClassifier(DecisionTreeClassifier(max_depth=1), algorithm="SAMME", n_estimators=200),
        'LR': LogisticRegression(penalty='l1', C=1e5),
        'SVM': svm.SVC(kernel='linear', probability=True, random_state=0),
        'GB': GradientBoostingClassifier(learning_rate=0.05, subsample=0.5, max_depth=6, n_estimators=10),
        'NB': GaussianNB(),
        'DT': DecisionTreeClassifier(),
        'SGD': SGDClassifier(loss="hinge", penalty="l2"),
        'KNN': KNeighborsClassifier(n_neighbors=3) 
            }

# list of classifier models to run
TO_RUN = ['GB','RF','DT','LR','NB']

# all grids to potentially loop through
LARGE_GRID = { 
'RF':{'n_estimators': [1,10,100,1000,10000], 'max_depth': [1,5,10,20,50,100], 'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
'LR': { 'penalty': ['l1','l2'], 'C': [0.00001,0.0001,0.001,0.01,0.1,1,10]},
'SGD': { 'loss': ['hinge','log','perceptron'], 'penalty': ['l2','l1','elasticnet']},
'ET': { 'n_estimators': [1,10,100,1000,10000], 'criterion' : ['gini', 'entropy'] ,'max_depth': [1,5,10,20,50,100], 'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
'AB': { 'algorithm': ['SAMME', 'SAMME.R'], 'n_estimators': [1,10,100,1000,10000]},
'GB': {'n_estimators': [1,10,100,1000,10000], 'learning_rate' : [0.001,0.01,0.05,0.1,0.5],'subsample' : [0.1,0.5,1.0], 'max_depth': [1,3,5,10,20,50,100]},
'NB' : {},
'DT': {'criterion': ['gini', 'entropy'], 'max_depth': [1,5,10,20,50,100], 'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
'SVM' :{'C' :[0.00001,0.0001,0.001,0.01,0.1,1,10],'kernel':['linear']},
'KNN' :{'n_neighbors': [1,5,10,25,50,100],'weights': ['uniform','distance'],'algorithm': ['auto','ball_tree','kd_tree']}
       }

small_grid = { 
'RF':{'n_estimators': [10,100], 'max_depth': [5,50], 'max_features': ['sqrt','log2'],'min_samples_split': [2,10]},
'LR': { 'penalty': ['l1','l2'], 'C': [0.00001,0.001,0.1,1,10]},
'SGD': { 'loss': ['hinge','log','perceptron'], 'penalty': ['l2','l1','elasticnet']},
'ET': { 'n_estimators': [10,100], 'criterion' : ['gini', 'entropy'] ,'max_depth': [5,50], 'max_features': ['sqrt','log2'],'min_samples_split': [2,10]},
'AB': { 'algorithm': ['SAMME', 'SAMME.R'], 'n_estimators': [1,10,100,1000,10000]},
'GB': {'n_estimators': [10,100], 'learning_rate' : [0.001,0.1,0.5],'subsample' : [0.1,0.5,1.0], 'max_depth': [5,50]},
'NB' : {},
'DT': {'criterion': ['gini', 'entropy'], 'max_depth': [1,5,10,20,50,100], 'max_features': ['sqrt','log2'],'min_samples_split': [2,5,10]},
'SVM' :{'C' :[0.00001,0.0001,0.001,0.01,0.1,1,10],'kernel':['linear']},
'KNN' :{'n_neighbors': [1,5,10,25,50,100],'weights': ['uniform','distance'],'algorithm': ['auto','ball_tree','kd_tree']}
       }

TEST_GRID = { 
'RF':{'n_estimators': [1], 'max_depth': [1], 'max_features': ['sqrt'],'min_samples_split': [10]},
'LR': { 'penalty': ['l1'], 'C': [0.01]},
'SGD': { 'loss': ['perceptron'], 'penalty': ['l2']},
'ET': { 'n_estimators': [1], 'criterion' : ['gini'] ,'max_depth': [1], 'max_features': ['sqrt'],'min_samples_split': [10]},
'AB': { 'algorithm': ['SAMME'], 'n_estimators': [1]},
'GB': {'n_estimators': [1], 'learning_rate' : [0.1],'subsample' : [0.5], 'max_depth': [1]},
'NB' : {},
'DT': {'criterion': ['gini'], 'max_depth': [1], 'max_features': ['sqrt'],'min_samples_split': [10]},
'SVM' :{'C' :[0.01],'kernel':['linear']},
'KNN' :{'n_neighbors': [5],'weights': ['uniform'],'algorithm': ['auto']}
       }

# which grid size to use
WHICH_GRID = TEST_GRID

COHORT_COLS = []