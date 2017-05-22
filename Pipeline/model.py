import pandas as pd
import numpy as np
from config import *
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression, Perceptron, SGDClassifier, OrthogonalMatchingPursuit, RandomizedLogisticRegression
from sklearn.neighbors.nearest_centroid import NearestCentroid
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.cross_validation import train_test_split
from sklearn.grid_search import ParameterGrid
from sklearn.metrics import *
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_recall_curve
import random
import pylab as pl
import matplotlib.pyplot as plt
import time

# based off Rayid's magicloops code: https://github.com/rayidghani/magicloops/blob/master/magicloops.py
def classifiers_loop(X_train, X_test, y_train, y_test):
    results =  pd.DataFrame(columns=('model_type','clf', 'parameters', 'auc-roc', 'precision_5', 'accuracy_5', 'recall_5',
                                                       'precision_10', 'accuracy_10', 'recall_10',
                                                       'precision_20', 'accuracy_20', 'recall_20', 'runtime', 'y_pred_probs'))
    for i, clf in enumerate([CLASSIFIERS[x] for x in TO_RUN]):
        print(TO_RUN[i])
        params = WHICH_GRID[TO_RUN[i]]
        for p in ParameterGrid(params):
            try:
                start_time = time.time()
                clf.set_params(**p)
                y_pred_probs = clf.fit(X_train, y_train).predict_proba(X_test)[:,1]
                y_pred_probs_sorted, y_test_sorted = zip(*sorted(zip(y_pred_probs, y_test), reverse=True))
                end_time = time.time()
                tot_time = end_time - start_time
                print(p)
                precision_5, accuracy_5, recall_5 = scores_at_k(y_test_sorted,y_pred_probs_sorted,5.0)
                precision_10, accuracy_10, recall_10 = scores_at_k(y_test_sorted,y_pred_probs_sorted,10.0)
                precision_20, accuracy_20, recall_20 = scores_at_k(y_test_sorted,y_pred_probs_sorted,20.0)
                results.loc[len(results)] = [TO_RUN[i], clf, p,
                                                       roc_auc_score(y_test, y_pred_probs),
                                                       precision_5, accuracy_5, recall_5,
                                                       precision_10, accuracy_10, recall_10,
                                                       precision_20, accuracy_20, recall_20,
                                                       tot_time, y_pred_probs]
                # plot_precision_recall_n(y_test,y_pred_probs,clf)
            except IndexError:
                    print('Error')
                    continue
    return results


def generate_binary_at_k(y_scores, k):
    cutoff_index = int(len(y_scores) * (k / 100.0))
    test_predictions_binary = [1 if x < cutoff_index else 0 for x in range(len(y_scores))]
    return test_predictions_binary

def scores_at_k(y_true, y_scores, k):
    preds_at_k = generate_binary_at_k(y_scores, k)
    precision = precision_score(y_true, preds_at_k)
    accuracy = accuracy_score(y_true, preds_at_k)
    recall = recall_score(y_true, preds_at_k)
    return precision, accuracy, recall

def plot_precision_recall_n(y_true, y_prob, model_name):
    y_score = y_prob
    precision_curve, recall_curve, pr_thresholds = precision_recall_curve(y_true, y_score)
    precision_curve = precision_curve[:-1]
    recall_curve = recall_curve[:-1]
    pct_above_per_thresh = []
    number_scored = len(y_score)
    for value in pr_thresholds:
        num_above_thresh = len(y_score[y_score>=value])
        pct_above_thresh = num_above_thresh / float(number_scored)
        pct_above_per_thresh.append(pct_above_thresh)
    pct_above_per_thresh = np.array(pct_above_per_thresh)
    
    plt.clf()
    fig, ax1 = plt.subplots()
    ax1.plot(pct_above_per_thresh, precision_curve, 'b')
    ax1.set_xlabel('percent of population')
    ax1.set_ylabel('precision', color='b')
    ax2 = ax1.twinx()
    ax2.plot(pct_above_per_thresh, recall_curve, 'r')
    ax2.set_ylabel('recall', color='r')
    ax1.set_ylim([0,1])
    ax1.set_ylim([0,1])
    ax2.set_xlim([0,1])
    
    name = model_name
    plt.title(name)
    plt.show()
    