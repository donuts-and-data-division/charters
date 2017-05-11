import pylab as pl
import seaborn as sns
import numpy as np
import pandas as pd
import math


def summary_by_outcome(df, col, round_to=2):
	'''
	'''
	split_data = df.groupby(col)

	return round(split_data.describe(percentiles=[.5]),round_to).T



def correlation_plot(df):
	'''
	Makes a correlation plot of numeric columns in dataframe
	df (pd.DataFrame)
	Relied heavily on: http://seaborn.pydata.org/examples/many_pairwise_correlations.html
	'''
	sns.set(style="white")
	corr = df.corr()
	corr = corr.sort_index(axis=0).sort_index(axis=1)
	# Generate a mask for the upper triangle
	mask = np.zeros_like(corr, dtype=np.bool)
	mask[np.triu_indices_from(mask)] = True

	# Set up the matplotlib figure
	f, ax = pl.subplots(figsize=(20, 9))

	# Generate a custom diverging colormap
	cmap = sns.diverging_palette(220, 10, as_cmap=True)

	# Draw the heatmap with the mask and correct aspect ratio
	sns.heatmap(corr, mask=mask, cmap=cmap, vmax=1, square=True,
	            linewidths=.5, cbar_kws={"shrink": .7})


def no_correction_function(x):
    return x

def my_distplot(df, col, binary_split=None,fn=no_correction_function, hist=False, kde=True):
    '''
    df (dataFrame)
    col (column in dataFrame) we expect values >= 0 
    binary_split (column in df of 0 and 1)
    fn (function) (e.g. math.log, math.sqrt)
    FUTURE: use quantile or other methods to cut outliers
    FUTURE: make arbitrary distinct graphs not just binary
    '''
    normalized_data = df[col].map(lambda x: fn(x) if x > 0 else x)
    if binary_split:
        sns.distplot(normalized_data.loc[df[binary_split]==1], color = 'red', hist=hist,kde=kde, label='{} =1'.format(binary_split))
        sns.distplot(normalized_data.loc[df[binary_split]==0], color = 'black',hist=hist,kde=kde,label='{} = 0'.format(binary_split))
        plt.title("Histogram of {} of {} split on {}".format(fn, col, binary_split))
    else:
        sns.distplot(normalized_data)
        plt.title("Histogram of {} of {}".format(fn, col), label = col)
    if fn==no_correction_function:
        plt.xlabel("{}".format(col))
    else:
        plt.xlabel("{} of {}".format(fn, col))
    plt.legend()
    


def print_null_freq(df):
    """
    for a given DataFrame, calculates how many values for 
    each variable is null and prints the resulting table to stdout
    Code from: https://github.com/yhat/DataGotham2013/blob/master/notebooks/3%20-%20Importing%20Data.ipynb
    """
    df_lng = pd.melt(df)
    null_variables = df_lng.value.isnull()
    return pd.crosstab(df_lng.variable, null_variables)


def histogram(df, column_name, num_bins):
    if skew(df[column_name]) > 10:
        q = df[column_name].quantile(0.99)
        no_outliers = df[df[column_name] < q]
        sns.distplot(no_outliers[column_name], bins=num_bins, norm_hist = False)
    else: 
        sns.distplot(df[col], bins=num_bins, norm_hist = True)
    plt.title('Histogram of' + ' ' + column_name)
    plt.ylabel('Count')
    plt.show()


def histogram_all(df, num_bins):
    for col in df.columns.tolist():
        if skew(df[col]) > 10:
            q = df[col].quantile(0.99)
            no_outliers = df[df[col] < q]
            sns.distplot(no_outliers[col], bins=num_bins, norm_hist = True)
        else: 
            sns.distplot(df[col], bins=num_bins, norm_hist = True)
        plt.show()
        

'''
Useful APIs:
seaborn.tsplot(data, time=None, unit=None, condition=None, 
			value=None, err_style='ci_band', ci=68, interpolate=True, 
			color=None, estimator=<function mean>, n_boot=5000, 
			err_palette=None, err_kws=None, legend=True, ax=None, 
			**kwargs)



'''





