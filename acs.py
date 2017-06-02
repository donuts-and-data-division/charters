

def sum_cols(df, cols, verbose = False):
    if verbose:
        print(df.ix[:,cols].columns)
    return df.ix[:,cols].apply(sum,1)


col_dict = {"total_below_poverty" : [5,19],
    "under_5_below_poverty" : [6,20],
    "age_5_below_poverty" : [7,21],
    "age_6_to_11_below_poverty" : [8,22],
    "age_12_to_17_below_poverty" : [9,10,11,23,24,25], 
    "age_18_65_below_poverty" : [12,13,14,15,16,26,27,28,29,30],
    "total_above_poverty" : [34,48],
    "under_5_above_poverty" : [35,49],
    "age_5_above_poverty" : [36,50],
    "age_6_to_11_above_poverty" : [37,51],
    "age_12_to_17_above_poverty" : [38,39,40,52,53,54], 
    "age_18_65_above_poverty" : [41,42,43,44,45,55,56,57,58,59]
    }

def aggregate_data(files):
    for f in files:
        name = f
        f = pd.read_csv(f)
        f.columns = [re.sub("Margin of .*", "REMOVE", col) for col in f.columns]
        del f['REMOVE']
        out = f.ix[:,:4]
        for k,v in col_dict.items():
            pd.DataFrame(sum_cols(f, v, True), columns=[k])
            out[k] = pd.DataFrame(sum_cols(f, v, True))
        out.to_csv(name, index=False)


def clean_dec(file):
    df = pd.read_csv(file)
    df.drop(['Income in 1999 below poverty level: - 65 to 74 years', 'Income in 1999 below poverty level: - 75 years and over', 'Income in 1999 at or above poverty level: - 65 to 74 years',\
            'Income in 1999 at or above poverty level: - 75 years and over'],axis=1)
    return df

def stack(files):
    out = pd.DataFrame()
    for f in files:
        out = concat([out, f], axis=0)
    return out


