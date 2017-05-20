import pandas as pd
import numpy as np

def combine():
    '''Returns csv mapping nces code to california code'''
    
    nces = pd.read_csv("../Data/NCES_full.csv", 
        dtype={'School ID - NCES Assigned [Public School] Latest available year': 'str'})
    ca = pd.read_csv("../Data/ca_pubschls.csv", 
        encoding='ISO-8859-1', 
        dtype={"CDSCode": 'str', "NCESCode": 'str', "NCESDist": 'str', "NCESSchool": 'str'})

    # concat nces cols to from ca
    nces['School ID - NCES Assigned [Public School] Latest available year'] = \
        '0' + nces['School ID - NCES Assigned [Public School] Latest available year']
    master = pd.merge(ca[ca.Charter == 'Y'], nces, 
        how='inner', 
        left_on='NCESCode', 
        right_on='School ID - NCES Assigned [Public School] Latest available year', 
        suffixes=('_ca','_nces'))

    cols = ['NCESCode', 'CDSCode']

    master = master[cols]

    master.to_csv('../Data/ca_to_nces.csv')