import pandas as pd
import numpy as np
import os
import sys

from ImportFunctions import *

input_path = '../input/'
out_path = '../output/'
out_file = 'victims.csv'

f = input_path + os.listdir(input_path)[0]
df = ReadMessy(f)
df.insert(0, "CRID", (df['Number']
                        .fillna(method='ffill')
                        .astype(int)))
df = (df
        .dropna(thresh = len(df.columns.values)-1, axis = 0)
        .drop('Number', axis=1)
        .reset_index(drop = True))
df.columns = ['CRID', 'Gender', 'Age', 'Race'] 
df.to_csv(out_path + out_file, index=False)
metadata_dataset(df, f, out_file).to_csv(out_path + "metadata_" + out_file, index=False)
