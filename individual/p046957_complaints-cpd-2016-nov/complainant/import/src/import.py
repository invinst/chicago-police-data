import pandas as pd
import numpy as np
import os
import sys

from ImportFunctions import *

input_path = "../input/"
out_path = "../output/"
out_file = "complainants.csv"

files = [input_path + f for f in os.listdir(input_path)]

data = pd.DataFrame()
metadata = pd.DataFrame()

for f in files:
    df = ReadMessy(f)
    df.insert(0, 'CRID', (df['Number']
                            .fillna(method='ffill')
                            .astype(int)))
    df = (df
            .dropna(thresh = len(df.columns.values)-1, axis=0)
            .drop("Number", axis = 1))
    df.columns = ["CRID", "Gender","Age", "Race"]
    
    data = (data
            .append(df)
            .reset_index(drop=True))
    metadata = (metadata
                .append(metadata_dataset(df, f, out_file))
                .reset_index(drop=True))

data.to_csv(out_path + out_file, index = False)
metadata.to_csv(out_path + "metadata_" + out_file, index=False)
