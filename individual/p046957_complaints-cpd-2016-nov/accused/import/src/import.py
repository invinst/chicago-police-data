import pandas as pd
import numpy as np
import os
import sys

from ImportFunctions import *

input_path = "../input/"
out_path = "../output/"
out_file = "accused.csv"

files = [input_path + f for f in os.listdir(input_path)]

data = pd.DataFrame()
metadata = pd.DataFrame()

for f in files:
    df = ReadMessy(f)
    df.insert(0, "CRID", (df["Number:"]
                            .fillna(method='ffill')
                            .astype(int)))
    df = (df.drop("Number:", axis=1)
            .dropna(thresh = 2))
    df.columns = ["CRID", "Full.Name", "Birth.Year", "Gender", "Race", "Appointed.Date", "Current.Unit", "Current.Rank", "Star","Complaint.Category", "Recommended.Finding", "Recommended.Discipline", "Final.Finding", "Final.Discipline"]
    
    data = (data
            .append(df)
            .reset_index(drop=True))

    metadata = (metadata
                    .append(metadata_dataset(df, f, out_file)
                    .reset_index(drop=True)))

data.to_csv(out_path + out_file, index=False)
metadata.to_csv(out_path + "metadata_" +  out_file, index=False)
