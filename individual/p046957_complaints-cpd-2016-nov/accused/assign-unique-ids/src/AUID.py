import os
import sys
import numpy as np
import pandas as pd

module_path = os.path.abspath(os.path.join('../../../../../tools/'))
if module_path not in sys.path: sys.path.append(module_path)

from AUIDFunctions import *

input_path = "../input/"
out_path = "../output/"

df = pd.read_csv(input_path + "accused.csv")
print(df.columns)
id_cols = ['First.Name', 'Last.Name', 'Middle.Initial', 'Suffix.Name', 'Appointed.Date', 'Birth.Year', 'Gender', 'Race', "Current.Unit", "Current.Rank", "Star"]
df = AssignUniqueIDs(df, id_cols)
df.to_csv(out_path + 'accused.csv',index=False)
dfu = ModeAggregate(df, 'TID',)
dfu.merge(df[['TID'] + id_cols].drop_duplicates(), on='TID').to_csv(out_path + 'accused_demo.csv', index=False)
