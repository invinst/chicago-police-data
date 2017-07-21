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

id_cols = ['First_Name', 'Last_Name', 'Suffix_Name', 'Appointed_Date', 'Birth_Year', 'Gender', 'Race']
df = AssignUniqueIDs(df, id_cols)
df.to_csv(out_path + 'accused.csv',index=False)
keep_cols = ['Middle_Initial', 'Current_Unit', 'Current_Rank', 'Star']
dfu = ModeAggregate(df, 'TID', keep_cols)
dfu.merge(df[['TID'] + id_cols].drop_duplicates(), on='TID').to_csv(out_path + 'accused_demo.csv', index=False)
