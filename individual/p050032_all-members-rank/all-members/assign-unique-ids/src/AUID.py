import pandas as pd
import os
from AUIDFunctions import *

f = '../input/' + os.listdir('../input/')[0]
out_path = '../output/'

df = pd.read_csv(f)
print(df.columns)
id_cols = ["First.Name", "Last.Name", "Appointed.Date", "Gender", "Race", "Birth.Year","Rank", "Last.Date"]
df = AssignUniqueIDs(df,id_cols)
'''
df.to_csv(out_path + 'all-members.csv', index=False)

keep_cols = ['Middle_Initial', 'Suffix_Name', 'Current_Rank', 'Seniority_Date']
dfu = ModeAggregate(df, 'TID', keep_cols)
dfu.merge(df[['TID'] + id_cols].drop_duplicates(), on='TID').to_csv(out_path + "all-members_demo.csv", index=False)
'''
