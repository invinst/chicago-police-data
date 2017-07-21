import pandas as pd
import os
from AUIDFunctions import *

f = '../input/' + os.listdir('../input/')[0]
out_path = '../output/'

df = pd.read_csv(f)

id_cols = ["First_Name", "Last_Name", "Appointed_Date", "Gender", "Race", "Birth_Year"]
df = AssignUniqueIDs(df,id_cols)
df.to_csv(out_path + 'all-members.csv', index=False)

keep_cols = ['Middle_Initial', 'Suffix_Name', 'Current_Rank', 'Seniority_Date']
dfu = ModeAggregate(df, 'TID', keep_cols)
dfu.merge(df[['TID'] + id_cols].drop_duplicates(), on='TID').to_csv(out_path + "all-members_demo.csv", index=False)
