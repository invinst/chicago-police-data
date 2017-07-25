import pandas as pd
import os
from AUIDFunctions import *

f = '../input/' + os.listdir('../input/')[0]
out_path = '../output/'

df = pd.read_csv(f)
id_cols = ["First.Name", "Last.Name","Middle.Initial","Suffix.Name", "Appointed.Date", "Gender", "Race", "Birth.Year"]
df = AssignUniqueIDs(df,id_cols, 'TID')
df.to_csv(out_path + 'all-members.csv', index=False)
AggregateData(df, "TID", id_cols).to_csv('../output/all-members_demo.csv', index=False)
