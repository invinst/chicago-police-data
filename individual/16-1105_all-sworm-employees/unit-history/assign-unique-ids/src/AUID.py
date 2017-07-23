import pandas as pd
import os
from AUIDFunctions import *

f = '../input/' + os.listdir('../input/')[0]

df = pd.read_csv(f)
stars = ["Star" + str(i) for i in range(1,11)]
dfu = df[["First.Name", "Last.Name", "Middle.Initial", "Suffix.Name", "Race", "Gender", "Current.Age", "Appointed.Date"] + stars].drop_duplicates()

dfu = AssignUniqueIDs(dfu,id_cols)
