import pandas as pd
import os
from AUIDFunctions import *

f = '../input/' + os.listdir('../input/')[0]

df = pd.read_csv(f)
stars = ["Star" + str(i) for i in range(1,11)]
id_cols = ["First.Name", "Last.Name", "Middle.Initial", "Suffix.Name", "Appointed.Date", "Current.Age", "Gender", "Race"]
AssignUniqueIDs(df, id_cols, 'TID').to_csv('../output/unit-history.csv', index=False)
AggregateData(df, "TID", id_cols, max_cols = stars).to_csv('../output/unit-history_demo.csv', index=False)
