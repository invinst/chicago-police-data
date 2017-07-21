import pandas as pd
import os
from CleaningFunctions import *
f = '../input/' + os.listdir('../input/')[0]
df = pd.read_csv(f)
df = CleanData(df)
df.to_csv('../output/unit-history.csv', index=False)
