import pandas as pd
import os
from ImportFunctions import *

f = '../input/' + os.listdir('../input')[0]
df = pd.read_excel(f)
df.columns = CorrectColumns(df.columns)
df.to_csv('../output/unit-history.csv', index=False)
metadata_dataset(df, f, 'unit-history.csv').to_csv('../output/metadata_unit-history.csv',index=False)
