import pandas as pd
import os
from ImportFunctions import *
f = '../input/' + os.listdir('../input')[0]
out_path = 'unit-history.csv.gz'
df = pd.read_excel(f)
df.columns = CorrectColumns(df.columns)
df.to_csv('../output/' + out_path, **out_opts)
metadata_dataset(df, f, out_path).to_csv('../output/metadata_' + out_path, **out_opts)
