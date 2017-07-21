import pandas as pd
import os

from ImportFunctions import *

files = ['../input/' + f for f in os.listdir('../input/')]
data = pd.DataFrame()
metadata = pd.DataFrame()

for f in files:
    xls = pd.ExcelFile(f)
    for s in xls.sheet_names:
        df = xls.parse(s)
        df.columns = CorrectColumns(df.columns)
        metadata = (metadata
                    .append(metadata_dataset(df, f + "-" + s,'salary.csv'))
                    .reset_index(drop=True))

        df['Year'] = int(s)
        data = (data
                .append(df)
                .reset_index(drop=True))

data.to_csv("../output/salary" + ".csv", index=False)
metadata.to_csv('../output/metadata_salary.csv', index=False)
