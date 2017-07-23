import pandas as pd
import os
from ImportFunctions import *
f = os.listdir('../input/')[0]
df = pd.read_excel('../input/'+f)
df.columns = CorrectColumns(df.columns)

df[df['Last.Name'].str.len() <= 20].to_csv('../output/all-members.csv', index=False)
notes = df[df['Last.Name'].str.len() > 20]['Last.Name'].rename('Notes')
metadata_dataset(df[df['Last.Name'].str.len() <= 20], f, 'all-members.csv', notes = notes).to_csv('../output/metadata_all-members.csv', index=False)
