import pandas as pd
import os
from ImportFunctions import *
f = '../input/' + os.listdir('../input/')[0]
df = pd.read_excel(f)
df.columns = CorrectColumns(df.columns)
out_file = 'all-members.csv.gz'
df[df['Last.Name'].str.len() <= 20].to_csv('../output/' + out_file, **out_opts)
notes = df[df['Last.Name'].str.len() > 20]['Last.Name'].rename('Notes')
metadata_dataset(df[df['Last.Name'].str.len() <= 20], f, out_file, notes = notes).to_csv('../output/metadata_' + out_file, **out_opts)
