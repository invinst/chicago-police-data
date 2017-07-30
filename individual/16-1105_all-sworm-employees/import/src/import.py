# Import necessary modules
import pandas as pd
import os
# Import helperfunctions from symlinked file that is 'in' the current src/ directory
from ImportFunctions import *
# Get the file / files from the ../input/ directory
f = '../input/' + os.listdir('../input')[0]
# define the outpath
out_path = 'unit-history.csv.gz'
# load the data
df = pd.read_excel(f)
# Do some stuff (importing, standardizing column names, cleaning, etc.)
df.columns = CorrectColumns(df.columns)
# write it out, out_opts and in_opts are stored in the ImportFunctions (CleanFunctions, AUIDFunctions) module
df.to_csv('../output/' + out_path, **out_opts)
metadata_dataset(df, f, out_path).to_csv('../output/metadata_' + out_path, **out_opts)
