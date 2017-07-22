import pandas as pd
import os
from CleaningFunctions import *
import sys
infile = sys.argv[1]
outfile = sys.argv[-1]
df = pd.read_csv(infile)
df = CleanData(df)
df.to_csv(outfile, index=False)
