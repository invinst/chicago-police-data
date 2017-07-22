import pandas as pd
import os
from CleaningFunctions import *
import sys
infile = "../input/all-members.csv"
outfile = "../output/all-members.csv"
df = pd.read_csv(infile)
df = CleanData(df)
df.to_csv(outfile, index=False)
