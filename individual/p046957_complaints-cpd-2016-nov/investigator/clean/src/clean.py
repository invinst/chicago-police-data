import os
import sys
import numpy as np
import pandas as pd

module_path = os.path.abspath(os.path.join('../../../../../tools/'))
if module_path not in sys.path: sys.path.append(module_path)

from CleaningFunctions import *

input_path = "../input/"
out_path = "../output/"
df = pd.read_csv(input_path + "investigators.csv")
CleanData(df, int_cols = ["Star", "Assignment"]).to_csv(out_path + 'investigators.csv', index=False)
