import os
import sys
import numpy as np
import pandas as pd

from CleaningFunctions import *

input_path = "../input/"
out_path = "../output/"

df = pd.read_csv(input_path + "accused.csv")
CleanData(df).to_csv(out_path + 'accused.csv', index = False)
