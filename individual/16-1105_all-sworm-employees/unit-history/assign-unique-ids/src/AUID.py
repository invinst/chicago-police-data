import pandas as pd
import os
from AUIDFunctions import *

f = '../input/' + os.listdir('../input/')[0]

df = pd.read_csv(f)

dfu = df[["First_Name", "Last_Name", "Middle_Initial", "Suffix_Name", "Appointed_Date", "Race", "Gender"] + ["Star" + str(i) for i in range(1,11)]].drop_duplicates()


