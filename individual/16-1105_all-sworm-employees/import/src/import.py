# Import necessary modules
import pandas as pd
# Import helperfunctions from symlinked file that is 'in' the current src/ directory
from import_functions import *

# Get input and output files
input_file = 'input/Kalven_16-1105_All_Sworn_Employees.xlsx'
output_file = 'output/unit-history.csv.gz'
output_metadata_file = "output/metadata_unit-history.csv.gz"

# load the data
df = pd.read_excel(input_file)
# Do some stuff (importing, standardizing column names, cleaning, etc.)
df.columns = standardize_columns(df.columns)
# write it out, out_opts and in_opts are stored in the ImportFunctions (CleanFunctions, AUIDFunctions) module
df.to_csv(output_file, **out_opts)
collect_metadata(df, input_file, output_file).to_csv(output_metadata_file, **out_opts)
