import pandas as pd

from cleaning_functions import clean_data, input_opts, output_opts

input_file = 'input/unit-history.csv.gz'
output_file = 'output/unit-history.csv.gz'

df = pd.read_csv(input_file, **input_opts)
df = clean_data(df)
df.to_csv(output_file, **output_opts)
