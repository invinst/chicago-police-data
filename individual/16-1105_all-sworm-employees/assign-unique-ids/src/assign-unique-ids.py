import pandas as pd

from AUIDFunctions import *

input_file = "input/unit-history.csv.gz"
output_file = 'output/unit-history.csv.gz'
output_demo_file = 'output/unit-history_demographics.csv.gz'

df = pd.read_csv(input_file, **input_opts)

stars = ["Star" + str(i) for i in range(1,11)]
id_cols = [
    "First.Name", "Last.Name", "Middle.Initial", "Suffix.Name",
    "Appointed.Date", "Current.Age", "Gender", "Race"
    ]

df = assign_unique_ids(df, 'TID', id_cols)
df.to_csv(output_file, **output_opts)

agg_df = aggregate_data(df, 'TID', id_cols,
                max_cols=stars,
                current_cols=['Unit'], time_col = 'Effective_Date')
agg_df.to_csv(output_demo_file, **output_opts)
