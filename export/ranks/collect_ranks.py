import pandas as pd
import os
rank_file_name = '~/Github/chicago-police-data/export/ranks/rank_list.csv'
rank_df = pd.read_csv(rank_file_name)

file_name = os.listdir('../input/')[0]
file_full_name = '../input/' + file_name
df = pd.read_csv(file_full_name)
rank_col = list(set(df.columns) & set(('Rank', 'Title', 'Current.Rank')))[0]
df[rank_col].value_counts(dropna=False)

if file_name in rank_df['File']:
    rank_df = rank_df[rank_df['File'] != file_name]

app_df = pd.DataFrame(df[rank_col].value_counts(dropna=False))
app_df['File'] = file_name[:-7]
app_df['Count'] = app_df[rank_col]
app_df['Rank'] = app_df.index
app_df['Column_Name'] = rank_col
rank_df = rank_df.append(app_df.reset_index(drop=True))
rank_df = rank_df[['Rank', 'Count', 'File', 'Column_Name']]
rank_df.to_csv(rank_file_name)
