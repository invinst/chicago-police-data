import pandas as pd
import os

id_cols = ['first_name', 'last_name', 'suffix_name', 'first_name_NS', 'last_name_NS', 'appointed_date', 'birth_year', 'gender', 'race']

old_df = pd.read_csv('../output/officer-reference.csv.gz')
new_df = pd.read_csv('../output/officer-reference_old.csv.gz')

old_data = pd.read_csv("../output/complaints-accused_2000-2018_2018-07_old.csv.gz")
new_data = pd.read_csv("../output/complaints-accused_2000-2018_2018-07.csv.gz")

data_id = "complaints-accused_2000-2018_2018-07_ID"
cols = ["UID", data_id, 'matched_on']

diffs = old_df[cols].drop_duplicates() \
    .merge(new_df[cols].drop_duplicates(), how="outer", indicator=True) \
    .query("_merge != 'both'") 

old_diff = diffs.query("_merge == 'left_only'").drop(["_merge"], axis=1)
new_diff = diffs.query("_merge == 'right_only'").drop(["_merge"], axis=1)

crosswalk = new_diff.merge(old_diff, left_on=data_id, right_on=data_id, how="outer", suffixes=["_new", "_old"]) \
    .merge(new_df[id_cols+["UID"]].drop_duplicates().rename(columns=lambda x: f"{x}_new"), left_on="UID_new", right_on="UID_new") \
    .merge(old_df[id_cols+["UID"]].drop_duplicates().rename(columns=lambda x: f"{x}_old"), left_on="UID_old", right_on="UID_old", how="left") \

import pdb; pdb.set_trace()

