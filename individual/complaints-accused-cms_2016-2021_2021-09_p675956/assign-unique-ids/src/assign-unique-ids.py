#!usr/bin/env python3
#
# Author(s):   Ashwin Sharma

'''assign-unique-ids script for complaints-accused_2000-2018_2018-07_18-060-294'''

import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
import setup
from functools import reduce


def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'input_file': 'input/complaints-accused-cms_2016-2021_2021-09_p675956.csv.gz',
        'output_file': 'output/complaints-accused-cms_2016-2021_2021-09_p675956.csv.gz',
        'output_profiles_file': 'output/complaints-accused-cms_2016-2021_2021-09_p675956_profiles.csv.gz',
        'id_cols': [
            'first_name_NS', 'last_name_NS', 
            'appointed_date', 'middle_initial', 'suffix_name',
            'race', 'gender'
            ],
        'incident_cols' : ['star', 'current_age'],
        'list_cols': ['cr_id', 'star'],
        'change_cols': ['last_name_NS', "appointed_date", "race", ('current_age', 'appointed_date'), 
            ('current_age', 'race'), ('current_age', 'last_name_NS')],
        'id': 'complaints-accused-cms_2016-2021_2021-09_p675956_ID',
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_csv(cons.input_file)

# get a baseline unique id by just looking at mostly stable columns as first step
# then look for instances where everything but one column is the same for separately assigned ids in the first step
# inspect possible changes manually then apply them
# NOTE: in particular, if you do inspect these changes, most of the time a field is missing, or the appointed date is just 1 day off
# for the 2 last name changes, they occur with females which is sensible, and have star matching between 
df = assign_unique_ids(df, cons.id, cons.id_cols,
                       log=log)

filtered_df = df.loc[(df.first_name != 'UNKNOWN') & (df.first_name != 'UNK') & 
                        (df.last_name != 'UNKNOWN') & (df.first_name != 'UNKNOWN') & 
                        (df.first_name != 'OFFICER') & (df.last_name != 'OFFICER'), 
                    [cons.id, 'cr_id'] + cons.id_cols + cons.incident_cols]\
    .dropna(subset=['first_name_NS'])\
    .drop_duplicates()

set_join = lambda x: ', '.join(set(x.astype(str)))

all_cols = cons.id_cols + cons.incident_cols

# leave one column out at a time, get possible changes within file
possible_changes = {}
for change_col in all_cols:
    id_cols = [col for col in all_cols if col != change_col]

    possible_changes[change_col] = filtered_df[all_cols + [cons.id]].fillna("") \
        .groupby(id_cols, as_index=False) \
        .agg(ids=(cons.id, set_join), 
            **{change_col: (change_col, set_join)}, 
            change_count=(cons.id, pd.Series.nunique)) \
        .assign(change_col=change_col,
                incident_col="") \
        .query("change_count > 1")

# leave 2 columns out: one column from main id columns + age
for incident_col in cons.incident_cols:
    for change_col in cons.id_cols:
        id_cols = [col for col in all_cols if (col != change_col) and (col != incident_col)]

        single_change_ids = possible_changes[change_col]['ids'].str.split(", ").explode().astype(int).values

        possible_changes[(incident_col, change_col)] = filtered_df \
            .loc[~filtered_df[cons.id].isin(single_change_ids), all_cols + [cons.id]] \
            .fillna("") \
            .groupby(id_cols, as_index=False) \
            .agg(ids=(cons.id, set_join),
                **{change_col: (change_col, set_join)},
                change_count=(cons.id, pd.Series.nunique),
                **{incident_col: (incident_col, set_join)},
                incident_count=(incident_col, pd.Series.nunique)) \
            .assign(change_col=change_col,
                    incident_col=incident_col) \
            .query("change_count > 1")

def apply_change(row, df):
    row_df = pd.DataFrame(row).T

    ids = row_df.ids.str.split(", ").explode().astype(int).values
    first_id = ids[0]
    other_ids = ids[1:]

    # set other ids to first id
    df.loc[df[cons.id].isin(other_ids), cons.id] = first_id
    return df

for change_cols in cons.change_cols:
    change_df = possible_changes[change_cols] 

    log.info(f"Applying {change_df.shape[0]} changes for {change_cols}")
    for idx, row in change_df.iterrows():
        df = apply_change(row, df)



# want to potentially merge on all variations of id cols back to reference, just dedupe with id cols and now fixed ids
profiles_df = df[[cons.id] + cons.id_cols + cons.incident_cols].drop_duplicates()

df.to_csv(cons.output_file, **cons.csv_opts)
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)