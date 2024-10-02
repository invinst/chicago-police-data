#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''assign-unique-ids script for complaints-accused_2000-2018_2018-07_18-060-294'''

import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
import setup


def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'input_file': 'input/complaints-accused-cms_2019-2021_2021-07.csv.gz',
        'output_file': 'output/complaints-accused-cms_2019-2021_2021-07.csv.gz',
        'output_profiles_file': 'output/complaints-accused-cms_2019-2021_2021-07_profiles.csv.gz',
        'id_cols': [
            'first_name', 'last_name', 'first_name_NS', 'last_name_NS',
            'middle_initial','middle_initial2', 'suffix_name',
            'birth_year', 'race', 'gender', 'employee_no', 'appointed_date'
            ],
        'incident_cols' : ['rank', 'current_unit', 'star'],
        'list_cols': ['cr_id'],
        'id': 'complaints-accused-cms_2019-2021_2021-07_ID',
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

# copy row id
df[cons.id] = df['row_id']

# id to employee number mappping: want to have sequential ids, but also use employee number to validate
id_mapping = df[['employee_no', cons.id]].groupby("employee_no")[cons.id].agg([list, pd.Series.nunique]).query('nunique > 1')
id_mapping['id'] = id_mapping.list.str[0]
id_mapping['list'] = id_mapping.list.str[1:]

for _, row in id_mapping.iterrows():
    df.loc[df[cons.id].isin(row.list), cons.id] = row['id']

df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = aggregate_data(df, cons.id, 
                            cons.id_cols, 
                            max_cols=cons.incident_cols,
                            list_cols=cons.list_cols)
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
