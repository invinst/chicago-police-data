#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''assign-unique-ids script for complaints-investigators_2000-2018_2018-07_18-060-157'''

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
        'input_file': 'input/complaints-investigators_2000-2018_2018-07.csv.gz',
        'output_file': 'output/complaints-investigators_2000-2018_2018-07.csv.gz',
        'output_profiles_file': 'output/complaints-investigators_2000-2018_2018-07_profiles.csv.gz',
        'id_cols': [
            'first_name', 'last_name', 'first_name_NS', 'last_name_NS',
            'middle_initial', 'suffix_name',
            'birth_year', 'race', 'gender', 'appointed_date', 'current_unit'
            ],
        'incident_cols' : ['star'],
        'id': 'complaints-investigators_2000-2018_2018-07_ID',
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

df = assign_unique_ids(df, cons.id, cons.id_cols,
                       log=log)
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = df[[cons.id] + cons.id_cols + cons.incident_cols]\
    .drop_duplicates()\
    .to_csv(cons.output_profiles_file, **cons.csv_opts)
