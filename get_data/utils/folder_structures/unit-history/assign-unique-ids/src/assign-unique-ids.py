#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''assign-unique-ids script for unit-history__2016-03_16-1105'''

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
        'input_file': 'input/unit-history__2016-03.csv.gz',
        'output_file': 'output/unit-history__2016-03.csv.gz',
        'output_profiles_file': 'output/unit-history__2016-03_profiles.csv.gz',
        'id_cols': [
            "first_name", "last_name", "middle_initial",
            'middle_initial2', "suffix_name", 'current_age',
            "appointed_date", "gender",
            'first_name_NS', 'last_name_NS'
            ],
        'max_cols': [
            'race',
            'star1', 'star2', 'star3', 'star4', 'star5',
            'star6', 'star7', 'star8', 'star9', 'star10'
            ],
        'current_cols': ['unit'],
        'time_col': 'unit_start_date',
        'id': 'unit-history__2016-03_ID'
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

df= assign_unique_ids(df, cons.id, cons.id_cols, log=log)
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = aggregate_data(df, cons.id, cons.id_cols,
                             max_cols=cons.max_cols,
                             current_cols=cons.current_cols,
                             time_col=cons.time_col)
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
