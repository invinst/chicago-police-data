#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''assign-unique-ids script for TRR-officers_2020-2021_2021-06_p660692'''

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
        'input_file': 'input/TRR-officers_2020-2021_2021-06_p660692.csv.gz',
        'output_file': 'output/TRR-officers_2020-2021_2021-06_p660692.csv.gz',
        'output_profiles_file': 'output/TRR-officers_2020-2021_2021-06_p660692_profiles.csv.gz',
        'id_cols': [
            "first_name", "last_name", "first_name_NS", "last_name_NS",
            "middle_initial", 'middle_initial2', "suffix_name", "birth_year",
            "appointed_date", "gender", "race"
            ],
        'current_cols': ['rank', 'unit'],
        'time_col': 'trr_date',
        'id': 'TRR-officers_2020-2021_2021-06_p660692_ID'
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

star_cols = [col for col in df if 'star' in col]

agg_df = aggregate_data(df, cons.id, cons.id_cols + star_cols, 
                        current_cols=cons.current_cols,
                        time_col=cons.time_col)
agg_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
