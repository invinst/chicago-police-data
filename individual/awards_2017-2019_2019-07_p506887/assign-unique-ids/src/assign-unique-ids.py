#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''assign-unique-ids script for awards_1967-2017_2017-08_p061715'''

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
        'input_file': 'input/awards_2017-2019_2019-07_p506887.csv.gz',
        'output_file': 'output/awards_2017-2019_2019-07_p506887.csv.gz',
        'output_demo_file': 'output/awards_2017-2019_2019-07_p506887_profiles.csv.gz',
        'id_cols': [
            "first_name", "last_name",
            "first_name_NS", "last_name_NS"
            ],
        'conflict_cols': [
            'appointed_date', 'gender', 'race',
            'middle_initial', 'middle_initial2',
            'birth_year', 'current_star', 'resignation_date'
            ],
        'id': 'awards_2017-2019_2019-07_p506887_ID'
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

df = assign_unique_ids(df, cons.id,
                       cons.id_cols,
                       cons.conflict_cols,
                       log=log)
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = aggregate_data(df, cons.id, cons.id_cols,
                             max_cols=cons.conflict_cols)
profiles_df.to_csv(cons.output_demo_file, **cons.csv_opts)
