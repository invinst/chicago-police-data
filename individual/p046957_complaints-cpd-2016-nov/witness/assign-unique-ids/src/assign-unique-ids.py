#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''assign-unique-ids script for p046957_complaints-cpd-2016-nov/witness'''

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
        'input_file': 'input/witnesses.csv.gz',
        'output_file': 'output/witnesses.csv.gz',
        'output_demo_file': 'output/witnesses_demographics.csv.gz',
        'id_cols': ['first_name_NS', 'last_name_NS'],
        'conflict_cols': [
            'middle_initial', 'middle_initial2', 'suffix_name',
            'appointed_date', 'birth_year', 'current_star'
            ],
        'max_cols': ['first_name', 'last_name'],
        'id': 'witnesses_ID',
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

df, uid_report = assign_unique_ids(df, cons.id, cons.id_cols,
                                   cons.conflict_cols)
log.info(uid_report)
df.to_csv(cons.output_file, **cons.csv_opts)

agg_df = aggregate_data(df, cons.id, cons.id_cols,
                        max_cols=cons.conflict_cols + cons.max_cols)
agg_df.to_csv(cons.output_demo_file, **cons.csv_opts)
