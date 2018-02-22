#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''assign-unique-ids script for roster_1936-2017_2017-04_p058155'''

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
        'input_file': 'input/roster_1936-2017_2017-04.csv.gz',
        'output_file': 'output/roster_1936-2017_2017-04.csv.gz',
        'output_profiles_file': 'output/roster_1936-2017_2017-04_profiles.csv.gz',
        'id_cols': [
            "first_name", "last_name", "first_name_NS", "last_name_NS",
            'suffix_name', "appointed_date", "gender",
            'birth_year', 'current_age', 'resignation_date', 'current_rank'
            ],
        'conflict_cols': [
            'middle_initial', 'middle_initial2',
            'star1', 'star2', 'star3', 'star4', 'star5',
            'star6', 'star7', 'star8', 'star9', 'star10'
            ],
        'max_cols': ['current_status', 'current_unit', 'race'],
        'merge_cols': ['unit_description'],
        'merge_on_cols': ['current_unit'],
        'id': 'roster_1936-2017_2017-04_ID'
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

po_df = df[df['first_name'] == 'POLICE']
log.info('{} hidden officers marked as merge = 0'.format(po_df.shape[0]))
log.info(('{} officer with no name marked as merge = 0'
          '').format(df[df['first_name'].isnull()].shape[0]))

df.loc[(df['first_name'].notnull()) &
       (df['first_name'] != 'POLICE'), 'merge'] = 1
df['merge'] = df['merge'].fillna(0)

df = assign_unique_ids(df, cons.id, cons.id_cols + ['merge'],
                       conflict_cols=cons.conflict_cols,
                       log=log)

df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = aggregate_data(df, cons.id, cons.id_cols + ['merge'],
                             max_cols=cons.max_cols + cons.conflict_cols,
                             merge_cols=cons.merge_cols,
                             merge_on_cols=cons.merge_on_cols)

profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
