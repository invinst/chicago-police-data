#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''export script for complaints-investigators_2000-2016_2016-11_p046957'''

import pandas as pd
import __main__

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
        'input_file': 'input/complaints-investigators_2000-2016_2016-11.csv.gz',
        'input_profiles_file': 'input/complaints-investigators_2000-2016_2016-11_profiles.csv.gz',
        'output_file': 'output/complaints-investigators_2000-2016_2016-11.csv.gz',
        'output_profiles_file': 'output/complaints-investigators_2000-2016_2016-11_profiles.csv.gz',
        'export_cols': [
            'cr_id', 'first_name', 'last_name', 'middle_initial',
            'suffix_name', 'appointed_date', 'current_star',
            'current_rank', 'current_unit'
            ],
        'id': 'complaints-investigators_2000-2016_2016-11_ID',
        'keep_ranks': [
            'LIEUTENANT OF POLICE', 'SERGEANT OF POLICE',
            'CAPTAIN OF POLICE', 'POLICE OFFICER',
            'POLICE AGENT', 'PO AS DETECTIVE',
            'PO LEGAL OFF 2', 'PO/MOUNTED PAT OFF.',
            'PO/FIELD TRNING OFF', 'PO ASGN EVID. TECHNI',
            'PO/MARINE OFFICER', 'PO ASSG CANINE HANDL',
            'CHIEF', 'DEP CHIEF', 'COMMANDER'
            ]
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
df = df[['row_id', cons.id] + cons.export_cols]
drop_ids = df[~df['current_rank'].isin(cons.keep_ranks)][cons.id].unique()
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = pd.read_csv(cons.input_profiles_file)
profiles_df.loc[profiles_df[cons.id].isin(drop_ids), 'merge'] = 0
profiles_df['merge'] = profiles_df['merge'].fillna(1)
log.info('{0} rows in profiles marked for not merging.'.format(len(drop_ids)))
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
