#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''export script for TRR-officers_2004-2016_2016-09_p046360'''

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
        'input_file': 'input/TRR-officers_2004-2016_2016-09.csv.gz',
        'input_profiles_file': 'input/TRR-officers_2004-2016_2016-09_profiles.csv.gz',
        'output_file': 'output/TRR-officers_2004-2016_2016-09.csv.gz',
        'output_profiles_file': 'output/TRR-officers_2004-2016_2016-09_profiles.csv.gz',
        'export_cols': [
            'trr_id', 'injured', 'unit', 'rank',
            'in_uniform', 'unit_detail',
            'duty_status', 'assigned_beat'
            ],
        'id': 'TRR-officers_2004-2016_2016-09_ID',
        'drop_ranks': ['DETENTION AIDE']
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
drop_ids = df[df['rank'].isin(cons.drop_ranks)][cons.id]
df = df[['row_id', cons.id] + cons.export_cols]
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = pd.read_csv(cons.input_profiles_file)
profiles_df.loc[profiles_df[cons.id].isin(drop_ids), 'merge'] = 0
profiles_df['merge'] = profiles_df['merge'].fillna(1)
log.info(('{} ids marked for not merging'
          '').format(profiles_df[profiles_df['merge'] == 0].shape[0]))
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
