#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''export script for awards_1967-2017_2017-08_p061715'''

import pandas as pd
import __main__
import yaml

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
        'input_file': 'input/awards_1967-2017_2017-08.csv.gz',
        'input_profiles_file': 'input/awards_1967-2017_2017-08_profiles.csv.gz',
        'output_file': 'output/awards_1967-2017_2017-08.csv.gz',
        'output_profiles_file': 'output/awards_1967-2017_2017-08_profiles.csv.gz',
        'export_cols': [
            'pps_award_detail_id', 'award_type', 'award_start_date',
            'current_award_status', 'award_request_date',
            'award_end_date', 'rank', 'last_promotion_date',
            'requester_full_name', 'ceremony_date', 'tracking_no'
            ],
        'id': 'awards_1967-2017_2017-08_ID'
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

with open("hand/award_po_ranks.yaml", "r") as f:
    po_ranks = yaml.load(f)
with open("hand/maybe_po_ranks.yaml", "r") as f:
    maybe_po_ranks = yaml.load(f)

po_ids = df.loc[(df['rank'].isin(po_ranks)) |
                ((df['rank'].isin(maybe_po_ranks)) &
                 (df['appointed_date'] < "2010-01-01")),
                cons.id].unique()

df_rows = df.shape[0]
df = df[['row_id', cons.id] + cons.export_cols]
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = pd.read_csv(cons.input_profiles_file)
profiles_df.loc[profiles_df[cons.id].isin(po_ids), 'merge'] = 1
profiles_df['merge'] = profiles_df['merge'].fillna(0)
log.info('%d IDs with PO ranks marked for merging', len(po_ids))
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
