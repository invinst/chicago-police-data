#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''export script for complaints-complaints-clear_2000-2021_2021-07'''

import pandas as pd
import yaml
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
        'input_file': 'input/complaints-accused-clear_2000-2021_2021-07.csv.gz',
        'input_profiles_file': 'input/complaints-accused-clear_2000-2021_2021-07_profiles.csv.gz',
        'output_file': 'output/complaints-accused-clear_2000-2021_2021-07.csv.gz',
        'output_profiles_file': 'output/complaints-accused-clear_2000-2021_2021-07_profiles.csv.gz'
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
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = pd.read_csv(cons.input_profiles_file)

with open("hand/nc_nonpo_ranks.yaml") as f:
    non_po_ranks = yaml.safe_load(f)

no_merge_mask = profiles_df['current_rank'].isin(non_po_ranks)
profiles_df.loc[no_merge_mask, 'merge'] = 0
profiles_df['merge'].fillna(1, inplace=True)

profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)