#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''export script for complaints-CPD-witnesses_2000-2018_2018-03_18-060-157'''

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
        'input_file': 'input/complaints-CPD-witnesses_2000-2018_2018-03.csv.gz',
        'input_profiles_file': 'input/complaints-CPD-witnesses_2000-2018_2018-03_profiles.csv.gz',
        'output_file': 'output/complaints-CPD-witnesses_2000-2018_2018-03.csv.gz',
        'output_profiles_file': 'output/complaints-CPD-witnesses_2000-2018_2018-03_profiles.csv.gz',
        'npor_file' : 'hand/nc_nonpo_ranks.yaml',
        'export_cols': [
           'cr_id', 'complaint_date', 'investigating_agency',
           'complainant_type', 'complainant_subtype', 'assigned_unit',
           'unit_detail', 'rank', 'is_sworn_officer',
           'current_unit_detail', 'current_unit',
           'complaints-CPD-witnesses_2000-2018_2018-03_ID'
            ],

        'id': 'complaints-CPD-witnesses_2000-2018_2018-03_ID'
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

with open(cons.npor_file, "r") as f:
    npo_ranks = yaml.load(f)

df = pd.read_csv(cons.input_file)
df = df[['row_id',cons.id] + cons.export_cols]
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = pd.read_csv(cons.input_profiles_file)
rows = profiles_df.shape[0]
profiles_df = profiles_df[~(profiles_df['rank'].isin(npo_ranks) |
                            profiles_df['rank'].isnull())]
log.info("Dropped %d rows given NA or non-PO ranks" % (rows - profiles_df.shape[0]))
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
