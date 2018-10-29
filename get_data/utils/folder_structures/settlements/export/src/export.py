#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''export script for settlements_1952-2016_2017-01_'''

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
        'input_file': 'input/settlements_1952-2016_2017-01.csv.gz',
        'input_profiles_file': 'input/settlements_1952-2016_2017-01_profiles.csv.gz',
        'output_file': 'output/settlements_1952-2016_2017-01.csv.gz',
        'output_profiles_file': 'output/settlements_1952-2016_2017-01_profiles.csv.gz',
        'export_cols': [
            'case_id', 'officer_id', 'complaint', 'incident_date',
            'location', 'plantiff', 'settlement', "service_length"
            ],
        'id': 'settlements_1952-2016_2017-01_ID'
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
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = pd.read_csv(cons.input_profiles_file)
profiles_df.loc[profiles_df["first_name_NS"].isnull() |
                profiles_df["last_name_NS"].isnull(),
                "merge"] = 0
profiles_df["merge"] = profiles_df["merge"].fillna(1)
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
