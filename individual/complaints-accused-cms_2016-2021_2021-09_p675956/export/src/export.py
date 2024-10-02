#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''export script for complaints-complaints_2000-2018_2018-07_18-060-294'''

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
        'input_file': 'input/complaints-accused-cms_2016-2021_2021-09_p675956.csv.gz',
        'input_profiles_file': 'input/complaints-accused-cms_2016-2021_2021-09_p675956_profiles.csv.gz',
        'output_file': 'output/complaints-accused-cms_2016-2021_2021-09_p675956.csv.gz',
        'output_profiles_file': 'output/complaints-accused-cms_2016-2021_2021-09_p675956_profiles.csv.gz'
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

profiles_df.loc[(profiles_df.first_name_NS == 'UNKNOWN') | (profiles_df.first_name_NS == 'UNK') |
                (profiles_df.last_name_NS == 'UNKNOWN') | (profiles_df.first_name_NS == 'UNKNOWN') |
                (profiles_df.first_name_NS == 'OFFICER') | (profiles_df.last_name_NS == 'OFFICER') |
                (profiles_df.first_name_NS.isnull()) | (profiles_df.last_name_NS.isnull()), "merge"] = 0

profiles_df["merge"] = profiles_df["merge"].fillna(1)

profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)