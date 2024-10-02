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
        'input_file': 'input/complaints-accused-cms_2019-2021_2021-07.csv.gz',
        'input_profiles_file': 'input/complaints-accused-cms_2019-2021_2021-07_profiles.csv.gz',
        'output_file': 'output/complaints-accused-cms_2019-2021_2021-07.csv.gz',
        'output_profiles_file': 'output/complaints-accused-cms_2019-2021_2021-07_profiles.csv.gz',
        'non_police_ranks': [
            'MANAGER OF POLICE RECORDS',
            'DETENTION AIDE',
            'CLERK 3',
            'POLICE ADMINISTRATIVE CLERK',
            'PROPERTY CUSTODIAN',
            'INVESTIGATOR 3 COPA',
            'DOMESTIC VIOLENCE AD',
            'PROJECT STRATEGY MANAGER',
            'PERS ASSIST II',
            'MGR INT SECURE COMM',
            'SUPERVISING INV COPA'
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
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = pd.read_csv(cons.input_profiles_file)

no_profile_mask = profiles_df["first_name_NS"].isnull() | \
    profiles_df["last_name_NS"].isnull() | \
    profiles_df["employee_no"].isnull()

# remove non police ranks
no_profile_mask = no_profile_mask | profiles_df["rank"].isin(cons.non_police_ranks)
    
profiles_df.loc[no_profile_mask, 'merge'] = 0
profiles_df['merge'] = profiles_df['merge'].fillna(1)

profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)