#!usr/bin/env python3
#
# Author(s):   Ashwin Sharma (Invisible Institute)

'''export script for TRR-officers_2004-2018_2018-08_p456008'''

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
        'input_file': 'input/TRR-officers_2004-2018_2018-08_p456008.csv.gz',
        'input_profiles_file': 'input/TRR-officers_2004-2018_2018-08_p456008_profiles.csv.gz',
        'output_file': 'output/TRR-officers_2004-2018_2018-08_p456008.csv.gz',
        'output_profiles_file': 'output/TRR-officers_2004-2018_2018-08_p456008_profiles.csv.gz'
        }
    

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_csv(cons.input_file)
    profiles_df = pd.read_csv(cons.input_profiles_file)
    
    df.to_csv(cons.output_file, **cons.csv_opts)

    # do not merge detention aides
    detention_aide_mask = profiles_df['current_rank'].isin(["DETENTION AIDE", "9122"])
    profiles_df.loc[detention_aide_mask, "merge"] = 0
    profiles_df['merge'] = profiles_df['merge'].fillna(1)

    profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
    