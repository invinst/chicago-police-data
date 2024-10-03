#!usr/bin/env python3
#
# Author(s):   Ashwin Sharma (Invisible Institute)

'''export script for complaints-accused_2000-2023_2023-09_p867849'''

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
        'input_file': 'input/complaints-accused_2000-2023_2023-09_p867849.csv.gz',
        'input_profiles_file': 'input/complaints-accused_2000-2023_2023-09_p867849_profiles.csv.gz',
        'output_file': 'output/complaints-accused_2000-2023_2023-09_p867849.csv.gz',
        'output_profiles_file': 'output/complaints-accused_2000-2023_2023-09_p867849_profiles.csv.gz'
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
               
    no_merge_mask = profiles_df['first_name_NS'].isnull() | profiles_df['last_name_NS'].isnull()

    with open("hand/not_officer_names.yaml", 'r') as f:
        not_officer_names = yaml.safe_load(f)

    no_merge_mask = no_merge_mask | profiles_df['first_name_NS'].isin(not_officer_names)

    profiles_df.loc[no_merge_mask, "merge"] = 0

    df.to_csv(cons.output_file, **cons.csv_opts)
    profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)     