#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''clean script for roster-inactive_2015-2024_2024-05_p944287'''

import pandas as pd
import __main__

from clean_functions import clean_data
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
        'input_file': 'input/roster-inactive_2015-2024_2024-05_p944287.csv.gz',
        'output_file': 'output/roster-inactive_2015-2024_2024-05_p944287.csv.gz'
        }
     
    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_csv(cons.input_file)
         
    df = clean_data(df, log)

    df['rank'] = df['rank'].str.split(" - ").str[1]

    # fill in missing resignation dates if vacate effective date is present
    df['resignation_date'] = df['resignation_date'].fillna(df['vacate_effective_date'])

    # flip all statuses to not active: those labelled as active, were at one point officers,
    # but are now in civilian roles, see header sheet in input
    df['current_status'] = 'N'
         
    df.to_csv(cons.output_file, **cons.csv_opts)     