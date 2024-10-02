#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''clean script for complaints-complainants-clear_2000-2020_2020-07'''


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
        'input_file': 'input/complaints-complainants-clear_2000-2020_2020-07.csv.gz',
        'output_file': 'output/complaints-complainants-clear_2000-2020_2020-07.csv.gz'
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
         
    df.to_csv(cons.output_file, **cons.csv_opts)     