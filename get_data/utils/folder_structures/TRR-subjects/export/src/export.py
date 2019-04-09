#!usr/bin/env python3
#


'''export script for TRR-subjects_2004-2016_2016-09_p046360'''

import pandas as pd
import __main__
import sys


import setup


def create_metadata_filename(filename):
    file_split = filename.split('/')
    return file_split[0] + '/metadata_' + file_split[1]



def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'input_file': sys.argv[1],
        'input_profiles_file': 'input/TRR-subjects_2004-2016_2016-09_profiles.csv.gz',
        'output_file': sys.argv[2],
        'output_profiles_file': 'output/TRR-subjects_2004-2016_2016-09_profiles.csv.gz',
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
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
