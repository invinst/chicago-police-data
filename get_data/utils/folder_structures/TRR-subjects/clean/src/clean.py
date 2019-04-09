#!usr/bin/env python3
#


'''clean script for TRR-subjects_2004-2016_2016-09_p046360'''

import pandas as pd
import numpy as np
import __main__
import sys


from clean_functions import clean_data
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
        'output_file': sys.argv[2],
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
log.info('Ensuring birth_year is max of birth_year and age, while age is min. '
         '%d rows changed' % df[df['age'] > df['birth_year']].shape[0])
new_ages = df[['birth_year', 'age']].apply(min, axis=1)
new_bys = df[['birth_year', 'age']].apply(max, axis=1)
df['age'] = new_ages
df['birth_year'] = new_bys
df['birth_year'] = df['birth_year'].map(lambda x: x if x > 1900 else np.nan)
df['age'] = df['age'].map(lambda x: x if x < 100 else np.nan)
log.info('Ensuring replacing ages >= 100 and birth_years <= 1900 with NA. '
         '%d rows changed' % df[(df['age'].isnull()) | (df['birth_year'].isnull())].shape[0])
df = clean_data(df, log)
df.to_csv(cons.output_file, **cons.csv_opts)
