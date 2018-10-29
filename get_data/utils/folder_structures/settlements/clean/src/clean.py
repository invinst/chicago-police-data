#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''clean script for settlements_1952-2016_2017-01_'''

import pandas as pd
import numpy as np
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
        'input_file': 'input/settlements_1952-2016_2017-01.csv.gz',
        'output_file': 'output/settlements_1952-2016_2017-01.csv.gz'
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
log.info("Custom cleaning on service_length")
def split_service_length(x):
    l = [np.nan, np.nan, np.nan]
    if isinstance(x, str):
        sx = x.split(' ')
        l[0] = int(sx[0])
        l[1] = int(sx[2])
        l[2] = 1 if "(active)" in x else 0
        return pd.Series(l)
    else:
        return pd.Series(l)
df[["service_years", "service_months", "current_status"]] = \
    df['service_length'].apply(split_service_length)
df = clean_data(df, log)
df.to_csv(cons.output_file, **cons.csv_opts)
