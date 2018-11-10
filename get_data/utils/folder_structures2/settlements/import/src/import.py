#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''import script for settlements_1952-2016_2017-01_'''

import pandas as pd
import __main__

from import_functions import collect_metadata
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
        'input_file': 'input/settlements.csv',
        'output_file': 'output/settlements_1952-2016_2017-01.csv.gz',
        'metadata_file': 'output/metadata_settlements_1952-2016_2017-01.csv.gz'
        }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()


df = pd.read_csv(cons.input_file)
df.drop(["Unnamed: 0", "oid"], axis=1, inplace=True)
df.insert(0, 'row_id', df.index+1)
df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
