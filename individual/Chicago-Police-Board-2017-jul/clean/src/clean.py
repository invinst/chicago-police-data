import numpy as np
import pandas as pd
import __main__

from cleaning_functions import clean_data

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
        'input_file': 'input/police-board.csv.gz',
        'output_file': 'output/police-board.csv.gz'
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
# Some CRID's have 2 listed, so these will be split into two
df['CRID2'] = df['CRID'].map(lambda x: x.split(' & ')[1]
                             if isinstance(x, str) and
                             len(x.split(' & ')) == 2
                             else np.nan)
df['CRID'] = df['CRID'].map(lambda x: x.split(' & ')[0]
                            if isinstance(x, str) else x)

df, conflicts_df = clean_data(df)
cons.write_yamlvar('Conflicts', conflicts_df)
df.to_csv(cons.output_file, **cons.csv_opts)
