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
        'input_file': 'input/police-board.csv.gz',
        'accused_file': 'input/accused.csv.gz',
        'output_file': 'output/police-board.csv.gz',
        'output_full_file': 'output/full_police-board.csv.gz',
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

full_df = pd.read_csv(cons.input_file)
full_df.to_csv(cons.output_full_file, **cons.csv_opts)

acc_df = pd.read_csv(cons.accused_file)
df = full_df[((full_df['CRID'].notnull()) &
              (full_df['CRID'].isin(acc_df['CRID']))) |
             ((full_df['CRID2'].notnull()) &
              (full_df['CRID2'].isin(acc_df['CRID'])))]
df.to_csv(cons.output_file, **cons.csv_opts)
