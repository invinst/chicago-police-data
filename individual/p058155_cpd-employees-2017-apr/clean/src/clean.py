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
        'input_file': 'input/cpd-employees.csv.gz',
        'output_file': 'output/cpd-employees.csv.gz'
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
df, conflicts_df = clean_data(df)
print('Current.Status column replaced: Y to 1, N to 0')
df['Current.Status'].replace({'Y': 1, 'N': 0}, inplace=True)
cons.write_yamlvar('Conflicts', conflicts_df)
df.to_csv(cons.output_file, **cons.csv_opts)
