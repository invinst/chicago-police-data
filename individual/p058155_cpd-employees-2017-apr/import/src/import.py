import pandas as pd
import __main__

from import_functions import standardize_columns, collect_metadata
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
        'input_file': 'input/P058155_-_Kiefer.xlsx',
        'output_file': 'output/cpd-employees.csv.gz',
        'metadata_file': 'output/metadata_cpd-employees.csv.gz',
        'drop_column' : 'Star 10',
        'custom_columns': {' ': 'Current.Unit',
                           'Description': 'Rank',
                           'Star 9': 'Star 10',
                           'Star 8': 'Star 9',
                           'Star 7': 'Star 8',
                           'Star 6.1': 'Star 7'}
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_excel(cons.input_file)
del df[cons.drop_column]
print(('Column names changed'
      ' before standardization: {}').format(cons.custom_columns))
df.rename(columns=cons.custom_columns, inplace=True)
df.columns = standardize_columns(df.columns)
df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
