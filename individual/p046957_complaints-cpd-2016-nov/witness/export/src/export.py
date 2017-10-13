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
        'input_file': 'input/witnesses.csv.gz',
        'input_demo_file': 'input/witnesses_demographics.csv.gz',
        'output_file': 'output/witnesses.csv.gz',
        'output_demo_file': 'output/witnesses_demographics.csv.gz',
        'export_cols': ['CR_ID'],
        'id': 'witnesses_ID',
        'notnull_col': ['first_name', 'appointed_date']
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
df = df[[cons.id] + cons.export_cols]
df.to_csv(cons.output_file, **cons.csv_opts)

demo_df = pd.read_csv(cons.input_demo_file)
initial_rows = demo_df.shape[0]
demo_df = demo_df.dropna(subset=[cons.notnull_col],
                         how='any', axis=0)
demo_df.reset_index(drop=True, inplace=True)
dropped_row_count = initial_rows - demo_df.shape[0]
log.info(('Dropping rows without any {0} data.\n'
          '{1} demographic rows dropped.').format(cons.notnull_col,
                                                  dropped_row_count))
demo_df.to_csv(cons.output_demo_file, **cons.csv_opts)
