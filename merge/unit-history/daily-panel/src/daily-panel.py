import pandas as pd
import __main__

from unit_history_functions import history_to_panel
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
        'input_file': 'input/unit-history.csv.gz',
        'output_file': 'output/daily-unit-panel.csv.gz',
        'max_date': '',
        'min_date': '2000-01-01',
        'uid_col': 'UID',
        'unit_col': 'Unit',
        'start_col': 'Start.Date',
        'end_col': 'End.Date'
        }

    assert args['input_file'].startswith('input/') and \
           args['input_file'].endswith('.csv.gz'),\
        'Input file, {}, is not correct.'.format(args['input_file'])
    assert args['output_file'].startswith('output/') and \
           args['output_file'].endswith('.csv.gz'),\
        'Output file, {}, is not correct.'.format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

unit_df_list = [pd.read_csv(input_file)
                for input_file in cons.input_unit_files]

resignation_df = pd.read_csv(cons.input_resignation_file)
resignation_df = resignation_df[[cons.uid_col, cons.resignation_col]]

uh_df = combine_histories(unit_df_list,
                          resignation_df,
                          uid_col=cons.uid_col,
                          unit_col=cons.unit_col,
                          start_col=cons.start_col,
                          end_col=cons.end_col,
                          resignation_col=cons.resignation_col)
print('{} unique unit movement events'.format(uh_df.shape[0]))
uh_df.to_csv(cons.output_file, **cons.csv_opts)
