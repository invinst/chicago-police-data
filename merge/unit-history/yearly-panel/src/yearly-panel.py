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
        'output_file': 'output/yearly-unit-panel.csv.gz',
        'frequency': 'Year',
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

history_df = pd.read_csv(cons.input_file)

panel_df = history_to_panel(history_df,
                            frequency=cons.frequency,
                            max_date=cons.max_date,
                            min_date=cons.min_date,
                            uid_col=cons.uid_col,
                            unit_col=cons.unit_col,
                            start_col=cons.start_col,
                            end_col=cons.end_col)
print('Shape of panel data: {}'.format(panel_df.shape))
panel_df.to_csv(cons.output_file, **cons.csv_opts)
