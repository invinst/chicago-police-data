import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
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
        'input_file': 'input/trr-officers.csv.gz',
        'output_file': 'output/trr-officers.csv.gz',
        'output_demo_file': 'output/trr-officers_demographics.csv.gz',
        'id_cols': [
            "First.Name", "Last.Name", "Middle.Initial", "Suffix.Name",
            "Appointed.Date", "Gender", "Race", "Current.Star"
            ],
        'id': 'trr-officers_ID'
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

df, uid_report = assign_unique_ids(df, cons.id, cons.id_cols)
cons.write_yamlvar('UID Report', uid_report)
df.to_csv(cons.output_file, **cons.csv_opts)

agg_df = aggregate_data(df, cons.id, cons.id_cols)
agg_df.to_csv(cons.output_demo_file, **cons.csv_opts)
