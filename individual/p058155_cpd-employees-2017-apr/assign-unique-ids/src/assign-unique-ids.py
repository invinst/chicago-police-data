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
        'input_file': 'input/cpd-employees.csv.gz',
        'output_file': 'output/cpd-employees.csv.gz',
        'output_demo_file': 'output/cpd-employees_demographics.csv.gz',
        'id_cols': [
            "first_name", "last_name", "first_name_NS", "last_name_NS",
            'suffix_name', "appointed_date", "gender", "race",
            'birth_year', 'current_age', 'resignation_date'
            ],
        'conflict_cols': [
            'middle_initial', 'middle_initial2',
            'star1', 'star2', 'star3', 'star4', 'star5',
            'star6', 'star7', 'star8', 'star9', 'star10'
            ],
        'max_cols': ['current_status', 'current_unit'],
        'merge_cols': ['unit_description'],
        'merge_on_cols': ['current_unit'],
        'id': 'cpd-employees_ID'
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

po_df = df[df['first_name'] == 'POLICE']
log.info('{} hidden officers dropped from data'.format(po_df.shape[0]))
log.info(('{} officer with no name dropped from data'
          '').format(df[df['first_name'].isnull()].shape[0]))

df = df[(df['first_name'].notnull()) &
        (df['first_name'] != 'POLICE')]

df, uid_report = assign_unique_ids(df, cons.id, cons.id_cols,
                                   conflict_cols=cons.conflict_cols)

log.info(uid_report)
df.to_csv(cons.output_file, **cons.csv_opts)

agg_df = aggregate_data(df, cons.id, cons.id_cols,
                        max_cols=cons.max_cols + cons.conflict_cols,
                        merge_cols=cons.merge_cols,
                        merge_on_cols=cons.merge_on_cols)

agg_df.to_csv(cons.output_demo_file, **cons.csv_opts)
