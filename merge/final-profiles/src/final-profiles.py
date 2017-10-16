import pandas as pd
import yaml
import __main__

from merge_functions import generate_profiles
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
        'input_file': 'input/officer-reference.csv.gz',
        'FOIA_file': 'hand/FOIA_dates.yaml',
        'output_file': 'output/final-profiles.csv.gz',
        'universal_id': 'UID',
        'column_order': [
            'first_name', 'last_name',
            'middle_initial', 'middle_initial2', 'suffix_name',
            'birth_year', 'race', 'gender',
            'appointed_date', 'resignation_date',
            'current_status', 'current_age', 'current_star',
            'current_unit', 'current_rank'
            ],
        'max_cols': ['resignation_date'],
        'current_cols': [
            'current_star', 'current_age', 'current_unit',
            'current_rank', 'current_status'
            ],
        'time_col': 'FOIA_date',
        'mode_cols': [
            'first_name', 'last_name', 'middle_initial',
            'middle_initial2', 'suffix_name', 'race', 'gender',
            'birth_year', 'appointed_date'
            ]
        }

    assert args['input_file'] == 'input/officer-reference.csv.gz',\
        'Input file is not correct.'
    assert args['output_file'] == 'output/final-profiles.csv.gz',\
        'Output file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.read_csv(cons.input_file)

with open(cons.FOIA_file, 'r') as f:
    FOIA_dict = yaml.load(f)
for fid, fdate in FOIA_dict.items():
    if fid in ref_df.columns:
        ref_df.loc[ref_df[fid].notnull(), 'FOIA_date'] = fdate

assert ref_df[ref_df['FOIA_date'].isnull()].shape[0] == 0,\
    "Some IDs are missing FOIA dates"

profile_df = generate_profiles(ref_df, cons.universal_id,
                               mode_cols=cons.mode_cols,
                               max_cols=cons.max_cols,
                               current_cols=cons.current_cols,
                               time_col=cons.time_col,
                               column_order=cons.column_order,
                               include_IDs=False)

log.info('Officer profile count: {}'.format(profile_df.shape[0]))

profile_df.to_csv(cons.output_file, **cons.csv_opts)
