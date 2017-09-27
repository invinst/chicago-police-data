import pandas as pd
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
        'FOIA_file': 'hand/FOIA_dictionary.csv',
        'output_file': 'output/final-profiles.csv.gz',
        'universal_id': 'UID',
        'column_order': [
            'First.Name', 'Last.Name',
            'Middle.Initial', 'Middle.Initial2', 'Suffix.Name',
            'Birth.Year', 'Race', 'Gender',
            'Appointed.Date', 'Resignation.Date',
            'Current.Status', 'Current.Age', 'Current.Star',
            'Current.Unit', 'Current.Rank'
            ],
        'max_cols': ['Resignation.Date'],
        'current_cols': [
            'Current.Star', 'Current.Age', 'Current.Unit',
            'Current.Rank', 'Current.Status'
            ],
        'time_col': 'FOIA_date',
        'mode_cols': [
            'First.Name', 'Last.Name', 'Middle.Initial',
            'Middle.Initial2', 'Suffix.Name', 'Race', 'Gender',
            'Birth.Year', 'Appointed.Date'
            ]
        }

    assert args['input_file'] == 'input/officer-reference.csv.gz',\
        'Input file is not correct.'
    assert args['output_file'] == 'output/final-profiles.csv.gz',\
        'Output file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.read_csv(cons.input_file)

FOIA_dict = pd.read_csv(cons.FOIA_file)
FOIA_dict = dict(zip(FOIA_dict.ID_name, FOIA_dict.FOIA_date))
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

profile_df.to_csv(cons.output_file, **cons.csv_opts)
