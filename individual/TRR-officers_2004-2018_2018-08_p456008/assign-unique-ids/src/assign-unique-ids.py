#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''assign-unique-ids script for TRR-officers_2004-2018_2018-08_p456008

NOTE: stars are actually just current stars despite age being point in time
manually checked differences, these are all separate individuals
'''


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
        'input_file': 'input/TRR-officers_2004-2018_2018-08_p456008.csv.gz',
        'output_file': 'output/TRR-officers_2004-2018_2018-08_p456008.csv.gz',
        'output_profiles_file': 'output/TRR-officers_2004-2018_2018-08_p456008_profiles.csv.gz',
        'id_cols': [
            "first_name", "last_name", "first_name_NS", "last_name_NS",
            "middle_initial", 'middle_initial2', "suffix_name",
            "appointed_date", "gender", "race", 'current_star'
            ],
        'current_cols': ['rank', 'unit'],
        'time_col': 'trr_date',
        'id': 'TRR-officers_2004-2018_2018-08_p456008_ID',
        }
    

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    df = pd.read_csv(cons.input_file)

    df = assign_unique_ids(df, cons.id, cons.id_cols,
                        log=log)
    df.to_csv(cons.output_file, **cons.csv_opts)
    
    agg_df = aggregate_data(df, cons.id, cons.id_cols, current_cols=cons.current_cols, 
                            time_col=cons.time_col)

    # convert age as of trr date to possible ages for merging (as of 2017 as merge function uses)
    df['trr_date'] = pd.to_datetime(df['trr_date'])

    df['possible_age'] = 2017 - df['trr_date'].dt.year + df['age']
    min_max_ages = df.groupby(cons.id).agg(current_age_m1=('possible_age', min), current_age_p1=('possible_age', max))

    same_mask = min_max_ages['current_age_m1'] == min_max_ages['current_age_p1']
    min_max_ages.loc[same_mask, 'current_age_p1'] = min_max_ages.loc[same_mask, 'current_age_p1'] + 1

    agg_df = agg_df.merge(min_max_ages, how='left', left_on=cons.id, right_index=True)

    agg_df.to_csv(cons.output_profiles_file, **cons.csv_opts)