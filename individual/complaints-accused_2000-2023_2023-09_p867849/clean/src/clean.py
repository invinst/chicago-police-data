#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''clean script for complaints-accused_2000-2023_2023-09_p867849'''

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
        'input_file': 'input/complaints-accused_2000-2023_2023-09_p867849.csv.gz',
        'output_file': 'output/complaints-accused_2000-2023_2023-09_p867849.csv.gz',
        }
     
    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_csv(cons.input_file)

    df = clean_data(df, log)

    # add back in "O" middle initial2 to last name
    middle_initial2_mask = df.middle_initial2 == 'O'
    df.loc[middle_initial2_mask, 'last_name_NS'] = 'O' + df.loc[middle_initial2_mask, 'last_name_NS'] 

    log.info("Forward filling cr ids")
    df.cr_id = df.cr_id.ffill()

    log.info("Forward filling role, grouped by cr_id")
    df['role'] = df.groupby('cr_id')['role'].ffill()

    log.info("Removing non-accused rows")
    df = df.query("role == 'Accused'")
    df.reset_index(drop=True, inplace=True)

    log.info("Forward filling officer info for multiple allegations")
    # use a pure forward fill, then mask that based on allegation numbers 
    # so that we don't forward fill for single allegations
    multiple_allegation_mask = df['allegation_number'] > 1 

    officer_fill_cols = ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name', 
                        'appointed_date', 'birth_year', 
                        'race', 'gender', 'star', 
                        'unit_description', 'current_unit', 'rank', 'current_rank']

    # only forward fill for multiple allegations
    for col in officer_fill_cols:
        temp_fill = df[col].ffill()
        df.loc[multiple_allegation_mask, col] = temp_fill

    # some manual fixes from name parsing
    # jrs that didn't get split from last name
    jr_mask = df.last_name_NS.isin(['BYKJR', 'JONESJR', 'PAZJR', 'LAUJR'])
    df.loc[jr_mask, 'suffix_name'] = 'JR'
    df.loc[jr_mask, 'last_name_NS'] = df.loc[jr_mask, 'last_name_NS'].str.split("JR").str[0]

    # missing O' from last name
    o_mask = df.last_name_NS.isin(['CARROLL', ])

    # cr_id now has two formats, old integer, and then new format of year-xxxxx
    # change happened sometime in 2019, but here they changed everything to the new format
    df[['year', 'cr_id_str']] = df.cr_id.str.split('-', expand=True)

    df['year'] = pd.to_numeric(df['year'])
    df['cr_id_int'] = pd.to_numeric(df['cr_id_str'])

    # everything pre 2018 is definitely old format, copy int
    df.loc[df.year <= 2018, 'cr_id'] = df.loc[df.year <= 2018, 'cr_id_int'].astype(str)

    # 2019 is a mix: starts as ints continuining from 2018, then resets to new format
    # look for instances where int is greater than max from 2018, convert to old format
    max_cr_id = df.groupby('year')['cr_id_int'].agg([min, max]).loc[2018]['max']

    old_format_mask = (df.year == 2019) & (df.cr_id_int > max_cr_id)
    df.loc[old_format_mask, 'cr_id'] = df.loc[old_format_mask, 'cr_id_int'].astype(str)

    # everything else is in the new format by default, drop extra columns
    df = df.drop(columns=['year', 'cr_id_str', 'cr_id_int'])

    df.to_csv(cons.output_file, **cons.csv_opts)     