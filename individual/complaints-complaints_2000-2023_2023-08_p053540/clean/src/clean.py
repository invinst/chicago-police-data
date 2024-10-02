#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''clean script for complaints-complaints_2000-2023_2023-08_p053540'''

import pandas as pd
import __main__

from clean_functions import clean_data
from general_utils import combine_address_cols, fewest_nans
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
        'input_file': 'input/complaints-complaints_2000-2023_2023-08_p053540.csv.gz',
        'output_file': 'output/complaints-complaints_2000-2023_2023-08_p053540.csv.gz',
        'address_cols': ['street_number', 'street_direction', "street_name", "city", "state", "zip"]
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

    # single row with no cr_id, drop it
    df = df.dropna(subset=['cr_id'])

    log.info("Fixing two cr_ids formats")
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

    df.cr_id = df.cr_id.astype(str)

    # format addresses
    df = df.rename(columns={"block": "street_number",
                            "street_dir": "street_direction"})

    df['street_number'] = df['street_number'].str.replace("XX", "00")
    df['street_direction'] = df['street_direction'].str.upper().replace("1250", "").str[0]
    df['street_name'] = df['street_name'].replace("Unknown", "").str.upper()
    df['city'] = df['city'].replace("her boss thinks of the princess https://www.linkedin.com/in/", "") \
        .replace("Leave HER ALONE", "").replace('get in line', "").replace('-',"").replace('Know dont', "") \
        .replace("Pathetic", "").replace("700 E. TEMPLE STREET, LOS ANGELES, CALIFORNIA 90012 • 866.", "") \
        .replace("GOD", "").str.upper().replace("NA", "")
    df['state'] = df['state'].str.upper() \
        .replace({'ALABAMA': 'AL',
                'ALASKA': 'AK',
                'ARIZONA': 'AZ',
                'CALIFORNIA': 'CA',
                'COLORADO': 'CO',
                'CONNECTICUT': 'CT',
                'FLORIDA': 'FL',
                'GEORGIA': 'GA',
                'INDIANA': 'IN',
                'IOWA': 'IA',
                'KANSAS': 'KS',
                'LOUISIANA': 'LA',
                'MARYLAND': 'MD',
                'MICHIGAN': 'MI',
                'MINNESOTA': 'MN',
                'MISSOURI': 'MO',
                'NEW JERSEY': 'NJ',
                'NEW YORK': 'NY',
                'NORTH CAROLINA': 'NC',
                'OHIO': 'OH',
                'PENNSYLVANIA': 'PA',
                'SOUTH CAROLINA': 'SC',
                'TENNESSEE': 'TN',
                'TEXAS': 'TX',
                'VIRGINIA': 'VA',
                'WISCONSIN': 'WI',
                'DISTRICT OF COLUMBIA': 'DC'})
    df['zip'] = df['zip'].str.split("-").str[0].str.upper()
    
    df['address'] = combine_address_cols(df, cons.address_cols)
    
    cr_ids = set(df['cr_id'].unique())

    # deduplicate on cr_ids
    df = df.groupby(['cr_id'], as_index=False).apply(fewest_nans)

    assert set(df['cr_id'].unique()) == cr_ids, "Missing cr_ids"
 
    df.to_csv(cons.output_file, **cons.csv_opts)     