#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''clean script for complaints-complaints_2000-2020_2020-07'''

import pandas as pd
import numpy as np
import __main__

from clean_functions import clean_data
from general_utils import keep_duplicates, remove_duplicates, fewest_nans, combine_address_cols
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
        'input_file': 'input/complaints-complaints-clear_2000-2020_2020-07.csv.gz',
        'output_file': 'output/complaints-complaints-clear_2000-2020_2020-07.csv.gz',
        'date_cols' : ['incident_date', 'complaint_date', 'closed_date'],
        'address_cols': ['street_number', 'street_direction', 'street_name', 'city', 'state', 'zip']
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
df = clean_data(df, log)

log.info('Assembling 2000 - 2020 clear complaints')
cr_ids = set(df.cr_id)

df_core = df\
    .drop(['row_id'] + cons.date_cols, axis=1)\
    .drop_duplicates()\
    .rename(columns={'category' : 'complaint_category',
                     'category_code' : 'complaint_code'})

df_core = pd.concat([keep_duplicates(df_core, 'cr_id')\
    .replace({'?': np.nan, '0' : np.nan, 0.0 : np.nan})\
    .groupby('cr_id', as_index=False)\
    .apply(fewest_nans)\
    , remove_duplicates(df_core, 'cr_id')])

df_dates = df[['cr_id'] + cons.date_cols].drop_duplicates()
df_dates[cons.date_cols] = df_dates[cons.date_cols].apply(pd.to_datetime)
df_dates = df_dates.groupby('cr_id', as_index=False).min()

df = df_core.merge(df_dates, on='cr_id', how='inner')

# address formatting 
df = df.rename(columns={'street_dir': "street_direction"})

df['street_number'] = df['street_number'].astype("Int64").astype(str).replace("<NA>", "")
df['street_direction'] = df['street_direction'].replace("?", "").str[0]
df['street_name'] = df['street_name'].str.upper().replace("UKNOWN", "")
df['city'] = df['city'].str.upper()
df['state'] = df['state'].str.upper().replace({'ALABAMA': 'AL',
        'ALASKA': 'AK',
        'ARIZONA': 'AZ',
        'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA',
        'COLORADO': 'CO',
        'CONNECTICUT': 'CT',
        'FLORIDA': 'FL',
        'GEORGIA': 'GA',
        'HAWAII': 'HI',
        'IDAHO': 'ID',
        'ILLINOIS': 'IL',
        'INDIANA': 'IN',
        'IOWA': 'IA',
        'KENTUCKY': 'KY',
        'LOUISIANA': 'LA',
        'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA',
        'MICHIGAN': 'MI',
        'MINNESOTA': 'MN',
        'MISSOURI': 'MO',
        'NEVADA': 'NV',
        'NEW JERSEY': 'NJ',
        'NEW MEXICO': 'NM',
        'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC',
        'OHIO': 'OH',
        'OKLAHOMA': 'OK',
        'OREGON': 'OR',
        'PENNSYLVANIA': 'PA',
        'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD',
        'TENNESSEE': 'TN',
        'TEXAS': 'TX',
        'UTAH': 'UT',
        'VIRGINIA': 'VA',
        'WASHINGTON': 'WA',
        'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI',
        'DISTRICT OF COLUMBIA': 'DC'})

df['zip'] = df['zip'].str.split("-").str[0]

df['address'] = combine_address_cols(df, cons.address_cols)


assert df.shape[0] == df_core.shape[0] == df_dates.shape[0] == df['cr_id'].nunique(), 'Missing a cr_id'
assert set(df.cr_id) == cr_ids, "Missing a cr_id"
assert df[df.cr_id.isnull()].empty

df.cr_id = df.cr_id.astype(str)

df.to_csv(cons.output_file, **cons.csv_opts)
