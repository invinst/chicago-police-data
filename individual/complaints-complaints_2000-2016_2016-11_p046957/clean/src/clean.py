#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''clean script for complaints-complaints_2000-2016_2016-11_p046957'''

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
        'input_file': 'input/complaints-complaints_2000-2016_2016-11.csv.gz',
        'output_file': 'output/complaints-complaints_2000-2016_2016-11.csv.gz',
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

df.beat = df.beat.astype(str).str.replace(" ", "").str.replace(".0", "").replace("nan", np.nan).astype('Int64')
df = df.replace("----", "").replace("-----", "")

log.info('Assembling 2000 - 2016 complaints')
cr_ids = set(df.cr_id)

df[cons.date_cols] = df[cons.date_cols].apply(pd.to_datetime)

# some duplicates have different beat, keep the first instance
df = df.drop_duplicates(subset=[col for col in df.columns if col not in ('row_id', 'beat')], 
                         keep='first') \
    .drop('row_id', axis=1) \
    .sort_values('cr_id') \
    .reset_index(drop=True)

assert df[df.cr_id.isnull()].empty

# split city state into it's component city state zip parts. 
# assume zip is always numeric, and state is always two alpha characters
# and that the order is always city -> state -> zip
# use regex to extract state and zip from the end, then use the same regex to extract city from the beginning
df = df.rename(columns={"address_number": "street_number"})

# one off fixes before split
df['city_state'] =df['city_state'] \
    .str.replace("` ", " CHICAGO") \
    .str.replace("`", "") \
    .str.replace(" -", "") \
    .str.replace(" .", "") \
    .str.replace('"', "") \
    .str.replace("CHGO IL", "CHICAGO IL") \
    .replace("CHICAGO IL CHGO", "CHICAGO IL") \
    .str.replace("60623A", "60623") \
    .str.replace("IL ACT", "IL") \
    .str.replace("IL AC", "IL") \
    .str.replace("IL A", "IL") \
    .str.replace("IL  N", "IL") \
    .str.replace("IL IL", "IL") \
    .str.strip()

df['state'] = df['city_state'].str.extract(r' ([A-Z]{2})(?: \d+)?$')

df['zip'] = df['city_state'].str.extract(r' (\d+)$')

df['city'] = df['city_state'].str.replace(r' [A-Z]{2}( \d+)?$', '', regex=True)

# split street to street_direction and street_name
df['street_direction'] = df['street'].str.split(" ").str[0]
df['street_name'] = df['street'].str.split(" ").str[1:].str.join(" ")

df['address'] = combine_address_cols(df, cons.address_cols)

df.cr_id = df.cr_id.astype(str)

df.to_csv(cons.output_file, **cons.csv_opts)
