#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''clean script for complaints-complaints_1967-1999_2016-12_'''

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
        'input_file': 'input/complaints-complaints_1967-1999_2016-12.csv.gz',
        'output_file': 'output/complaints-complaints_1967-1999_2016-12.csv.gz',
        'date_cols' : ['incident_date', 'complaint_date', 'closed_date']
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_csv(cons.input_file).reset_index(drop=True)
df = clean_data(df, log)

# for some reason this pull has a prefixed c
df.cr_id = df.cr_id.str.replace("C", "").astype(int)

df["incident_location"] = df["incident_location"].fillna("")

log.info("Removed C character at start of cr_ids.")

log.info('Assembling 1967 - 1999 complaints')
cr_ids = set(df.cr_id)

df = df[['cr_id', 'incident_date', 'complaint_date', 'closed_date', 'incident_location']] \
    .rename(columns={'incident_location': 'address'}) \
    .drop_duplicates()

df[cons.date_cols] = df[cons.date_cols].apply(pd.to_datetime)
df['address'] = df['address'].fillna('').astype(str) \
    .str.replace("**", "00")

df = df.groupby('cr_id', as_index=False) \
    .agg(
        incident_date = ('incident_date', min), 
        complaint_date = ('complaint_date', min), 
        closed_date = ('closed_date', min),
        address = ('address', lambda x: sorted(x, key=len)[-1])
    )

assert df.shape[0] == df['cr_id'].nunique(), "Duplicate cr id found after complaints processing"
assert set(df.cr_id) == cr_ids, "Missing a cr id after complaints file processing"
assert df[df.cr_id.isnull()].empty, "All complaints must have a cr id"

df.cr_id = df.cr_id.astype(str)


df.to_csv(cons.output_file, **cons.csv_opts)
