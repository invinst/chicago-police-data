#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''import script for complaints-accused_1967-1999_2016-12_'''

import pandas as pd
import __main__

from import_functions import standardize_columns, collect_metadata
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


        'input_file': 'input/MAINFRAME_COMPLAINTS_REVISED_30NOV2016.CSV',
        'output_file': 'output/complaints-accused_1967-1999_2016-12.csv.gz',
        'metadata_file': 'output/metadata_complaints-accused_1967-1999_2016-12.csv.gz',
        'column_names_key': 'complaints-accused_1967-1999_2016-12_',
        'keep_columns': ['CASE_NUMBER', 'ACCUSED OFFICERS NAME',
                         'STAR_AT_INCIDENT', 'SEX', 'DOB', 'APPOINTED_DATE',
                         'ASSGND_UNIT_AT_INCIDENT', 'FINAL_COMPLAINT_CAT',
                         'RANK_AT_INCIDENT', 'COMPLAINT DESC', 'FINDING',
                         'FINDING DESC', 'ACTION_TAKEN', 'ACTION DESC']
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()


df = pd.read_csv(cons.input_file)
df = df[cons.keep_columns]
log.info('Columns selected: {}'.format(cons.keep_columns))
df.columns = standardize_columns(df.columns, cons.column_names_key)
df.insert(0, 'row_id', df.index+1)
df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
