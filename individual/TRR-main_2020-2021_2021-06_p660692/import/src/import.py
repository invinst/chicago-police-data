#!usr/bin/env python3
# Author(s):    Ashwin Sharma (Invisible Institute)
#

'''import script for TRR-main_2020-2021_2021-06_p660692'''

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
        'input_file': 'input/17300-P660692-Trr-Jun2020-May2021_R (1).xlsx',
        'output_file': 'output/TRR-main_2020-2021_2021-06_p660692.csv.gz',
        'metadata_file': 'output/metadata_TRR-main_2020-2021_2021-06_p660692.csv.gz',
        'sheet': 'June 2020 - May 20, 2021',
        'keep_columns': [
            'UFE REPORT NO', 'RD', 'EVENT NO', 'OCCURANCE BEAT', 'OCCURANCE LOCATION CODE',
            'STREET NO', 'OCCURANCE STREET NAME', 'OCCURANCE STREET DIRECTION',
            'INCIDENTDATETIME', 'INDOOR OR OUTDOOR', 'LIGHTING CONDITION',
            'WEATHER CONDITION', 'NOTIFICATION OEMC I',
            'NOTIFICATION DISTOFOCCURR I', 'NOTIFICATION IMMEDSUPER I'
            ],
        'column_names_key': 'TRR-main_2020-2021_2021-06_p660692',
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_excel(cons.input_file, sheet_name=cons.sheet)
df = df[cons.keep_columns]
log.info('{} columns selected.'.format(cons.keep_columns))
df.columns = standardize_columns(df.columns, cons.column_names_key)
df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
