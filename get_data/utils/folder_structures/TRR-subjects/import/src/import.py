#!usr/bin/env python3
#


'''import script for TRR-subjects_2004-2016_2016-09_p046360'''

import pandas as pd
import __main__
import sys


from import_functions import standardize_columns, collect_metadata
import setup


def create_metadata_filename(filename):
    file_split = filename.split('/')
    return file_split[0] + '/metadata_' + file_split[1]


def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'input_file': sys.argv[1],
        'output_file': sys.argv[2],
        'metadata_file': create_metadata_filename(sys.argv[2]),
        'sheet': 'Sheet1',
        'keep_columns': [
            'TRR_REPORT_ID', 'SUBGNDR',
            'SUBRACE', 'SUBAGE', 'SUBYEARDOB',
            'SUBJECT_ARMED', 'SUBJECT_INJURED',
            'SUBJECT_ALLEGED_INJURY', 'SUBJECT_NO',
            'SR_NO', 'SE_NO', 'DTE'
            ],
         'column_names_key': 'TRR-subjects_2004-2016_2016-09_p046360'
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_excel(cons.input_file, sheet_name=cons.sheet)
missing_columns = set(cons.keep_columns) - set(df.columns)
if missing_columns:
    log.warning('Warning: {} columns missing.'.format(missing_columns))
    # set values in missing columns to be empty, for tracking in the metadata
    for col in missing_columns:
        df[col] = ''
df = df[cons.keep_columns]
log.info('{} columns selected.'.format(cons.keep_columns))
df.columns = standardize_columns(df.columns, cons.column_names_key)
df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
