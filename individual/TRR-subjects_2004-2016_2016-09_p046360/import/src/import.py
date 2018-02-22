#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''import script for TRR-subjects_2004-2016_2016-09_p046360'''

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
        'input_file': 'input/10655-FOIA-P046360-TRRdata_sterilized.xlsx',
        'output_file': 'output/TRR-subjects_2004-2016_2016-09.csv.gz',
        'metadata_file': 'output/metadata_TRR-subjects_2004-2016_2016-09.csv.gz',
        'sheet': 'Sheet1',
        'keep_columns': [
            'TRR_REPORT_ID', 'SUBGNDR',
            'SUBRACE', 'SUBAGE', 'SUBYEARDOB',
            'SUBJECT_ARMED', 'SUBJECT_INJURED',
            'SUBJECT_ALLEGED_INJURY', 'SUBJECT_NO',
            'SR_NO', 'SE_NO'
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

df = pd.read_excel(cons.input_file, sheetname=cons.sheet)
df = df[cons.keep_columns]
log.info('{} columns selected.'.format(cons.keep_columns))
df.columns = standardize_columns(df.columns, cons.column_names_key)
df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
