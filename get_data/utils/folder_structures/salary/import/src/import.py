#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''import script for salary_2002-2017_2017-09_'''

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
        'input_files': [
            'input/9-13-17_FOIA_Updated_CPD_Data-2002-2007.xls',
            'input/9-13-17_FOIA_Updated_CPD_Data-2008-2012.xls',
            'input/9-13-17_FOIA_Updated_CPD_Data_-2013-2017.xls'
            ],
        'output_file': 'output/salary_2002-2017_2017-09.csv.gz',
        'metadata_file': 'output/metadata_salary_2002-2017_2017-09.csv.gz',
        'column_names_key': 'salary_2002-2017_2017-09_'
        }

    assert all(input_file.startswith('input/')
               for input_file in args['input_files']),\
        "An input_file is malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

data_df = pd.DataFrame()
meta_df = pd.DataFrame()

for input_file in cons.input_files:
    xls_file = pd.ExcelFile(input_file)
    for sheet in xls_file.sheet_names:
        df = xls_file.parse(sheet)
        df.columns = standardize_columns(df.columns, cons.column_names_key)
        df.insert(0, 'year', int(sheet))

        data_df = (data_df
                   .append(df)
                   .reset_index(drop=True))

        meta_df = (meta_df
                   .append(collect_metadata(df,
                                            '{0}-{1}'.format(input_file,
                                                             sheet),
                                            cons.output_file))
                   .reset_index(drop=True))
data_df.insert(0, 'row_id', data_df.index+1)
data_df.to_csv(cons.output_file, **cons.csv_opts)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
