#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''import script for complaints-victims_2000-2016_2016-11_p046957'''

import pandas as pd
import __main__

from import_functions import read_p046957_file, collect_metadata
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
        'input_file': 'input/p046957_-_report_4_-_victim_data.xls',
        'output_file': 'output/complaints-victims_2000-2016_2016-11.csv.gz',
        'metadata_file': 'output/metadata_complaints-victims_2000-2016_2016-11.csv.gz',
        'column_names': ['cr_id', 'gender', 'age', 'race']
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()


data_df, report_produced_date, FOIA_request = \
                        read_p046957_file(cons.input_file,
                                          original_crid_col='Number',
                                          drop_col_val=('Race Desc',
                                                        'end of record'))
log.info(('Processing {0} file, of FOIA number {1}, produced on {2}'
          '').format(cons.input_file, FOIA_request, report_produced_date))
cons.write_yamlvar("Report_Produced_Date", report_produced_date)
cons.write_yamlvar("FOIA_Request", FOIA_request)

data_df.columns = cons.column_names

data_df.reset_index(drop=True, inplace=True)
data_df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = (collect_metadata(data_df, cons.input_file, cons.output_file)
           .reset_index(drop=True))
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
