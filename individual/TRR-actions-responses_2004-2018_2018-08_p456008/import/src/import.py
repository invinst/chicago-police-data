#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''import script for TRR-actions-responses_2004-2018_2018-08_p456008'''

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
        'input_file': 'input/P456008 13094-FOIA-P456008-TRRdata Responsive Record Produced By R&A.xlsx',
        'output_file': 'output/TRR-actions-responses_2004-2018_2018-08_p456008.csv.gz',
        'metadata_file': 'output/metadata_TRR-actions-responses_2004-2018_2018-08_p456008.csv.gz',
        'column_names_key': 'TRR-actions-responses_2004-2018_2018-08_p456008',
        'sheet_name': 'ActionsResponses'
        }
     
    assert args['input_file'].startswith('input/'),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_excel(cons.input_file, sheet_name='ActionsResponses')
         
    df.columns = standardize_columns(df.columns, cons.column_names_key)
    df.insert(0, 'row_id', df.index + 1)
         
    df.to_csv(cons.output_file, **cons.csv_opts)

    meta_df = collect_metadata(df, cons.input_file, cons.output_file)
    meta_df.to_csv(cons.metadata_file, **cons.csv_opts)     