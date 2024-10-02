#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''import script for TRR-actions-responses_2021-2023_2023-10_p877988'''

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
        'input_file': 'input/P877988_TRR_2021-YTD.xlsx',
        'output_file': 'output/TRR-actions-responses_2021-2023_2023-10_p877988.csv.gz',
        'metadata_file': 'output/metadata_TRR-actions-responses_2021-2023_2023-10_p877988.csv.gz',
        'column_names_key': 'TRR-actions-responses_2021-2023_2023-10_p877988',
        'sheet_name': 'Jan1-2021-Sep15-2023 TRR',
        'keep_cols': ['REPORT NO', 'Subject Actions', 'Subj Act Does Not Apply NewTRR', 'Subj Act Unknown NewTRR', 'Type Activity New TRR',
                          'ForceMitigationEfforts New TRR', 'ControlTactics New TRR', 'ResponseWithoutWeapons New TRR', 'ResponseWithWeapon New TRR',
                          'ForceWhileHandcuffed New TRR']
        }
     
    assert args['input_file'].startswith('input/'),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_excel(cons.input_file, sheet_name=cons.sheet_name, usecols=cons.keep_cols)
         
    df.columns = standardize_columns(df.columns, cons.column_names_key)
    df.insert(0, 'row_id', df.index + 1)
         
    df.to_csv(cons.output_file, **cons.csv_opts)

    meta_df = collect_metadata(df, cons.input_file, cons.output_file)
    meta_df.to_csv(cons.metadata_file, **cons.csv_opts)     