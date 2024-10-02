#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''import script for complaints-accused_2000-2023_2023-08_p053540'''

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
        'input_file_non_concur': 'input/AllAccusedtoDatePostNonConcur.xlsx',
        'input_file': 'input/AllAccusedtoDate.xlsx',
        'output_file': 'output/complaints-accused_2000-2023_2023-08_p053540.csv.gz',
        'metadata_file': 'output/metadata_complaints-accused_2000-2023_2023-08_p053540.csv.gz',
        'column_names_key': 'complaints-accused_2000-2023_2023-08_p053540',
        }
     
    assert args['input_file'].startswith('input/'),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df_concur = pd.read_excel(cons.input_file).drop_duplicates()
    df_non_concur = pd.read_excel(cons.input_file_non_concur).drop_duplicates()

    combined_df = pd.concat([df_concur, df_non_concur], ignore_index=True)
    all_cr_ids = set(combined_df.LOG_NO.unique())

    df = df_non_concur.merge(df_concur, how='outer', indicator=True)

    assert all_cr_ids.issubset(set(df.LOG_NO.unique())), "CR IDs are missing from the merged dataframe."

    df = df.drop(columns=["_merge"])

    df.columns = standardize_columns(df.columns, cons.column_names_key)
    df.insert(0, 'row_id', df.index + 1)
         
    df.to_csv(cons.output_file, **cons.csv_opts)

    meta_df = collect_metadata(df, cons.input_file, cons.output_file)
    meta_df.to_csv(cons.metadata_file, **cons.csv_opts)     