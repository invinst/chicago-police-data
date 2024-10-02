#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''import script for TRR-officers_2021-2023_2023-10_p877988'''

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
        'output_file': 'output/TRR-officers_2021-2023_2023-10_p877988.csv.gz',
        'metadata_file': 'output/metadata_TRR-officers_2021-2023_2023-10_p877988.csv.gz',
        'main_column_names_key': 'TRR-officers_2021-2023_2023-10_p877988',
        'star_column_names_key': 'TRR-officers-star_2021-2023_2023-10_p877988',
        'main_sheet': 'Jan1-2021-Sep15-2023 TRR',
        'star_sheet': "Additional Officer Info",
        'keep_columns': [
            'REPORT NO', 'TITLE AT INCIDENT', 'CURRENT TITLE', 'MEMBER FIRST NAME', 'MEMBER LAST NAME', 'MI', 
            'UNIT AT INCIDENT', 'CURRENT UNIT', 'MEMBER_WATCH', 'MEMBERRACE', 'MEMBERSEX', 'MEMBER_BIRTH_YEAR', 'MEMBERHEIGHT',
            'MEMBERWEIGHT', 'APPOINTED_DATE', 'MEMBER_ASSIGNED_BEAT', 'INCIDENTDATETIME', 
            'MEMBER_ON_DUTY', 'MEMBER_IN_UNIFORM','MEMBER_INJURED',
        ]
        }
     
    assert args['input_file'].startswith('input/'),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    main_df = pd.read_excel(cons.input_file, sheet_name=cons.main_sheet)
    main_df = main_df[cons.keep_columns]
    log.info('{} columns selected from main sheet.'.format(cons.keep_columns))
    main_df.columns = standardize_columns(main_df.columns, cons.main_column_names_key)

    star_df = pd.read_excel(cons.input_file, sheet_name=cons.star_sheet)
    star_df.columns = standardize_columns(star_df.columns, cons.star_column_names_key)

    df = main_df.merge(star_df, on=['trr_id', 'last_name', 'first_name'], how='left')

    df.insert(0, 'row_id', df.index+1)
    df.to_csv(cons.output_file, **cons.csv_opts)

    meta_df = collect_metadata(df, cons.input_file, cons.output_file)
    meta_df.to_csv(cons.metadata_file, **cons.csv_opts)

