#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''import script for TRR-officers_2004-2018_2018-08_p456008'''


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
        'output_file': 'output/TRR-officers_2004-2018_2018-08_p456008.csv.gz',
        'metadata_file': 'output/metadata_TRR-officers_2004-2018_2018-08_p456008.csv.gz',
        'main_column_names_key': 'TRR-officers_2004-2018_2018-08_p456008',
        'star_column_names_key': 'TRR-stars_2004-2018_2018-08_p456008',
        'main_sheet': 'Sheet1',
        'star_sheet': "Star #",
        'notes_sheet': 'Source Info and Notes',
        'main_keep_columns': [
            'TRR_REPORT_ID', 'POLAST', 'POFIRST', 'POGNDR',
            'PORACE', 'POAGE', 'APPOINTED_DATE', 'UNITASSG',
            'UNITDETAIL', 'ASSGNBEAT', 'RANK', 'DUTYSTATUS',
            'POINJURED', 'MEMBER_IN_UNIFORM', 'DTE'
            ],
        }
    

    assert args['input_file'].startswith('input/'),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    notes_df = pd.read_excel(cons.input_file, sheet_name=cons.notes_sheet,
                            header=None)
    notes = '\n'.join(notes_df.loc[notes_df[0].isin([cons.main_sheet,
                                                    cons.star_sheet]),
                                1].dropna())
    cons.write_yamlvar('Notes', notes)
    log.info('Notes written to cons: {}'.format(notes))

    main_df = pd.read_excel(cons.input_file, sheet_name=cons.main_sheet)
    main_df = main_df[cons.main_keep_columns]
    log.info('{} columns selected from main sheet.'.format(cons.main_keep_columns))
    main_df.columns = standardize_columns(main_df.columns, cons.main_column_names_key)

    star_df = pd.read_excel(cons.input_file, sheet_name=cons.star_sheet)
    star_df.columns = standardize_columns(star_df.columns,  cons.star_column_names_key)

    # there's one instance of a name change so merging just on trr_id
    df = main_df.merge(star_df, on='trr_id', how='outer', indicator=True)

    log.info(f"{df[df['last_name_x'] != df['last_name_y']]} name changes")

    assert (df._merge == 'both').all(), "Row mismatch"

    # drop hyphenated name 
    df = df.drop(['first_name_y', 'last_name_y', '_merge'], axis=1) \
        .rename(columns={"first_name_x": "first_name", "last_name_x": "last_name"})

    df.insert(0, 'row_id', df.index+1)
    df.to_csv(cons.output_file, **cons.csv_opts)

    meta_df = collect_metadata(df, cons.input_file, cons.output_file)
    meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
