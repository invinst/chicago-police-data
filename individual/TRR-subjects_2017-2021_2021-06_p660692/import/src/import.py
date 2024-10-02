#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''import script for TRR-subjects_2017-2021_2021-06_p660692'''

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
        'input_file': 'input/17300-P660692-Trr-Jun2020-May2021_R_(1).xlsx',
        'output_file': 'output/TRR-subjects_2017-2021_2021-06_p660692.csv.gz',
        'metadata_file': 'output/metadata_TRR-subjects_2017-2021_2021-06_p660692.csv.gz',
        'column_names_key': 'TRR-subjects_2017-2021_2021-06_p660692',
        'sheet_name': 'June 2020 - May 20, 2021',
        'keep_cols': [
            'UFE REPORT NO', 'SUBJECT IR NO', 'SUBJECT CB NO', 'SUBJECT ACTION ASSULTBATPOL I',
            'WEAPCONTRIBUTE SUBINJRY', 'WEAPON DISCHRG SELFINJUR SUB I',
            'SUBJECT FORCEWHILEHANDCUFFED I', 'SUBJSEX', 'SUB RACE',
            'SUB BIRTHYEAR', 'SUB HEIGHT', 'SUB WEIGHT', 'SUB ZIPCODE',
            'SUBJECT CONDITION NEW TRR', 'SUBJECT MED TREATMENT NEW TRR',
            'SUBJECT INJ MEM UOFF OLD TRR', 'SUBJECT INJ MEM UOFF NEW TRR',
            'SUBJECT ACTIONS NEW TRR', 'SUBJECT UNDER INFLUENCE', 'SUBJECT ARMED',
            'SUBJECTARMEDWITH', 'SUBJECT WEAPON USE', 'SUBJECT INJURED',
            'SUBJECT ALLEGED INJURY', 'SUBJECT REFUSE MEDICAL AID',
            'SUBJECT HOSPITALIZED', 'SUBDRUGRELATED', 'SUBGANGRELATED',
            'SUBJECT ACTION ASSULTBATPOL I 1', 'SUBJ VEH USED WEAP']
        }
     
    assert args['input_file'].startswith('input/'),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_excel(cons.input_file, sheet_name='June 2020 - May 20, 2021', usecols=cons.keep_cols)
         
    df.columns = standardize_columns(df.columns, cons.column_names_key)
    df.insert(0, 'row_id', df.index + 1)
         
    df.to_csv(cons.output_file, **cons.csv_opts)