#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''assign-unique-ids script for complaints-accused-cms_2019-2022_2022-09_p031093
in_file_changes are rows that are manually being corrected to being the same id
'''

import pandas as pd
import numpy as np
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
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
        'input_file': 'input/complaints-accused-cms_2019-2022_2022-09_p031093.csv.gz',
        'output_file': 'output/complaints-accused-cms_2019-2022_2022-09_p031093.csv.gz',
        'output_profiles_file': 'output/complaints-accused-cms_2019-2022_2022-09_p031093_profiles.csv.gz',
        'id_cols': ['first_name', 'last_name', 'middle_initial', 'birth_year', 'race', 'gender', 'first_name_NS', 'last_name_NS', 'middle_initial2'],
        'incident_cols' : ['rank', 'current_unit', 'star'],
        'list_cols': ['cr_id'],
        'id': 'complaints-accused-cms_2019-2022_2022-09_p031093_ID',
        'in_file_changes': [{'group_id': 1,
                            'first_name': 'EDLIN',
                            'last_name': 'GARCIA',
                            'middle_initial': np.nan,
                            'birth_year': 1985.0,
                            'race': np.nan,
                            'gender': 'FEMALE',
                            'first_name_NS': 'EDLIN',
                            'last_name_NS': 'GARCIA'},
                            {'group_id': 1,
                            'first_name': 'EDLIN',
                            'last_name': 'RENDON',
                            'middle_initial': np.nan,
                            'birth_year': 1985.0,
                            'race': np.nan,
                            'gender': 'FEMALE',
                            'first_name_NS': 'EDLIN',
                            'last_name_NS': 'RENDON'},
                            {'group_id': 2,
                            'first_name': 'JENNIFER',
                            'last_name': 'FINNEGAN',
                            'middle_initial': 'M',
                            'birth_year': 1969.0,
                            'race': 'WHITE',
                            'gender': 'FEMALE',
                            'first_name_NS': 'JENNIFER',
                            'last_name_NS': 'FINNEGAN'},
                            {'group_id': 2,
                            'first_name': 'JENNIFER',
                            'last_name': 'UZUBELL',
                            'middle_initial': 'M',
                            'birth_year': 1969.0,
                            'race': 'WHITE',
                            'gender': 'FEMALE',
                            'first_name_NS': 'JENNIFER',
                            'last_name_NS': 'UZUBELL'},
                            {'group_id': 3,
                            'first_name': 'KENDALL',
                            'last_name': 'BROWN',
                            'middle_initial': 'A',
                            'birth_year': 1991.0,
                            'race': 'WHITE',
                            'gender': 'MALE',
                            'first_name_NS': 'KENDALL',
                            'last_name_NS': 'BROWN'},
                            {'group_id': 3,
                            'first_name': 'KENDALL',
                            'last_name': 'BROWN',
                            'middle_initial': 'A',
                            'birth_year': 1991.0,
                            'race': 'BLACK',
                            'gender': 'MALE',
                            'first_name_NS': 'KENDALL',
                            'last_name_NS': 'BROWN'},
                            {'group_id': 4,
                            'first_name': 'LUIS',
                            'last_name': 'LAURENZANA',
                            'middle_initial': 'A',
                            'birth_year': 1981.0,
                            'race': 'HISPANIC',
                            'gender': 'MALE',
                            'first_name_NS': 'LUIS',
                            'last_name_NS': 'LAURENZANA'},
                            {'group_id': 4,
                            'first_name': 'LUIS',
                            'last_name': 'LAVRENZANA',
                            'middle_initial': 'A',
                            'birth_year': 1981.0,
                            'race': 'HISPANIC',
                            'gender': 'MALE',
                            'first_name_NS': 'LUIS',
                            'last_name_NS': 'LAVRENZANA'}]
    }
     
    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_csv(cons.input_file)
        
    df = assign_unique_ids(df, cons.id, cons.id_cols,
                        log=log)
    
    in_file_changes = pd.DataFrame(cons.in_file_changes)

    # get a dictionary mapping old id to new id to apply change
    change_dict = in_file_changes.merge(df) \
        .groupby('group_id', as_index=False) \
        .agg(old_id=(cons.id, min), new_id=(cons.id, max)) \
        .set_index("old_id")['new_id'] \
        .to_dict()
    
    # apply manual in file changes
    df.loc[df[cons.id].isin(change_dict), cons.id] = df.loc[df[cons.id].isin(change_dict), cons.id].map(change_dict)

    
    profiles_df = aggregate_data(df, cons.id, 
                                cons.id_cols, 
                                max_cols=cons.incident_cols)
         
    df.to_csv(cons.output_file, **cons.csv_opts)
    profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)     