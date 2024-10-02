#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''assign-unique-ids script for complaints-accused_2000-2023_2023-08_p053540'''

import pandas as pd
import numpy as np
import __main__

import yaml
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
        'input_file': 'input/complaints-accused_2000-2023_2023-08_p053540.csv.gz',
        'output_file': 'output/complaints-accused_2000-2023_2023-08_p053540.csv.gz',
        'output_profiles_file': 'output/complaints-accused_2000-2023_2023-08_p053540_profiles.csv.gz',
        'id_cols': ['first_name', 'last_name', 'middle_initial', 'birth_year', 'race', 'gender', 'appointed_date', 'first_name_NS', 'last_name_NS', 'middle_initial2'],
        'incident_cols' : ['rank', 'current_unit', 'star'],
        'list_cols': ['cr_id'],
        'in_file_changes': [
            {'group_id': 1, 'first_name_NS': 'EDLIN', 'last_name_NS': 'RENDON', 'middle_initial': np.nan, 'appointed_date': '2017-01-17', 'birth_year': 1985.0, 'race': np.nan, 'star': 3250.0}, 
            {'group_id': 1, 'first_name_NS': 'EDLIN', 'last_name_NS': 'GARCIA', 'middle_initial': np.nan, 'appointed_date': '2017-01-17', 'birth_year': 1985.0, 'race': np.nan, 'star': 3250.0}, 
            {'group_id': 2, 'first_name_NS': 'KENDALL', 'last_name_NS': 'BROWN', 'middle_initial': 'A', 'appointed_date': '2017-03-16', 'birth_year': 1991.0, 'race': 'WHITE', 'star': 11966.0}, 
            {'group_id': 2, 'first_name_NS': 'KENDALL', 'last_name_NS': 'BROWN', 'middle_initial': 'A', 'appointed_date': '2017-03-16', 'birth_year': 1991.0, 'race': 'BLACK', 'star': 11966.0}],
        'id': 'complaints-accused_2000-2023_2023-08_p053540_ID',
        }
     
    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_csv(cons.input_file)

    df.star = df.star.astype(float)
        
    df = assign_unique_ids(df, cons.id, cons.id_cols,
                        log=log)
    
    # handle non merge separately so that there's not 1000 extra cr_id columns for UNKNOWN profiles
    profiles_df = aggregate_data(df.query("merge == 1"), cons.id, 
                                cons.id_cols, 
                                max_cols=cons.incident_cols + ['merge'],
                                list_cols=cons.list_cols)
    
    non_merge_profiles_df = aggregate_data(df.query("merge != 1"), cons.id, 
                                cons.id_cols, 
                                max_cols=cons.incident_cols + ['merge'])
         
    profiles_df = pd.concat([profiles_df, non_merge_profiles_df], ignore_index=True)

    with open('hand/nc_nonpo_ranks.yaml', 'r') as f:
        nonpo_ranks = yaml.safe_load(f)

    profiles_df.loc[profiles_df['rank'].isin(nonpo_ranks), 'merge'] = 0

    df.to_csv(cons.output_file, **cons.csv_opts)
    profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)     