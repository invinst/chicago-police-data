#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 29_complaints-accused_2000-2023_2023-09_p867849'''

import pandas as pd
import numpy as np
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge, 
    base_merge_dict,
    null_appointed_date_supplemental_merge,
    null_appointed_date_and_birth_supplemental_merge,
    multirow_merge,
    married_merge,
    appointed_date_day_offset
)
from merge_data import Merge
from general_utils import keep_duplicates, remove_duplicates
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
        'input_reference_file': 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID',
        'data_id': 'complaints-accused_2000-2023_2023-09_p867849_ID',
        'input_profiles_file' : 'input/complaints-accused_2000-2023_2023-09_p867849_profiles.csv.gz',
        'add_cols' : ['BY_to_CA', 'current_age', 'F4FN', 'F4LN', 'L4LN'],
        'input_remerge_file' : 'input/complaints-accused_2000-2023_2023-09_p867849.csv.gz',
        'output_remerge_file' : 'output/complaints-accused_2000-2023_2023-09_p867849.csv.gz',
        }
     
    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file)

    ref_df.appointed_date = pd.to_datetime(ref_df.appointed_date)
    sup_df.appointed_date = pd.to_datetime(sup_df.appointed_date)
    
    base_merge.check_duplicates = False
    base_merge.filter_reference_merges_flag = False
    base_merge.merge_dict['cr_id'] = ['cr_id', '']
    base_merge.merge_dict['last_name'] = ['last_name_NS', 'F4LN', 'L4LN']

    appointed_date_update_star = Merge(name='appointed date update star', 
                                  merge_dict={**base_merge.merge_dict, 
                                              'appointed_date': ['appointed_date', ''],
                                              'star': ['star'],
                                              'birth_year': ['birth_year']},
                                  check_duplicates=False,
                                  filter_reference_merges_flag=False)
    
    appointed_date_update_offset_merge = Merge(name='appointed date update, offset 1 day',
                                         merge_dict={**base_merge.merge_dict,
                                                     'appointed_date': ['appointed_date', ''],
                                                     'birth_year': ['birth_year']},
                                        check_duplicates=False,
                                        filter_reference_merges_flag=False,
                                        merge_postprocess=appointed_date_day_offset)
    
    null_appointed_date_and_birth_supplemental_merge.check_duplicates = False
    null_appointed_date_and_birth_supplemental_merge.filter_reference_merges_flag = False
    
    merges = [base_merge, appointed_date_update_offset_merge,  
              null_appointed_date_and_birth_supplemental_merge, 
              appointed_date_update_star, married_merge]
    
    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, one_to_one=False, from_year=2023)\
        .loop_merge(merges=merges)\
        .append_to_reference(keep_sup_um=False)\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
        

    log.info('Assembling 2000 - 2023 clear accused')
    df = pd.read_csv(cons.output_remerge_file)

    cr_ids = set(df.cr_id)

    df = df[['cr_id', 'UID', 'category_code', 'category', 'finding', 'penalty', 
             'days']]\
            .rename(columns={
                'category_code': 'complaint_code',
                'category' : 'complaint_category',
                'finding': 'final_finding',
                'penalty': 'final_penalty'})

    df['final_finding'] = df['final_finding'].str.upper() \
        .replace({'NOT SUSTAINED' : 'NS', 
            'SUSTAINED' : 'SU',
            'UNFOUNDED': 'UN', 
            'NO AFFIDAVIT' : 'NAF',
            'EXONERATED' : 'EX', 
            'ADMINISTRATIVELY CLOSED': 'AC',
            'ADMINISTRATIVELY TERMINATED': 'EX',
            'ADDITIONAL INVESTIGATION REQUESTED' : np.nan,
            None: np.nan})

    df['ff_val'] = df['final_finding'].replace({'SU' : 1, 'EX' : 2, 'UN' : 3, 'NS' : 4, 'NAF' : 5, "AC": 6})

    df = pd.concat([
            df[df['UID'].notnull()]\
            .sort_values(['cr_id', 'UID', 'ff_val'], na_position='last')\
            .groupby(['cr_id', 'UID'], as_index=False)\
            .first()\
            , df[df['UID'].isnull()]])\
        .drop('ff_val', axis=1) \
        .replace({None: np.nan})

    assert keep_duplicates(df, ['cr_id', 'UID'])["UID"].notnull().sum() == 0, "Should not have duplicates on cr_id and UID"
    assert set(df.cr_id) == cr_ids, "Missing cr_ids"
    assert df[df.cr_id.isnull()].empty, "Row without cr_id" 

    df.to_csv(cons.output_remerge_file, **cons.csv_opts)