#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 28_complaints-accused_2000-2023_2023-08_p053540

Trickier merges
    - these are directly from complainants, often with mispellings, general officers, inaccurate names, etc.
    - these have no appointed date
    - file is not properly deduplicated
'''

import pandas as pd
import numpy as np
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge,
    base_merge_dict,
    remove_duplicate_merges_filter,
    married_merge,
    null_appointed_date_supplemental_merge
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
        'data_id': 'complaints-accused_2000-2023_2023-08_p053540_ID',
        'input_profiles_file' : 'input/complaints-accused_2000-2023_2023-08_p053540_profiles.csv.gz',
        'add_cols' : ['BY_to_CA', 'current_age', 'F4FN', 'F4LN'],
        'input_remerge_file' : 'input/complaints-accused_2000-2023_2023-08_p053540.csv.gz',
        'output_remerge_file' : 'output/complaints-accused_2000-2023_2023-08_p053540.csv.gz',
        }
     
    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file)

    sup_df.loc[sup_df['first_name_NS'].isnull(), 'merge'] = 0

    sup_df.star = sup_df.star.astype(float)

    base_merge.check_duplicates = False
    base_merge.filter_reference_merges_flag = False

    appointed_date_update_merge = Merge(
        name='appointed date updates',
        custom_merges=[['star', 'first_name_NS', 'last_name_NS', 'birth_year']],
        check_duplicates=False,
        filter_reference_merges_flag=False
    )

    null_appointed_date_merge = Merge(
        name='null appointed date: star, first name, last name merge',
        custom_merges=[['star', 'first_name_NS', 'last_name_NS']],
        supplemental_preprocess="appointed_date_null and birth_year_null",
        check_duplicates=False,
        filter_reference_merges_flag=False
    )

    married_merge.check_duplicates = False
    married_merge.filter_reference_merges_flag = False

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, one_to_one=False, from_year=2023)\
        .loop_merge(merges=[base_merge, 
                            married_merge,
                            appointed_date_update_merge, 
                            null_appointed_date_merge])\
        .append_to_reference(keep_sup_um=False)\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
        

    log.info('Assembling 2000 - 2023 clear accused')
    df = pd.read_csv(cons.output_remerge_file)

    # TODO: move to individual, first row has a null cr_id
    df = df[df.cr_id.notnull()]
     
    cr_ids = set(df.cr_id)

    df = df[['cr_id', 'UID', 'complaint_code', 'allegation_category', 'recommended_finding', 'final_finding', 'recommended_penalty',
                'final_penalty']]\
            .rename(columns={'allegation_category' : 'complaint_category'})
    

    df['final_finding'] = df['final_finding'].str.upper() \
        .replace({'NOT SUSTAINED' : 'NS', 
            'SUSTAINED' : 'SU',
            'UNFOUNDED': 'UN', 
            'NO AFFIDAVIT' : 'NAF',
            'EXONERATED' : 'EX', 
            'ADDITIONAL INVESTIGATION REQUESTED' : np.nan,
            'ADMINISTRATIVELY CLOSED': 'AC',
            None: np.nan})

    df['ff_val'] = df['final_finding'].replace({'SU' : 1, 'EX' : 2, 'UN' : 3, 'NS' : 4, 'NAF' : 5})

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