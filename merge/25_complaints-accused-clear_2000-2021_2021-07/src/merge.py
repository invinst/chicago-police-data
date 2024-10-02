#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 25_complaints-accused-clear_2000-2021_2021-07'''

import pandas as pd
import numpy as np
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge, 
    null_appointed_date_supplemental_merge, 
    null_appointed_date_and_birth_supplemental_merge, 
    multirow_merge, 
    married_merge,
    whitelist_merge,
    remove_duplicate_merges_filter,
    remove_sup_duplicate_merges_filter
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
        'data_id': 'complaints-accused-clear_2000-2021_2021-07_ID',
        'input_profiles_file' : 'input/complaints-accused-clear_2000-2021_2021-07_profiles.csv.gz',
        'merge_report': "output/merge_report.xlsx",
        'add_cols' : ["BY_to_CA", "current_age", "F4FN", "F4LN"],
        'reshape_cols': {'index': 'cr_id'},
        'date_cols' : ['incident_date', 'complaint_date', 'closed_date'],
        'outcome_lists' : [
            ['000', 'Violation Noted'],
            ['001', '1 Day Suspension'],
            ['002', '2 Day Suspension'],
            ['003', '3 Day Suspension'],
            ['004', '4 Day Suspension'],
            ['005', '5 Day Suspension'],
            ['006', '6 Day Suspension'],
            ['007', '7 Day Suspension'],
            ['008', '8 Day Suspension'],
            ['009', '9 Day Suspension'],
            ['010', '10 Day Suspension'],
            ['011', '11 Day Suspension'],
            ['012', '12 Day Suspension'],
            ['013', '13 Day Suspension'],
            ['014', '14 Day Suspension'],
            ['015', '15 Day Suspension'],
            ['016', '16 Day Suspension'],
            ['017', '17 Day Suspension'],
            ['018', '18 Day Suspension'],
            ['019', '19 Day Suspension'],
            ['020', '20 Day Suspension'],
            ['021', '21 Day Suspension'],
            ['022', '22 Day Suspension'],
            ['023', '23 Day Suspension'],
            ['024', '24 Day Suspension'],
            ['025', '25 Day Suspension'],
            ['026', '26 Day Suspension'],
            ['027', '27 Day Suspension'],
            ['028', '28 Day Suspension'],
            ['029', '29 Day Suspension'],
            ['030', '30 Day Suspension'],
            ['045', '45 Day Suspension'],
            ['060', '60 Day Suspension'],
            ['090', '90 Day Suspension'],
            ['100', 'Reprimand'],
            ['120', 'Suspended for 120 Days'],
            ['180', 'Suspended for 180 Days'],
            ['200', 'Suspended over 30 Days'],
            ['300', 'Administrative Termination'],
            ['400', 'Separation'],
            ['500', 'Reinstated by Police Board'],
            ['600', 'No Action Taken'],
            ['700', 'Reinstated by Court Action'],
            ['800', 'Resigned'],
            ['900', 'Penalty Not Served'],
            [-1, 'Unknown']],
        'input_remerge_file' : 'input/complaints-accused-clear_2000-2021_2021-07.csv.gz',
        'output_remerge_file' : 'output/complaints-accused-clear_2000-2021_2021-07.csv.gz',
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    # cast cr_id to string in ref here
    ref_df = pd.read_csv(cons.input_reference_file)
    ref_df.cr_id = ref_df.cr_id.fillna('').astype(str).str.replace(".0", "", regex=False).replace("", np.nan)

    sup_df = pd.read_csv(cons.input_profiles_file)

    # recast cr ids to string, csv will not preserve datatype on read and will default to float
    cr_cols = sup_df.columns.str.contains("cr_id")
    sup_df.loc[:, cr_cols] = sup_df.loc[:, cr_cols] \
        .apply(lambda x: x.fillna("").astype(str).str.replace(".0", "", regex=False).replace("", np.nan))

    sup_df.star = sup_df.star.astype(float)

    base_merge.merge_dict['cr_id'] = ['cr_id', '']
    base_merge.post_process = remove_sup_duplicate_merges_filter
    base_merge.check_duplicates = False

    null_appointed_date_and_birth_supplemental_merge.merge_dict['cr_id'] = ['cr_id', '']
    # some values in supplemental match to multiple in ref, treat these separately/drop them here
    null_appointed_date_and_birth_supplemental_merge.post_process = remove_sup_duplicate_merges_filter
    null_appointed_date_and_birth_supplemental_merge.check_duplicates = False

    custom_merge = Merge(name='custom merges', 
                         custom_merges=[['star', 'cr_id', 'first_name_NS', 'last_name_NS']],
                         merge_postprocess=remove_sup_duplicate_merges_filter,
                         check_duplicates=False)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, one_to_one=False)\
        .loop_merge([base_merge, null_appointed_date_and_birth_supplemental_merge, custom_merge])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
    
    log.info('Assembling 2000 - 2021 clear accused')
    df = pd.read_csv(cons.output_remerge_file)
    cr_ids = set(df.cr_id)

    df = df[['cr_id', 'UID', 'complaint_code', 'allegation_category_desc', 'final_finding', 'final_penalty', 
             'final_penalty_desc', 'days']]\
        .rename(columns={'allegation_category_desc' : 'complaint_category'})

    df['final_finding'] = df['final_finding'].replace({'NOT SUSTAINED' : 'NS', 
        'SUSTAINED' : 'SU',
        'UNFOUNDED': 'UN', 
        'NO AFFIDAVIT' : 'NAF',
        'EXONERATED' : 'EX', 
        'ADDITIONAL INVESTIGATION REQUESTED' : np.nan,
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