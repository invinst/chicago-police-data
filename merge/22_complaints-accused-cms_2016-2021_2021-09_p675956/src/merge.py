#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 22_complaints-accused-cms_2016-2021_2021-09_p675956'''

import pandas as pd
import numpy as np
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge,
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
        'data_id': 'complaints-accused-cms_2016-2021_2021-09_p675956_ID',
        'input_profiles_file' : 'input/complaints-accused-cms_2016-2021_2021-09_p675956_profiles.csv.gz',
        'add_cols' : ['BY_to_CA', 'current_age'],
        'reshape_cols': {'index': 'cr_id'},
        'custom_merges': [['star', 'first_name_NS', 'last_name_NS', 'current_age_m1', 'gender', 'race'], # darlene alexander
                          ['star', 'first_name_NS', 'last_name_NS', 'gender', 'race'], # wojciech dratwa, appointed date off by 1
                          ['first_name_NS', 'last_name_NS', 'current_age_m1', 'gender', 'race'], # robert earnshaw
                          ['star', 'appointed_date']], # lots of values in which the last name was incorrectly parsed/had suffixes
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
        'input_remerge_file' : 'input/complaints-accused-cms_2016-2021_2021-09_p675956.csv.gz',
        'output_remerge_file' : 'output/complaints-accused-cms_2016-2021_2021-09_p675956.csv.gz',
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    base_merge.merge_dict['cr_id'] = ['cr_id', '']

    # cast cr_id to string in ref here
    ref_df = pd.read_csv(cons.input_reference_file)
    ref_df.cr_id = ref_df.cr_id.fillna('').astype(str).str.replace(".0", "", regex=False)

    sup_df = pd.read_csv(cons.input_profiles_file)

    custom_merge = Merge(name='custom merge', custom_merges=cons.custom_merges)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, null_flag_cols=['appointed_date'], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2021)\
        .loop_merge(merges=[base_merge, null_appointed_date_supplemental_merge, custom_merge])\
        .append_to_reference(keep_sup_um=False)\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
    
    # some basic prep for resolve: reread output file, could probably be refactored
    log.info("Preparing accused file for resolve.")
    df = pd.read_csv(cons.output_remerge_file)
    cr_ids = set(df.cr_id)

    # TODO: rename in import step, not here
    df = df.loc[df['UID'].notnull(), ['cr_id', 'UID', 'complaint_code', 'category_tier_1', 
                                      'cateogry_tier_2', 'category_tier_3', 
                                      'category_tier_4', 'final_finding', 
                                      'recommended_penalty', 'final_penalty']]\
        .rename(columns={"cateogry_tier_2": "category_tier_2"}) \
        .drop_duplicates()

    # accusations without UID
    accused_drop = cr_ids - set(df.cr_id)
    log.warning(f"Dropped {len(accused_drop)} cr_ids for not having UIDs associated")

    cr_ids = set(df.cr_id)

    # replace final finding with values temporarily to sort/prioritize on grouping
    df['final_finding'] = df['final_finding'].replace({"Sustained": "SU", 'Not Sustained': 'NS', 'Unfounded': "UN", 'EXONERATED': "EX"})
    df['ff_val'] = df['final_finding'].replace({'SU' : 1, 'EX' : 2, 'UN' : 3, 'NS' : 4, 'NAF' : 5})
    df = pd.concat([
            keep_duplicates(df, ['cr_id', 'UID'])\
                .sort_values(['cr_id', 'UID', 'ff_val'])\
                .groupby(['cr_id', 'UID'], as_index=False)\
                .first(),
            remove_duplicates(df, ['cr_id', 'UID'])])\
        .drop('ff_val', axis=1)

    assert keep_duplicates(df, ['cr_id', 'UID']).empty, "Duplicate cr_id, UID found"
    assert set(df.cr_id) == cr_ids, "Missing a cr_id after processing"
    assert df[df.cr_id.isnull()].empty, "All accused must have a cr_id"

    df.cr_id = df.cr_id.astype(str) 

    df.to_csv(cons.output_remerge_file, **cons.csv_opts)
