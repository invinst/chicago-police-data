#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute) / Ashwin Sharma

'''merge script for 05_salary_2002-2017_2017-09_'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import (
    default_merges, 
    base_merge,
    whitelist_merge,
    remove_sup_duplicate_merges_filter
    # multirow_merge,
    # first_name_substring_merge,
    # last_name_substring_merge,
    # first_name_fuzzy_match_merge,
    # last_name_fuzzy_match_merge
)
from copy import deepcopy
from merge_data import Merge
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
        'data_id': 'salary_2002-2017_2017-09_ID',
        'from_year': 2017,
        'input_profiles_file': 'input/salary_2002-2017_2017-09_profiles.csv.gz',
        'add_cols' : [
            'F4FN', 'F4LN','L4LN', 'F1FN','F3LN', 'BY_to_CA',
            {'id' : 'UID',
             'exec' : "df['current_age_mp'], df['current_age_pm'], df['current_age2_mp'], df['current_age2_pm'], df['current_age2_p1'], df['current_age2_m1'], df['current_age2_mp2'] = df['current_age_p1'], df['current_age_m1'], df['current_age_p1'], df['current_age_m1'], df['current_age_p1'], df['current_age_m1'], df['current_age_p1'] + 1"},
            {'id' : 'UID',
             'exec' : "df['so_max_date'], df['so_min_date'] = zip(*df['appointed_date'].apply(lambda x: (x,x)))"},
            {'id' : 'UID',
             'exec' :
                "df['so_max_year'],df['so_min_year'], df['so_min_year_m1'], df['so_max_year_m1'] = zip(*pd.to_datetime(df['appointed_date']).dt.year.apply(lambda x: (x,x,x,x)))"},
            {'id' : 'UID',
             'exec' : "df['org_hire_date'] = df['resignation_date']"},
            {'id' : 'UID',
             'exec' : "df['resignation_year'] = pd.to_datetime(df['resignation_date']).dt.year"},
            {'id' : 'salary_2002-2017_2017-09_ID',
             'exec' : "df['resignation_date'] = df['org_hire_date']"},
            {'id' : 'salary_2002-2017_2017-09_ID',
             'exec' : "df['current_age2_mp2'] = df['current_age_mp']"}
             ],
        'merge_dict' : {
                 'first_name': ['first_name_NS', 'F4FN'],
                 'last_name': ['last_name_NS', 'F3LN'],
                 'middle_initial': ['middle_initial' , ''],
                 'middle_initial2': ['middle_initial2', ''],
                 'suffix_name': ['suffix_name', ''],
                 'appointed_date': ['so_min_date', 'so_max_date',
                                    'so_min_year', 'so_max_year',
                                    'so_min_year_m1', 'so_max_year_m1',
                                    'resignation_year'],
                 'birth_year': ['birth_year', 'current_age',
                                'current_age_m1', 'current_age2_m1',
                                'current_age_p1', 'current_age2_p1',
                                'current_age_mp', 'current_age2_mp'
                                'current_age_pm','current_age2_pm', ''],
                 'gender': ['gender', ''],
                 'race': ['race',''],
                 'current_unit': ['current_unit','']
        } ,
            'custom_merges' : [
                ['first_name_NS', 'L4LN', 'so_max_date'],
                ['first_name_NS', 'L4LN', 'so_min_date'],
                ['F1FN', 'last_name_NS', 'so_max_date'],
                ['F1FN', 'last_name_NS', 'so_min_date'],
                ['first_name_NS', 'last_name_NS', 'middle_initial', 'current_age2_mp2'],
                ['first_name_NS', 'last_name_NS', 'current_age_mp'],
                ['first_name_NS', 'last_name_NS', 'current_age_pm'],
                ['first_name_NS', 'last_name_NS', 'current_age_mp'],
                ['first_name_NS', 'last_name_NS', 'current_age2_m1'],
                ['first_name_NS', 'last_name_NS', 'current_age2_p1'],
                ['first_name_NS', 'last_name_NS', 'current_age2_pm'],
                ['first_name_NS', 'last_name_NS', 'current_age2_mp'],
                ['F4FN', 'last_name_NS', 'current_age_pm'],
                ['F4FN', 'last_name_NS', 'current_age_mp'],
                ['F4FN', 'last_name_NS', 'current_age2_pm'],
                ['F4FN', 'last_name_NS', 'current_age2_mp'],
                ['F4FN', 'last_name_NS', 'current_age2_m1'],
                ['F4FN', 'last_name_NS', 'current_age2_p1']],

            'last_name_changes': [
                 ['first_name_NS', 'so_max_date', 'current_age_pm', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'current_age2_pm', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'current_age_mp', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'current_age2_mp', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'current_age_p1', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'current_age_m1', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'current_age2_m1', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'current_age_pm', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'current_age2_pm', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'current_age_mp', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'current_age2_mp', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'current_age_p1', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'current_age2_p1', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'current_age_m1', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'current_age2_m1', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'middle_initial'],
                 ['first_name_NS', 'so_min_date', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'middle_initial'],
                 ['first_name_NS', 'so_max_date', 'current_age_mp'],
                 ['first_name_NS', 'so_max_date', 'current_age2_mp'],
                 ['first_name_NS', 'so_max_date', 'current_age_mp'],
                 ['first_name_NS', 'so_max_date', 'current_age2_mp'],
                 ['first_name_NS', 'so_max_date', 'current_age_p1'],
                 ['first_name_NS', 'so_max_date', 'current_age2_p1'],
                 ['first_name_NS', 'so_max_date', 'current_age_m1'],
                 ['first_name_NS', 'so_max_date', 'current_age2_m1']],
            'whitelist_merge': { # curtistee thompson to curtisteen gilbert, odd case that comes up later
                'matched_on': ['F4FN-current_age_p1-so_min_date-gender-middle_initial'],
                'UID': [104609],
                'salary_2002-2017_2017-09_ID': [1966.0]
            },
            'merge_dict_edits' : {
                'birth_year': ['birth_year', 'current_age',
                                'current_age_m1', 'current_age2_m1',
                                'current_age_p1', 'current_age2_p1',
                                'current_age_mp', 'current_age2_mp'
                                'current_age_pm','current_age2_pm', ''],
                'appointed_date': {'so_min_date', 'so_max_date',
                                    'so_min_year', 'so_max_year',
                                    'so_min_year_m1', 'so_max_year_m1',
                                    'resignation_year'}
            },
        'input_remerge_file' : 'input/salary_2002-2017_2017-09.csv.gz',
        'output_remerge_file' : 'output/salary_2002-2017_2017-09.csv.gz'
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file)

    first_merge = Merge(name='first_merge',
                        merge_dict=cons.merge_dict,
                        check_duplicates=False,
                        merge_postprocess=remove_sup_duplicate_merges_filter)
    
    custom_merges = Merge(name='custom_merges',
                          custom_merges=cons.custom_merges,
                          check_duplicates=False,
                          merge_postprocess=remove_sup_duplicate_merges_filter)
    
    last_name_changes = Merge(name='last_name_changes',
                              custom_merges=cons.last_name_changes,
                              query="gender == 'FEMALE'",
                              check_duplicates=False,
                              merge_postprocess=remove_sup_duplicate_merges_filter)
    
    whitelisted_merge = whitelist_merge(cons.whitelist_merge, cons.universal_id, cons.data_id)

    merges = [first_merge, custom_merges, last_name_changes, whitelisted_merge]

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols,
                       null_flag_cols=[], log=log) \
                    .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, one_to_one=False,
                                  from_year=cons.from_year) \
                    .loop_merge(merges=merges) \
                    .append_to_reference(drop_cols=['F4FN', 'F4LN','L4LN', 'F1FN','F3LN', 'resignation_date',
                                                    'resignation_year', 'current_age2_mp2', 'current_age_m1',
                                                    'current_age2_m1', 'current_age_p1', 'current_age2_p1',
                                                    'current_age_mp', 'current_age2_mp', 'current_age_pm',
                                                    'current_age2_pm', 'so_max_year_m1', 'so_min_year_m1',
                                                    'so_max_year', 'so_min_year']) \
                    .remerge_to_file(cons.input_remerge_file, cons.output_remerge_file, cons.csv_opts) \
                    .write_reference(cons.output_reference_file, cons.csv_opts)
