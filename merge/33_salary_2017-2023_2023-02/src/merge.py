#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 32_salary_2017-2023_2023-02'''


import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge, 
    married_merge, 
    remove_duplicate_merges_filter, 
    remove_ref_duplicate_merges_filter,
    remove_sup_duplicate_merges_filter
)
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
        'data_id': 'salary_2017-2023_2023-02_ID',
        'input_profiles_file' : 'input/salary_2017-2023_2023-02_profiles.csv.gz',
        'add_cols' : [            'F4FN', 'F4LN','L4LN', 'F1FN','F3LN', 'BY_to_CA',
            {'id' : 'UID',
             'exec' : "df['current_age_mp'], df['current_age_pm'], df['current_age2_mp'], df['current_age2_pm'], df['current_age2_p1'], df['current_age2_m1'], df['current_age2_mp2'] = df['current_age_p1'], df['current_age_m1'], df['current_age_p1'], df['current_age_m1'], df['current_age_p1'], df['current_age_m1'], df['current_age_p1'] + 1"},
            {'id' : 'UID',
             'exec' : "df['so_max_date'], df['so_min_date']= zip(*df['appointed_date'].apply(lambda x: (x,x)))"},
            {'id': 'UID',
             'exec': "df['org_hire_date'] = df['appointed_date']"},
            {'id': 'UID',
             'exec': "df['org_hire_date'] = df['appointed_date']"},
            {'id' : 'UID',
             'exec' :
                "df['so_max_year'],df['so_min_year'], df['so_min_year_m1'], df['so_max_year_m1'] = zip(*pd.to_datetime(df['appointed_date']).dt.year.apply(lambda x: (x,x,x,x)))"},],
        'merge_dict' : {
                'first_name': ['first_name_NS', 'F4FN'],
                'last_name': ['last_name_NS', 'F3LN'],
                'middle_initial': ['middle_initial' , ''],
                'middle_initial2': ['middle_initial2', ''],
                'suffix_name': ['suffix_name', ''],
                'appointed_date': ['appointed_date', 'org_hire_date', 'so_min_date', 'so_max_date',
                                    'so_min_year', 'so_max_year',
                                    'so_min_year_m1', 'so_max_year_m1'],
                'birth_year': ['birth_year', 'current_age',
                                'current_age_m1', 'current_age2_m1',
                                'current_age_p1', 'current_age2_p1',
                                'current_age_mp', 'current_age2_mp'
                                'current_age_pm','current_age2_pm', ''],
                'gender': ['gender', ''],
                'race': ['race',''],
                'current_unit': ['current_unit','']},
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
        'input_remerge_file' : 'input/salary_2017-2023_2023-02.csv.gz',
        'output_remerge_file' : 'output/salary_2017-2023_2023-02.csv.gz',
        }
    

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    
    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file)

    sup_df.rename(columns={"rank": "current_rank"}, inplace=True)

    sup_df['current_rank'] = sup_df['current_rank'].map({"SERGEANT": "SERGEANT OF POLICE",
                                                         'SUPERINTENDENT': "SUPERINTENDENT OF POLICE",
                                                         "CAPTAIN": "CAPTAIN OF POLICE"})
    
    # file not deduplicated by years, expecting many dupes per UID
    base_merge.filter_supplemental_merges_flag = False
    base_merge.check_duplicates = False
    base_merge.merge_dict['current_rank'] = ['current_rank', '']

    first_merge = Merge(name='first merge', 
                        merge_dict={**base_merge.merge_dict,
                                    'appointed_date': ['appointed_date', 'org_hire_date', 'so_min_date', 'so_max_date', 'so_min_year', 'so_max_year']},
                                    # 'appointed_date': ['org_hire_date']},
                        check_duplicates=False,
                        merge_postprocess=remove_sup_duplicate_merges_filter,
                        filter_reference_merges_flag=False)
    
    rank_merge = Merge(name='rank merge',
                              merge_dict={**base_merge.merge_dict,
                                          'first_name': ['first_name_NS'],
                                          'last_name': ['last_name_NS'],
                                          'current_rank': ['current_rank'],
                                          'appointed_date': ['appointed_date', 'org_hire_date', 'so_min_date', 'so_max_date', 'so_min_year', 'so_max_year', '']},
                              merge_postprocess=remove_sup_duplicate_merges_filter,
                              filter_reference_merges_flag=False,
                              check_duplicates=False)


    loose_merge = Merge(name='loose merge',
                        merge_dict={**base_merge.merge_dict,
                                    'appointed_date': ['appointed_date', 'so_min_date', 'so_max_date', 'so_min_year', 'so_max_year', '']},
                        merge_postprocess=remove_sup_duplicate_merges_filter,
                        filter_reference_merges_flag=False,
                        check_duplicates=False)


    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, null_flag_cols=['appointed_date'], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, one_to_one=False, from_year=2023)\
        .loop_merge([first_merge, rank_merge, loose_merge])\
        .append_to_reference(keep_sup_um=False)\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)