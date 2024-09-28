#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''assign-unique-ids script for salary_2017-2023_2023-02'''


import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data, intrafile_dedupe, get_intrafile_changes, filter_changes, apply_changes
import numpy as np
from reference_data import ReferenceData
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
        'input_file': 'input/salary_2017-2023_2023-02.csv.gz',
        'output_file': 'output/salary_2017-2023_2023-02.csv.gz',
        'output_profiles_file': 'output/salary_2017-2023_2023-02_profiles.csv.gz',
        'id': 'salary_2017-2023_2023-02_ID',
        'sub_id': 'salary-year_2017-2023_2023-02_ID',
        'id_cols': ['first_name_NS', 'last_name_NS', 'gender', 'middle_initial', 'age_at_hire', 'appointed_date', 'rank', 'org_hire_date'],
        'conflict_cols' : ['spp_date', 'org_hire_date', 'start_date', 'salary', 'employee_status'],
        'add_cols': {
            "age_at_hire":  {
                'age_at_hire_diff': ('age_at_hire', lambda x: x.replace("", np.nan).max() - x.replace("", np.nan).min())
                }
            },
        'change_filters': {
            "last_name_NS": "gender == 'FEMALE'",
            "middle_initial": "middle_initial == middle_initial", # all pass
            "age_at_hire": "age_at_hire_diff <= 1"
            },
        'change_whitelist': {
            "first_name_NS": ['DAVE, DAVID'],
            "last_name_NS": ["CAMPBELL, CAMPBELLSADLER"]
        },
        'change_blacklist': {},
        "base_OD": {
            "first_name_NS": ["first_name_NS"],
            "last_name_NS": ["last_name_NS"],
            "age_at_hire": ["age_at_hire", ""],
            "appointed_date": ['appointed_date', 'org_hire_date', 'spp_date', 'start_date'],
            "middle_initial": ["middle_initial"],
            "suffix_name": ["suffix_name", ""],
            "pay_grade": ['pay_grade', ""]},
        'custom_merges': [
            {"query": "gender == 'FEMALE'",
             "cols": ["first_name_NS", "age_at_hire", "start_date", "spp_date"]}],
        'ad_cols': ['appointed_date', 'start_date', 'spp_date', 'org_hire_date'],
        'profile_cols' : [
            'salary_2017-2023_2023-02_ID', 'age_at_hire', 'gender', 'appointed_date',
            'first_name', 'last_name', 'rank',
            'first_name_NS', 'last_name_NS', 'suffix_name',
            'middle_initial', 'middle_initial2', 'so_max_year', 'so_min_year',
            'so_max_year_m1', 'so_min_year_m1',
            'so_max_date', 'so_min_date', 'current_age_p1', 'current_age2_pm',
            'current_age_m1', 'current_age_pm', 'current_age_mp',
            'current_age2_p1','current_age2_m1','current_age2_mp',
            'start_date', 'spp_date', 'appointed_date', 'org_hire_date'
        ],
        }
    

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_csv(cons.input_file)

    df = df.fillna("")
    
    year_dfs = []

    max_id = 0
    log.info("Assigning unique ids per year")
    for year, year_df in df.groupby('year'):
        log.info(f"Year {year}")
        log.info(f"Initial assign unique ids")
        year_df = assign_unique_ids(year_df, cons.sub_id, cons.id_cols, log=log)

        year_dfs.append(year_df.assign(year=year))

    full_df = pd.concat(year_dfs, axis=0)
    df = full_df

    log.info("Starting self merge between years")
    for year, year_df in df.groupby("year"):
        # yid = cons.sub_id.replace('year', str(year))
        if year == 2017:
            sd = ReferenceData.from_first_file(year_df, id=cons.id, data_id=cons.sub_id, starting_id=1,log=log)
        else:
            # year_df.rename(columns={cons.sub_id: yid},
                    # inplace=True)
            sd = (sd.reset_reference() \
                .add_sup_data(year_df, data_id=cons.sub_id, one_to_one=False, add_cols=['F4FN', 'F4LN'])
                .loop_merge([Merge(merge_dict=cons.base_OD, check_duplicates=False)])
                .append_to_reference())

    sd.write_reference(cons.output_file, cons.csv_opts)

    sal = sd.reference.df

 
    sal = sal[[col for col in sal.columns if col in cons.profile_cols]].drop_duplicates()

    sal['so_max_date'] = sal[cons.ad_cols]\
                                .apply(pd.to_datetime)\
                                .apply(max, axis=1).dt.date
    sal['so_min_date'] = sal[cons.ad_cols]\
                                .apply(pd.to_datetime)\
                                .apply(min, axis=1).dt.date


    log.info('Creating so_max_year and so_min_year from so_max_date and so_min_date')
    sal['so_max_year'] = pd.to_datetime(sal['so_max_date']).dt.year
    sal['so_min_year'] = pd.to_datetime(sal['so_min_date']).dt.year
    sal['so_max_year_m1'] = sal['so_max_year']-1
    sal['so_min_year_m1'] = sal['so_min_year']-1

    log.info('Creating current_age columns from so_max_year and age_at_hire')
    sal['current_age_p1'] = 2023 - (sal['so_min_year'] - sal['age_at_hire']) + 1
    sal['current_age_m1'] = 2023 - (sal['so_min_year'] - sal['age_at_hire'])
    sal['current_age_pm'] = sal['current_age_p1']
    sal['current_age_mp'] = sal['current_age_m1']

    sal['current_age2_p1'] = 2023 - (sal['so_max_year'] - sal['age_at_hire']) + 1
    sal['current_age2_m1'] = 2023 - (sal['so_max_year'] - sal['age_at_hire'])
    sal['current_age2_pm'] = sal['current_age2_p1']
    sal['current_age2_mp'] = sal['current_age2_m1']

    log.info('Exporting profiles data set with %s columns', cons.profile_cols)
    sal[cons.profile_cols]\
        .drop_duplicates()\
        .to_csv(cons.output_profiles_file, **cons.csv_opts)
