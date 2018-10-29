#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''assign-unique-ids script for salary_2002-2017_2017-09_'''

import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
from merge_functions import ReferenceData
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
        'input_file': 'input/salary_2002-2017_2017-09.csv.gz',
        'output_file': 'output/salary_2002-2017_2017-09.csv.gz',
        'output_profiles_file': 'output/salary_2002-2017_2017-09_profiles.csv.gz',
        'id_cols': [
            'first_name_NS', 'last_name_NS',
            'middle_initial', 'middle_initial2',
            'suffix_name', 'gender'
            ],
        'sub_id_cols': [
            'first_name_NS', 'last_name_NS',
            'middle_initial', 'middle_initial2',
            'suffix_name', 'gender',
             'year'
            ],
        'profile_cols' : [
            'salary_2002-2017_2017-09_ID', 'age_at_hire', 'gender',
            'first_name', 'last_name',
            'first_name_NS', 'last_name_NS', 'suffix_name',
            'middle_initial', 'middle_initial2', 'so_max_year', 'so_min_year',
            'so_max_year_m1', 'so_min_year_m1',
            'so_max_date', 'so_min_date', 'current_age_p1', 'current_age2_pm',
            'current_age_m1', 'current_age_pm', 'current_age_mp',
             'current_age2_p1','current_age2_m1','current_age2_mp',
            'start_date', 'org_hire_date', 'resignation_year'
        ],
        'sub_conflict_cols': ['org_hire_date', 'age_at_hire'],
        'id': 'salary_2002-2017_2017-09_ID',
        'sub_id': 'salary-year_2002-2017_2017-09_ID',
        'ad_cols': ['start_date', 'org_hire_date'],
        'sid': 'salary_2002-2017_2017-09_ID',
        'year_id': 'salary-year_2002-2017_2017-09_ID',
        'custom_merges': [
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'suffix_name','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'suffix_name', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'suffix_name','org_hire_date', 'age_at_hire'],
            ['F4FN', 'F4LN', 'suffix_name','org_hire_date', 'start_date', 'age_at_hire'],
            ['F4FN', 'F4LN', 'suffix_name', 'start_date', 'age_at_hire'],
            ['F4FN', 'F4LN', 'suffix_name','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS','start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'middle_initial', 'suffix_name','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'org_hire_date', 'spp_date', 'salary', 'pay_grade'],
            ['first_name_NS', 'last_name_NS', 'spp_date','org_hire_date', 'start_date', 'pay_grade'],
            ['first_name_NS', 'last_name_NS', 'spp_date','org_hire_date', 'pay_grade'],
            ['first_name_NS', 'last_name_NS', 'spp_date','start_date', 'pay_grade'],
            ['first_name_NS', 'last_name_NS', 'org_hire_date'],
            ['first_name_NS', 'last_name_NS', 'start_date'],
            ['first_name_NS', 'middle_initial', 'spp_date', 'start_date', 'org_hire_date', 'pay_grade'],
            ['first_name_NS', 'spp_date', 'start_date', 'org_hire_date', 'pay_grade', 'age_at_hire']
        ]
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_csv(cons.input_file)

full_df = pd.DataFrame()

for year in df['year'].unique():
    log.info('Assigning unique sub-ids for year: %d', year)
    sub_df = df[df['year']==year]
    sub_df = assign_unique_ids(sub_df, cons.sub_id,
                               cons.sub_id_cols,
                               conflict_cols=cons.sub_conflict_cols,
                               log=log)
    sub_df[cons.sub_id] = sub_df[cons.sub_id] + year * 100000
    full_df = full_df.append(sub_df)

assert full_df.shape[0] == df.shape[0],\
    print('Remerged data does not match input dataset')

df = full_df

log.info("Beginning self-merge process")

for year in range(2002, 2018):
    dfy = df[df['year'] == year].copy()
    yid = cons.year_id.replace('year', str(year))
    dfy.rename(columns={cons.year_id: yid},
               inplace=True)
    if year == 2002:
        sd = ReferenceData(dfy, uid=cons.sid, data_id=yid, log=log)
    else:
        sd = (sd.add_sup_data(dfy, add_cols=['F4FN', 'F4LN'], base_OD=[])
                .loop_merge(custom_merges=cons.custom_merges, verbose=False)
                .append_to_reference())
log.info('Number of unique IDs = %d', len(sd.ref_df[cons.sid].unique()))

sd.write_reference(cons.output_file, cons.csv_opts)

sal = sd.ref_df

res_years = sal[[cons.id, 'year']].groupby(cons.id, as_index=False).max().rename(columns={'year' : 'resignation_year'})
sal = sal[[col for col in sal.columns if col in cons.profile_cols]].drop_duplicates()
assert sal[sal['start_date'].isnull() & sal['org_hire_date'].isnull()].empty

log.info('Creating so_max_date and so_min_date from max/min'
         ' of start_date and org_hire_date')

sal['so_max_date'] = sal[cons.ad_cols]\
                            .apply(pd.to_datetime)\
                            .apply(max, axis=1).dt.date
sal['so_min_date'] = sal[cons.ad_cols]\
                            .apply(pd.to_datetime)\
                            .apply(min, axis=1).dt.date
sal.loc[sal['so_max_date'].isnull() &
        sal['start_date'].notnull(),
        'so_max_date'] = sal.loc[sal['so_max_date'].isnull() &
                                 sal['start_date'].notnull(),
                                 'start_date']
sal.loc[sal['so_min_date'].isnull() &
        sal['start_date'].notnull(),
        'so_min_date'] = sal.loc[sal['so_min_date'].isnull() &
                                 sal['start_date'].notnull(),
                                 'start_date']
sal.loc[sal['so_max_date'].isnull() &
        sal['org_hire_date'].notnull(),
        'so_max_date'] = sal.loc[sal['so_max_date'].isnull() &
                                 sal['org_hire_date'].notnull(),
                                 'org_hire_date']
sal.loc[sal['so_min_date'].isnull() &
        sal['org_hire_date'].notnull(),
        'so_min_date'] = sal.loc[sal['so_min_date'].isnull() &
                                 sal['org_hire_date'].notnull(),
                                 'org_hire_date']

log.info('Creating so_max_year and so_min_year from so_max_date and so_min_date')
sal['so_max_year'] = pd.to_datetime(sal['so_max_date']).dt.year
sal['so_min_year'] = pd.to_datetime(sal['so_min_date']).dt.year
sal['so_max_year_m1'] = sal['so_max_year']-1
sal['so_min_year_m1'] = sal['so_min_year']-1

log.info('Creating current_age columns from so_max_year and age_at_hire')
sal['current_age_p1'] = 2017 - (sal['so_min_year'] - sal['age_at_hire']) + 1
sal['current_age_m1'] = 2017 - (sal['so_min_year'] - sal['age_at_hire'])
sal['current_age_pm'] = sal['current_age_p1']
sal['current_age_mp'] = sal['current_age_m1']

sal['current_age2_p1'] = 2017 - (sal['so_max_year'] - sal['age_at_hire']) + 1
sal['current_age2_m1'] = 2017 - (sal['so_max_year'] - sal['age_at_hire'])
sal['current_age2_pm'] = sal['current_age2_p1']
sal['current_age2_mp'] = sal['current_age2_m1']

log.info('Adding resignation_year column = max year of observation')
sal = sal.merge(res_years, on=cons.id, how='left')
log.info('Exporting profiles data set with %s columns', cons.profile_cols)
sal[cons.profile_cols]\
    .drop_duplicates()\
    .to_csv(cons.output_profiles_file, **cons.csv_opts)
