#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''merge script for 05_salary_2002-2017_2017-09_'''

import pandas as pd
import __main__

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
        'input_reference_file': 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID',
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
        'base_OD' : [
                 ('first_name', ['first_name_NS', 'F4FN']),
                 ('last_name', ['last_name_NS', 'F3LN']),
                 ('middle_initial', ['middle_initial' , '']),
                 ('middle_initial2', ['middle_initial2', '']),
                 ('suffix_name', ['suffix_name', '']),
                 ('appointed_date', ['appointed_date']),
                 ('birth_year', ['birth_year', 'current_age', '']),
                 ('gender', ['gender', '']),
                 ('race', ['race','']),
                 ('current_unit', ['current_unit',''])],
        'loop_merge' : {
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
                ['F4FN', 'last_name_NS', 'current_age2_p1'],
                {'query': 'gender == "FEMALE"',
                 'cols' :['first_name_NS', 'so_max_date', 'current_age_pm', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols' :['first_name_NS', 'so_max_date', 'current_age2_pm', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age_mp', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age2_mp', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age_p1', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age_m1', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age2_m1', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'current_age_pm', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'current_age2_pm', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'current_age_mp', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'current_age2_mp', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'current_age_p1', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'current_age2_p1', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'current_age_m1', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'current_age2_m1', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_min_date', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'middle_initial']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age_mp']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age2_mp']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age_mp']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age2_mp']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age_p1']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age2_p1']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age_m1']},
                {'query': 'gender == "FEMALE"',
                 'cols':['first_name_NS', 'so_max_date', 'current_age2_m1']},
                ['first_name_NS', 'last_name_NS', 'middle_initial', 'so_max_year_m1'],
                ['first_name_NS', 'last_name_NS', 'middle_initial', 'so_min_year_m1'],
                ['first_name_NS', 'last_name_NS', 'current_age_m1', 'resignation_date'],
                ],
            'verbose' : True,
            'base_OD_edits' :
            [
                ('birth_year', ['birth_year', 'current_age',
                                'current_age_m1', 'current_age2_m1',
                                'current_age_p1', 'current_age2_p1',
                                'current_age_mp', 'current_age2_mp'
                                'current_age_pm','current_age2_pm', '']),
                ('appointed_date', ['so_min_date', 'so_max_date',
                                    'so_min_year', 'so_max_year',
                                    'so_min_year_m1', 'so_max_year_m1',
                                    'resignation_year'])
            ],
            },
        'input_remerge_file' : 'input/salary_2002-2017_2017-09.csv.gz',
        'output_remerge_file' : 'output/salary_2002-2017_2017-09.csv.gz'
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.read_csv(cons.input_reference_file)
sup_df = pd.read_csv(cons.input_profiles_file)

rd = ReferenceData(ref_df, uid=cons.universal_id, log=log)\
        .add_sup_data(sup_df, add_cols=cons.add_cols, base_OD=cons.base_OD)\
        .loop_merge(**cons.loop_merge)
rd.merged_df.to_csv("output/merged_df.csv.gz", **cons.csv_opts)
rd.append_to_reference(keep_sup_um=False)\
    .add_sup_data(rd.sup_um, add_cols=[],
                  base_OD=cons.base_OD)\
    .loop_merge(verbose=True, base_OD_edits=[
      ('birth_year', ['birth_year', 'current_age',
                      'current_age_m1', 'current_age2_m1',
                      'current_age_p1', 'current_age2_p1',
                      'current_age_mp', 'current_age2_mp'
                      'current_age_pm','current_age2_pm', '']),
      ('appointed_date', ['so_min_date', 'so_max_date',
                          'so_min_year', 'so_max_year',
                          'so_min_year_m1', 'so_max_year_m1'])
    ])\
  .append_to_reference(drop_cols=[
    'F4FN', 'F4LN','L4LN', 'F1FN','F3LN', 'resignation_date',
    'resignation_year', 'current_age2_mp2', 'current_age_m1',
    'current_age2_m1', 'current_age_p1', 'current_age2_p1',
    'current_age_mp', 'current_age2_mp', 'current_age_pm',
    'current_age2_pm', 'so_max_year_m1', 'so_min_year_m1',
    'so_max_year', 'so_min_year'])\
  .remerge_to_file(cons.input_remerge_file,
                  cons.output_remerge_file,
                  cons.csv_opts)\
  .write_reference(cons.output_reference_file, cons.csv_opts)
