#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''merge script for 11_roster__2018-03_p441436'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from merge_data import Merge
from default_merges import (
    default_merges, 
    base_merge, 
    married_merge, 
    null_appointed_date_supplemental_merge,
    null_appointed_date_reference_merge,
    whitelist_merge
)
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
        'input_profiles_file' : 'input/roster__2018-03_profiles.csv.gz',
        'add_cols' : [
            "F4FN", "F4LN", "BY_to_CA", "current_age",
            {'id' : 'roster__2018-03_ID',
             'exec' : 'df["appointed_date2"], df["appointed_date3"] = df["appointed_date"], df["appointed_date"]'},
            {'id' : 'UID',
             'exec' : 'df["appointed_date2"], df["appointed_date3"] = df["start_date"], df["org_hire_date"]'}
        ],
        'data_id': 'roster__2018-03_ID',
        'from_year': 2018,
        'custom_merges' : [
            ['first_name_NS', 'middle_initial', 'race', 'gender', 'current_age_m1', 'appointed_date', 'star'], # robert sterling to robert rhodes
            ['last_name_NS', 'middle_initial', 'appointed_date', 'star', 'race', 'current_age_p1'], # jimmy woods to megan woods
            ['first_name_NS', 'last_name_NS', 'current_age_m1', 'middle_initial', 'gender', 'race'], # rony d mammoo appointed date update
            ['first_name_NS', 'last_name_NS', 'current_age_p1', 'middle_initial', 'gender', 'race'], # peter p biedron appointed date update
                ],
        'custom_married_merges': [
            ['first_name_NS', 'race', 'current_age_p1', 'appointed_date', 'star'],
            ['first_name_NS', 'race', 'current_age_m1', 'appointed_date', 'star'],
            ['first_name_NS', 'current_age_p1', 'appointed_date', 'star'],
            ['first_name_NS', 'current_age_m1', 'appointed_date', 'star'],
            ['first_name_NS', 'current_age_p1', 'appointed_date'],
            ['first_name_NS', 'current_age_m1', 'appointed_date']
        ],
        'whitelist_dict': { # dillan halley: later on matches with star, bad data from only salary here, has an appointed date change
            'matched_on': 'first_name_NS-last_name_NS-middle_initial-gender',
            'UID': 132742,
            'roster__2018-03_ID': 10398,
        },
        'base_OD_edits' : {
            'appointed_date': ['appointed_date', 'appointed_date2', 'appointed_date3']},
        'input_remerge_file' : 'input/roster__2018-03.csv.gz',
        'output_remerge_file' : 'output/roster__2018-03.csv.gz',
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

    sup_df.rename(columns={"current_star": "star"}, inplace=True)

    base_merge.merge_dict['appointed_date'] = ['appointed_date', 'appointed_date2', 'appointed_date3']
    base_merge.merge_dict['birth_year'] = ['birth_year', 'current_age', '']

    married_merge.custom_merges = cons.custom_married_merges

    custom_merge = Merge(name='custom_merges', custom_merges=cons.custom_merges)

    whitelist_df = pd.DataFrame(cons.whitelist_dict, index=[0])

    whitelist = whitelist_merge(whitelist_df, cons.universal_id, cons.data_id)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, 
                       null_flag_cols=['appointed_date'], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=cons.from_year)\
        .loop_merge(merges=[base_merge, 
                            null_appointed_date_supplemental_merge, 
                            null_appointed_date_reference_merge,
                            married_merge, 
                            custom_merge,
                            whitelist])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)