#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute) / Ashwin Sharma (Invisible Institute)

'''merge script for 03_unit-history__2016-03_16-1105'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import (
    default_merges, 
    base_merge,
    multirow_merge,
    null_appointed_date_reference_merge,
    null_appointed_date_supplemental_merge,
    married_merge,
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
        'data_id': 'unit-history__2016-03_ID',
        'from_year': 2016,
        'input_profiles_file' : 'input/unit-history__2016-03_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN', 'L4FN', 'BY_to_CA', 'current_age'],
        'null_flag_cols': ['appointed_date'],
        'whitelist': {'matched_on': 
                      {0: 'first_name_NS-last_name_NS-current_age_m1-gender-race-current_unit',
                       1: 'first_name_NS-current_age_p1-appointed_date-current_unit-gender-race',
                       2: 'star-appointed_date-current_unit-current_age_p1-gender'},
                    'UID': {0: 123057,
                            1: 112858,
                            2: 129956},
                    'unit-history__2016-03_ID': {0: 21562, 
                                                 1: 14901, 
                                                 2: 1917}},
        'input_remerge_file' : 'input/unit-history__2016-03.csv.gz',
        'output_remerge_file' : 'output/unit-history__2016-03.csv.gz'
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

    ref_df.appointed_date = pd.to_datetime(ref_df.appointed_date)
    sup_df.appointed_date = pd.to_datetime(sup_df.appointed_date)

    # 3 matches: peter palka matches on everything but appointed date, has an earlier one in sup
    # jennifer mcclendon to koniarski, and valena bradley to nacala bey: have the exact same unit history
    whitelist_df = pd.DataFrame(cons.whitelist)

    merges = [base_merge, multirow_merge, null_appointed_date_reference_merge,
            null_appointed_date_supplemental_merge, married_merge,
            whitelist_merge(whitelist_df, cons.universal_id, cons.data_id)]


    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, 
                       null_flag_cols=cons.null_flag_cols,
                       log=log)\
        .add_sup_data(sup_df, add_cols=cons.add_cols, data_id=cons.data_id ,
                      from_year=cons.from_year)\
        .loop_merge(merges=merges)\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
