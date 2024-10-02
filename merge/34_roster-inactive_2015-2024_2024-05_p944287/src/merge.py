#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 34_roster-inactive_2015-2024_2024-05_p944287'''

import pandas as pd
import numpy as np
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge, 
    null_appointed_date_supplemental_merge, 
    null_appointed_date_reference_merge,
    null_appointed_date_and_birth_supplemental_merge, 
    null_appointed_date_and_birth_reference_merge,
    multirow_merge, 
    married_merge,
    remove_duplicate_merges_filter
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
        'data_id': 'roster-inactive_2015-2024_2024-05_p944287_ID',
        'input_profiles_file' : 'input/roster-inactive_2015-2024_2024-05_p944287_profiles.csv.gz',
        'add_cols' : ["BY_to_CA", "current_age", "F4FN", "F4LN"],
        'custom_merges': [
            # ['star', 'first_name_NS', 'last_name_NS', 'birth_year', 'middle_initial', 'gender', 'race'],
            # ['first_name_NS', 'last_name_NS', 'birth_year', 'middle_initial', 'gender', 'race'],
            # ['star', 'first_name_NS', 'middle_initial', 'appointed_date', 'birth_year'] # bryan schaeffer to wong
        ],
        'reshape_cols': {'index': 'cr_id'},
        'date_cols' : ['incident_date', 'complaint_date', 'closed_date'],
        'input_remerge_file' : 'input/roster-inactive_2015-2024_2024-05_p944287.csv.gz',
        'output_remerge_file' : 'output/roster-inactive_2015-2024_2024-05_p944287.csv.gz',
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

    # custom_merge = Merge(name='custom merge', custom_merges=cons.custom_merges)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols)\
        .loop_merge([base_merge, 
                     null_appointed_date_supplemental_merge])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
    
