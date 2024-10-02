#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 15_roster_1981-2021_2021-08_p679629

NOTE: there is an expectation that there will be a lot of unmerged records, 
but that they will be almost all after 2018 since this is the first file with data past that date.

Should only be more lenient with data before that date. 
'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge, 
    null_appointed_date_reference_merge, 
    null_appointed_date_supplemental_merge, 
    null_appointed_date_and_birth_reference_merge,
    null_appointed_date_and_birth_supplemental_merge,
    multirow_merge,
    multirow_merge_process, 
    married_merge
)
from merge_data import Merge
from functools import partial
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
        'data_id': 'roster_1981-2021_2021-08_p679629_ID',
        'input_profiles_file' : 'input/roster_1981-2021_2021-08_p679629_profiles.csv.gz',
        'add_cols' : [
            "F4FN", "F4LN", "BY_to_CA",
        ],
        'custom_merges' : [
            ['first_name_NS', 'last_name_NS', 'birth_year', 'middle_initial', 'gender'],
            ['star', 'last_name_NS', 'appointed_date', 'middle_initial', 'gender', 'race'] #tom to tomasz surma 
        ],
        'input_remerge_file' : 'input/roster_1981-2021_2021-08_p679629.csv.gz',
        'output_remerge_file' : 'output/roster_1981-2021_2021-08_p679629.csv.gz',
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

    married_merge.custom_merges = [['first_name_NS', 'middle_initial', 'birth_year', 'appointed_date']]

    custom_merge = Merge(custom_merges=cons.custom_merges)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, 
                       null_flag_cols=['appointed_date', 'birth_year'], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2021)\
        .loop_merge([base_merge, 
                     null_appointed_date_reference_merge, 
                     null_appointed_date_and_birth_reference_merge,
                     multirow_merge,
                     married_merge,
                     custom_merge])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
