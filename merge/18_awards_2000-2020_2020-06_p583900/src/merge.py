#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 18_awards_2000-2020_2020-06_p583900'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge,
    null_appointed_date_reference_merge,
    null_appointed_date_and_birth_reference_merge,
    married_merge
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
        'data_id': 'awards_2000-2020_2020-06_p583900_ID',
        'input_profiles_file' : 'input/awards_2000-2020_2020-06_p583900_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN'],
        'custom_merges': [
            ['star', 'first_name_NS', 'last_name_NS', 'birth_year', 'middle_initial', 'gender', 'race'],
            ['first_name_NS', 'last_name_NS', 'birth_year', 'middle_initial', 'gender', 'race']
        ],
        'input_remerge_file' : 'input/awards_2000-2020_2020-06_p583900.csv.gz',
        'output_remerge_file' : 'output/awards_2000-2020_2020-06_p583900.csv.gz'
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

    custom_merge = Merge(name='custom merges', custom_merges=cons.custom_merges)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2020)\
        .loop_merge([base_merge, 
                     null_appointed_date_reference_merge, 
                     null_appointed_date_and_birth_reference_merge, 
                     married_merge,
                     custom_merge])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
