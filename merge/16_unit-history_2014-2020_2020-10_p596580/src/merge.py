#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 16_unit-history_2014-2020_2020-10_p596580'''


import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge,
    null_appointed_date_reference_merge,
    null_appointed_date_and_birth_reference_merge,
    married_merge,
    multirow_merge
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
        'data_id': 'unit-history_2014-2020_2020-10_p596580_ID',
        'input_profiles_file' : 'input/unit-history_2014-2020_2020-10_p596580_profiles.csv.gz',
        'add_cols' : ['BY_to_CA'],
        'custom_merges': [
            ['star', 'last_name_NS', 'appointed_date', 'birth_year', 'gender', 'race', 'current_unit'], #cindy to cynthia woodville, alex to alexander hotza
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'birth_year', 'gender', 'race'], # appointed date updates: john oleary, mark gentille, raul jaimes, deshawnna thigpen
            ['first_name_NS', 'middle_initial', 'birth_year', 'appointed_date'] #kristen steward to kendall, doesn't have a star so was not a married merge
        ],
        'input_remerge_file' : 'input/unit-history_2014-2020_2020-10_p596580.csv.gz',
        'output_remerge_file' : 'output/unit-history_2014-2020_2020-10_p596580.csv.gz',
        }
    

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file) 

    ref_df.appointed_date = pd.to_datetime(ref_df.appointed_date)
    sup_df.appointed_date = pd.to_datetime(sup_df.appointed_date)

    custom_merge = Merge(name='custom_merges', custom_merges=cons.custom_merges)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, null_flag_cols=['birth_year', 'appointed_date'], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2020)\
        .loop_merge(merges=[base_merge, 
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