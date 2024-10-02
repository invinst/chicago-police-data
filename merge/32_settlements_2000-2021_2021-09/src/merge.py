#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 32_settlements_2000-2021_2021-09'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from merge_data import Merge
from default_merges import base_merge, remove_duplicate_merges_filter, remove_sup_duplicate_merges_filter
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
        'data_id': 'settlements_2000-2021_2021-09_ID',
        'input_profiles_file' : 'input/settlements_2000-2021_2021-09_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN'],
        'input_remerge_file' : 'input/settlements_2000-2021_2021-09.csv.gz',
        'output_remerge_file' : 'output/settlements_2000-2021_2021-09.csv.gz'
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

    # base merge here are those already merged by the chicago reporter: 
    # they would have gotten appointed date off of older cpdp data so anything
    base_merge.name = "Chicago Reporter Tracked Merge"
    base_merge.filter_reference_merges_flag = False
    base_merge.check_duplicates = False
    base_merge.post_process = remove_sup_duplicate_merges_filter

    loose_merge = Merge("Loose Merge/Untracked Merge", 
                        merge_dict={"first_name_NS": ['first_name_NS', "F4FN"],
                                    "last_name_NS": ['last_name_NS', "F4LN"],
                                    'star': ['star', '']},
                        filter_reference_merges_flag=False,
                        check_duplicates=False,
                        merge_postprocess=remove_sup_duplicate_merges_filter)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, null_flag_cols=[], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, one_to_one=False, from_year=2021)\
        .loop_merge(merges=[base_merge, loose_merge])\
        .append_to_reference(keep_sup_um=False)\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
