#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''merge script for 13_complaints-CPD-witnesses_2000-2018_2018-03_18-060-157'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from merge_data import Merge
from default_merges import base_merge, multirow_merge_process
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
        'data_id': 'complaints-CPD-witnesses_2000-2018_2018-07_ID',
        'input_profiles_file' : 'input/complaints-CPD-witnesses_2000-2018_2018-07_profiles.csv.gz',
        'add_cols' : ["F4FN", "F4LN"],
        # 'custom_merges': [
        #     ['first_name_NS', 'last_name_NS', 'middle_intitial'],
        #     ['first_name_NS', 'last_name_NS', 'current_unit']
        # ],
        'input_remerge_file' : 'input/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz',
        'output_remerge_file' : 'output/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz',
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

    base_merge.merge_dict['birth_year'] = ['birth_year']

    # default multirow uses first name last name and appointed date, switch to birth year since no appointed date in file
    no_appointed_date_mutirow_merge_process = partial(multirow_merge_process, 
                                                      required_cols=['first_name_NS', 'last_name_NS', 'birth_year'])

    multirow_merge = Merge(name="multirow merges",
                        merge_dict={**base_merge.merge_dict, 
                                    'birth_year': ['birth_year', ''],
                                    'first_name': ['first_name_NS'],
                                    'last_name': ['last_name_NS']}, 
                        filter_reference_merges_flag=False,
                        filter_supplemental_merges_flag=False,
                        merge_postprocess=no_appointed_date_mutirow_merge_process)

    null_sup_birth_year_merge = Merge(name='null supplemental birth year merge', 
                                  merge_dict={**base_merge.merge_dict, 
                                              'birth_year': ['birth_year', 'current_age', '']},
                                              supplemental_preprocess="birth_year_null")

    null_ref_birth_year_merge = Merge(name='null reference birth year merge',
                                      merge_dict={**base_merge.merge_dict,
                                                  'birth_year': ['birth_year', '']},
                                                  reference_preprocess="birth_year_null")

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, 
                       null_flag_cols=['birth_year'], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2018)\
        .loop_merge(merges=[base_merge, null_ref_birth_year_merge, null_sup_birth_year_merge, multirow_merge])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
