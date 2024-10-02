#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 31_settlements_1952-2016_2017-01'''

import pandas as pd
import numpy as np
import __main__

from reference_data import ReferenceData
from default_merges import base_merge, remove_duplicate_merges_filter
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
        'data_id': 'settlements_1952-2016_2017-01_ID',
        'input_profiles_file' : 'input/settlements_1952-2016_2017-01_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN'],
        'input_remerge_file' : 'input/settlements_1952-2016_2017-01.csv.gz',
        'output_remerge_file' : 'output/settlements_1952-2016_2017-01.csv.gz'
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

    service_end = ref_df['resignation_date'].map(lambda x: "2018-01-01" 
                                                 if not isinstance(x, str)
                                                 else x)
    
    service_duration = (pd.to_datetime(service_end) - pd.to_datetime(ref_df['appointed_date'])).dt.days
    ref_df['service_years'] = service_duration.map(lambda x: x if not np.isfinite(x) else int(x/365.25))

    sup_df.rename(columns={'rank': 'current_rank'}, inplace=True)

    base_merge.filter_reference_merges_flag = False
    base_merge.check_duplicates = False
    base_merge.post_process = remove_duplicate_merges_filter

    base_merge.merge_dict = {**base_merge.merge_dict, 
                            'appointed_date': [''],
                            'star': ['star'],
                            'current_rank': ['current_rank', ''],
                            'service_years': ['service_years', '']}
    
    without_star_merge = Merge(name='without star merge',
                                 merge_dict={
                                     **base_merge.merge_dict,
                                    'appointed_date': [''],
                                    'star': ['']},
                                 check_duplicates=False,
                                 filter_reference_merges_flag=False,
                                 merge_postprocess=remove_duplicate_merges_filter)


    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, null_flag_cols=[], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2021)\
        .loop_merge(merges=[base_merge, without_star_merge])\
        .append_to_reference(keep_sup_um=False)\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
