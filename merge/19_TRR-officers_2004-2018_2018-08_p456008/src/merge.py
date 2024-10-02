#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''merge script for 19_TRR-officers_2004-2018_2018-08_p456008'''


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
        'data_id': 'TRR-officers_2004-2018_2018-08_p456008_ID',
        'input_profiles_file' : 'input/TRR-officers_2004-2018_2018-08_p456008_profiles.csv.gz',
        'add_cols' : ['BY_to_CA', 'F4FN', 'L4LN', 'F1FN',
            # {"id": "UID",
            # "exec": "df.loc[df.current_age_m1.isnull() & df.current_age.notnull(), 'current_age_m1'] = df.loc[df.current_age_m1.isnull() & df.current_age.notnull(), 'current_age']"},
            # {"id": "UID",
            # "exec": "df.loc[df.current_age_p1.isnull() & df.current_age.notnull(), 'current_age_p1'] = df.loc[df.current_age_p1.isnull() & df.current_age.notnull(), 'current_age']"}
            ],
        'custom_merges' :[
            # 1 merge, scott dedore, changed appointed date likely due to promotion to lieutenant
            ['star', 'first_name_NS', 'last_name_NS', 'gender', 'race', 'current_unit'],
        ],
        'input_remerge_file' : 'input/TRR-officers_2004-2018_2018-08_p456008.csv.gz',
        'output_remerge_file' : 'output/TRR-officers_2004-2018_2018-08_p456008.csv.gz',
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

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, null_flag_cols=['appointed_date'], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2018)\
        .loop_merge(merges=[base_merge, 
                            null_appointed_date_reference_merge,
                            null_appointed_date_supplemental_merge,
                            multirow_merge,
                            married_merge,
                            custom_merge])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)

    
    