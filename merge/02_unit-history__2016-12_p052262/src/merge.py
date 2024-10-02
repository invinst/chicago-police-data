#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute) / Ashwin Sharma (Invisible Institute)

'''merge script for 02_unit-history__2016-12_p052262'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import default_merges, whitelist_merge
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
        'data_id': 'unit-history__2016-12_ID',
        'input_profiles_file' : 'input/unit-history__2016-12_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN', 'L4FN', "BY_to_CA"],
        'null_flag_cols': ['appointed_date', 'birth_year'],
        'whitelist': {
            'matched_on': 
                {0: 'first_name_NS-last_name_NS-birth_year-middle_initial-gender-race-current_unit',
                1: 'first_name_NS-last_name_NS-gender-race-current_unit',
                2: 'L4FN-birth_year-race-gender-current_unit-appointed_date',
                3: 'last_name_NS-appointed_date-birth_year-middle_initial-gender-race',
                4: 'last_name_NS-appointed_date-birth_year-gender-race-current_unit'},
            'UID': 
                {0: 100808, 
                 1: 116595, 
                 2: 121423,
                 3: 111109,
                 4: 104139},
            'unit-history__2016-12_ID': 
                {0: 798.0, 
                 1: 16688.0,
                 2: 7219.0, 
                 3: 11101.0,
                 4: 19573.0}
        },
        'input_remerge_file' : 'input/unit-history__2016-12.csv.gz',
        'output_remerge_file' : 'output/unit-history__2016-12.csv.gz'
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

    # 2 manual values: amy petrouski had a appointed date update 
    # jacqueline mizera = jackie mizera
    whitelist_df = pd.DataFrame(cons.whitelist)
    merges = default_merges + [whitelist_merge(whitelist_df, cons.universal_id, cons.data_id)]

    rd = ReferenceData(ref_df, id=cons.universal_id,add_cols=cons.add_cols, 
                       null_flag_cols=cons.null_flag_cols, 
                       log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols)\
        .loop_merge(merges=merges)\
        .append_to_reference() \
        .remerge_to_file(cons.input_remerge_file,
                         cons.output_remerge_file,
                         cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)