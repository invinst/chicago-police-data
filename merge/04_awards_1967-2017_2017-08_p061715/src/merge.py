#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute) / Ashwin Sharma (Invisible Institute)

'''merge script for 04_awards_1967-2017_2017-08_p061715'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import default_merges, whitelist_merge
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
        'data_id': 'awards_1967-2017_2017-08_ID',
        'from_year': 2017,
        'input_profiles_file' : 'input/awards_1967-2017_2017-08_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN', 'F2FN', 'current_age', 'BY_to_CA'],
        'null_flag_cols': ['birth_year', 'appointed_date'],
        'input_remerge_file' : 'input/awards_1967-2017_2017-08.csv.gz',
        'output_remerge_file' : 'output/awards_1967-2017_2017-08.csv.gz',
        'whitelist': 
            {'matched_on': 
                {0: 'star-first_name_NS-last_name_NS-birth_year-middle_initial-gender-race',
                3: 'star-last_name_NS-appointed_date-birth_year-middle_initial-gender-race',
                6: 'first_name_NS-last_name_NS-birth_year-middle_initial-gender-race'},
            'UID': {0: 130169, 3: 125969, 6: 132142},
            'awards_1967-2017_2017-08_ID': {0: 28349.0, 3: 24822.0, 6: 4207.0}}
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

    weaker_married_merge = Merge('married with middle name, not star',
                                 custom_merges=[['first_name_NS', 'middle_initial', 'appointed_date', 'birth_year', 'gender', 'race']])

    whitelist_df = pd.DataFrame(cons.whitelist)
    merges = default_merges + [weaker_married_merge, whitelist_merge(whitelist_df, cons.universal_id, cons.data_id)]

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, 
                       null_flag_cols=cons.null_flag_cols, log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=cons.from_year)\
        .loop_merge(merges=merges)\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)