#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''merge script for 02_unit-history__2016-12_p052262'''

import pandas as pd
import __main__

from merge_functions import ReferenceData
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
        'input_profiles_file' : 'input/unit-history__2016-12_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN', 'L4FN'],
        'loop_merge' : {
            'custom_merges' : [
             ['L4FN', 'birth_year', 'race', 'gender', 'current_unit', 'appointed_date'],
             ['race', 'gender', 'birth_year', 'appointed_date', 'last_name_NS'],
             ['first_name_NS', 'last_name_NS', 'birth_year', 'gender'],
             ['first_name_NS', 'last_name_NS', 'gender', 'race', 'current_unit']
            ],
            'verbose' : True,
            },
        'input_remerge_file' : 'input/unit-history__2016-12.csv.gz',
        'output_remerge_file' : 'output/unit-history__2016-12.csv.gz'
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.read_csv(cons.input_reference_file)
sup_df = pd.read_csv(cons.input_profiles_file)

ReferenceData(ref_df, uid=cons.universal_id, log=log)\
    .add_sup_data(sup_df, add_cols=cons.add_cols)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference()\
    .remerge_to_file(cons.input_remerge_file,
                    cons.output_remerge_file,
                    cons.csv_opts)\
    .write_reference(cons.output_reference_file, cons.csv_opts)
