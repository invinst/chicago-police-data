#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for unit history file(s)'''

import pandas as pd
import __main__

from merge_functions import merge_process
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
        'input_profile_file': 'input/officer-profiles.csv.gz',
        'input_reference_file': 'input/officer-reference.csv.gz',
        'arg_dicts': [
            {
                'input_demo_file': 'input/all-sworn-units_demographics.csv.gz',
                'input_full_file': 'input/all-sworn-units.csv.gz',
                'output_full_file': 'output/all-sworn-units.csv.gz',
                'intrafile_id': 'all-sworn-units_ID',
                'args': {'no_match_cols': ['last_name_NS', 'first_name_NS'],
                         'L4': True,
                         'custom_matches': [
                             ['L4FN', 'birth_year', 'race', 'gender',
                              'current_unit', 'appointed_date']
                         ],
                         'merge_report': True,
                         'print_merging': True}
            },
            {
                'input_demo_file': 'input/ase-units_demographics.csv.gz',
                'input_full_file': 'input/ase-units.csv.gz',
                'output_full_file': 'output/ase-units.csv.gz',
                'intrafile_id': 'ase-units_ID',
                'args': {
                    'no_match_cols': [
                        'last_name_NS', 'star1', 'first_name_NS'
                        ],
                    'custom_matches': [
                        ["first_name_NS", "appointed_date", "race",
                         "gender", "current_unit"],
                        ["appointed_date", "gender", "star1",
                         "current_age_p1", "current_unit"]
                        ],
                    'current_age_from': 2016,
                    'drop_base_cols': ['star' + str(i)
                                       for i in range(2, 11)],
                    'merge_report': True,
                    'print_merging': True}
            }
        ],
        'output_profile_file': 'output/officer-profiles.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID'
        }

    assert args['input_profile_file'] == 'input/officer-profiles.csv.gz',\
        'Input profiles file is not correct.'
    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_profile_file'] == 'output/officer-profiles.csv.gz',\
        'Output profile file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.read_csv(cons.input_reference_file)
profile_df = pd.read_csv(cons.input_profile_file)

for arg_dict in cons.arg_dicts:
    assert (arg_dict['input_full_file'].startswith('input/') and
            arg_dict['input_demo_file'].startswith('input/') and
            arg_dict['output_full_file'].startswith('output/')),\
        'A file does not start with the proper prefix'
    assert (arg_dict['input_full_file'].endswith('.csv.gz') and
            arg_dict['input_demo_file'].endswith('.csv.gz') and
            arg_dict['output_full_file'].endswith('.csv.gz')),\
        'A file does not end with .csv.gz'

    ref_df, profile_df, full_df = \
        merge_process(arg_dict['input_demo_file'],
                      arg_dict['input_full_file'],
                      ref_df,
                      profile_df,
                      args_dict=arg_dict['args'],
                      log=log,
                      intrafile_id=arg_dict['intrafile_id'],
                      uid=cons.universal_id)

    full_df.to_csv(arg_dict['output_full_file'], **cons.csv_opts)

profile_df.to_csv(cons.output_profile_file, **cons.csv_opts)
ref_df.to_csv(cons.output_reference_file, **cons.csv_opts)
