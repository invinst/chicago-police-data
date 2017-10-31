import pandas as pd
import __main__
import yaml
import itertools

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
        'universal_id': 'UID',
        'arg_dicts': [
            {
                'input_demo_file':  'input/cpd-employees_demographics.csv.gz',
                'input_full_file':  '',
                'intrafile_id': 'cpd-employees_ID',
                'args': {}
            },

            {
                'input_demo_file': 'input/all-sworn-units_demographics.csv.gz',
                'input_full_file': '',
                'intrafile_id':    'all-sworn-units_ID',
                'args': {
                           'no_match_cols':  ['last_name_NS', 'first_name_NS'],
                           'L4': True,
                           'custom_matches': [
                               ['L4FN', 'birth_year',
                                'gender', 'appointed_date'
                                'current_unit', 'race']
                                             ],
                           'merge_report':   True
                    },
            },

            {
                'input_demo_file':  'input/ase-units_demographics.csv.gz',
                'input_full_file':  '',
                'intrafile_id':     'ase-units_ID',
                'args': {
                            'no_match_cols':  ['last_name_NS', 'star1',
                                               'first_name_NS'],
                            'custom_matches': [
                                               ['first_name_NS', 'appointed_date',
                                                'race', 'gender', 'current_unit'],
                                               ['gender', 'star1', 'current_unit',
                                                'current_age_p1', 'appointed_date']
                                              ],
                        'current_age_from':   2016,
                        'drop_base_cols': ['star2', 'star3', 'star4',
                                           'star5', 'star6', 'star7',
                                           'star8', 'star9', 'star10'],
                        'merge_report':   True
                    }
        },

        {
            'input_demo_file':  'input/accused_demographics.csv.gz',
            'input_full_file':  '',
            'intrafile_id':     'accused_ID',
            'args': {
                        'no_match_cols': ['last_name_NS'],
                        'merge_report':  True
                    },
        },

        {
            'input_demo_file':  'input/investigators_demographics.csv.gz',
            'input_full_file':  '',
            'intrafile_id':     'investigators_ID',
            'args': {
                        'no_match_cols':    ['last_name_NS', 'current_star'],
                        'expand_stars':     True,
                        'min_match_length': 4,
                        'merge_report':     True
                    }
        },

        {
            'input_demo_file':  'input/trr-officers_demographics.csv.gz',
            'input_full_file':  '',
            'intrafile_id': 'trr-officers_ID',
            'args': {
                        'no_match_cols':    ['last_name_NS', 'current_star'],
                        'expand_stars':     True,
                        'custom_matches':   [
                                                ['appointed_date',
                                                 'star1', 'gender']
                                            ],
                        'merge_report':     True
                    }
        },

        {
            'input_demo_file':  'input/trr-statuses_demographics.csv.gz',
            'input_full_file':  '',
            'intrafile_id':     'trr-statuses_ID',
            'args': {
                        'no_match_cols':    ['last_name_NS', 'current_star'],
                        'expand_stars':     True,
                        'custom_matches':   [
                                                ['appointed_date',
                                                 'star1', 'gender']
                                            ],
                        'merge_report':     True
                    }
        },

        {
            'input_demo_file':  'input/awards_demographics.csv.gz',
            'input_full_file':  '',
            'intrafile_id':     'awards_ID',
            'args': {
                        'no_match_cols':    ['last_name_NS', 'current_star',
                                             'first_name_NS'],
                        'expand_stars':     True,
                        'custom_matches':   [
                                                ['appointed_date', 'gender',
                                                 'star1', 'birth_year',
                                                 'middle_initial']
                                            ],
                        'merge_report':     True
                    }
        }
            ]
    }

    return setup.do_setup(script_path, args)


cons, log = get_setup()
file_dict_lists = cons.arg_dicts
permutations = itertools.permutations(list(range(len(file_dict_lists))))
for permutation in permutations:
    print(permutation)
'''
    ref_df = pd.DataFrame()
    profile_df = pd.DataFrame()
    for arg_dict in permutation:
        ref_df, profile_df, full_df = \
            merge_process(arg_dict['input_demo_file'],
                          arg_dict['input_full_file'],
                          ref_df,
                          profile_df,
                          args_dict=arg_dict['args'],
                          log=log,
                          intrafile_id=arg_dict['file_id'],
                          uid=cons.universal_id)

profile_df.to_csv(cons.output_profile_file, **cons.csv_opts)
ref_df.to_csv(cons.output_reference_file, **cons.csv_opts)
'''
