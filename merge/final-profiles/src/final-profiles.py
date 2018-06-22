#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''final-profiles generation script'''

import pandas as pd
import numpy as np
import yaml
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
        'input_file': 'input/officer-reference.csv.gz',
        'output_file': 'output/final-profiles.csv.gz',
        'recode_file' : 'hand/rank_recode.yaml',
        'universal_id': 'UID',
        'column_order': [
            'first_name', 'last_name',
            'middle_initial', 'middle_initial2', 'suffix_name',
            'birth_year', 'race', 'gender',
            'appointed_date', 'resignation_date',
            'current_status', 'current_star',
            'current_unit', 'current_unit_description', 'current_rank',
            'start_date', 'org_hire_date'
            ],
        'aggregate_data_args' : {
            'current_cols': [
                'current_star', 'current_unit',
                'current_rank', 'current_status',
                ],
            'time_col' : 'foia_date',
            'mode_cols': [
                'first_name', 'last_name', 'middle_initial',
                'middle_initial2', 'suffix_name', 'race', 'gender',
                'birth_year', 'appointed_date', 'start_date', 'org_hire_date'
                ],
            'max_cols': ['resignation_date'],
            }
        }

    assert args['input_file'] == 'input/officer-reference.csv.gz',\
        'Input file is not correct.'
    assert args['output_file'] == 'output/final-profiles.csv.gz',\
        'Output file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.read_csv(cons.input_file)
ref_df['current_rank'] = ref_df["current_rank"].replace("UNKNOWN", np.nan)
profiles = \
ReferenceData(ref_df, uid=cons.universal_id, log=log)\
    .final_profiles(aggregate_data_args=cons.aggregate_data_args,
                    column_order=cons.column_order,
                    include_IDs=False)\
    .profiles

with open(cons.recode_file, "r") as f:
    rank_recode = yaml.load(f)
profiles['cleaned_rank'] = profiles['current_rank'].replace(rank_recode)
profiles.to_csv(cons.output_file, **cons.csv_opts)
