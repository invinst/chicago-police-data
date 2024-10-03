#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute), Ashwin Sharma (Invisible Institute)

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
        'crosswalk_file': 'input/crosswalk.csv.gz',
        'output_file': 'output/final-profiles.csv.gz',
        'recode_file' : 'hand/rank_recode.yaml',
        'universal_id': 'UID',
        'column_order': [
            'first_name', 'last_name',
            'middle_initial', 'middle_initial2', 'suffix_name',
            'birth_year', 'race', 'gender',
            'appointed_date', 'resignation_date',
            'current_status', 'current_star',
            'current_unit', 'current_rank',
            'start_date', 'org_hire_date', 'foia_names', 'matches'
            ],
        'foia_type_order':  ['roster-inactive', 'roster', 'unit-history', 'TRR', 'complaints', 'awards', 'salary'],
        'aggregate_data_args' : {
            'current_cols': [
                'current_star', 'current_unit',
                'current_rank', 'current_status'
                ],
            'time_col' : 'foia_date',
            'mode_cols': [
                'middle_initial',
                'middle_initial2', 'suffix_name' 
                ],
            'max_cols': ['resignation_date'],
            'min_cols': ['appointed_date', 'start_date', 'org_hire_date'],
            'func_cols': {'foia_name': lambda x: ", ".join(list(set(x))),
                            'match': lambda x: ", ".join(list(set(x)))
            },
            'sorted_first_instance_cols': ['first_name', 'last_name', 'birth_year', 'race', 'gender']
            },
        'rename_columns': {
            "foia_name": "foia_names",
            "match": "matches"
            },
        'rank_subword_replacements': {
            "PO": "POLICE OFFICER",
            "P O": "POLICE OFFICER",
            "POL": "POLICE OFFICER",
            "ASSIGN AS": "ASSIGNED AS",
            "ASSGN": "ASSIGNED AS",
            "ASSG": "ASSIGNED AS",
            "ASGN": "ASSIGNED AS",
            "TRAFF": "TRAFFIC",
            "SEC": "SECURITY",
            "SPEC": "SPECIALIST",
            "SUPT": "SUPERINTENDENT",
            "EVID.": "EVIDENCE",
            "HANDL": "HANDLER",
            "ARB": "ARBITRATION",
            "OFF": "OFFICER",
            "OFF.": "OFFICER",
            "DIR OF": "DIRECTOR OF",
            "DIR": "DIRECTOR OF",
            "COOR": "COORDINATOR OF",
            "/INVESTIGATIONS": "INVESTIGATIONS"
        },
        "rank_fix_file": "hand/normalized_ranks.yaml",
        }

    assert args['input_file'] == 'input/officer-reference.csv.gz',\
        'Input file is not correct.'
    assert args['output_file'] == 'output/final-profiles.csv.gz',\
        'Output file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

if __name__ == "__main__":
    ref_df = pd.read_csv(cons.input_file)
    ref_df['current_rank'] = ref_df["current_rank"].replace("UNKNOWN", np.nan)

    crosswalk = pd.read_csv(cons.crosswalk_file)

    ref_df = ref_df.sort_values(["UID", "matched_on"], na_position='first')

    rd = ReferenceData(ref_df, uid=cons.universal_id, log=log) \
        .generate_foia_dates() \
        .final_profiles(foia_type_order=cons.foia_type_order,
                        aggregate_data_args=cons.aggregate_data_args,
                        column_order=cons.column_order,
                        rename_columns=cons.rename_columns,
                        include_IDs=True)\
    
    profiles = rd.profiles

    # apply crosswalk to get officer id (do it here in case profiles are dropped later)
    profiles = profiles.merge(crosswalk, on='UID', how='outer', indicator=True)
    assert (profiles["_merge"] == 'both').all(), "Crosswalk merge failed"
    profiles = profiles.drop(columns='_merge')

    # fix of current rank where rank code is given first TODO: move this to individual scripts
    rank_code_mask = (profiles['current_rank'].str[0] == '9')
    profiles.loc[rank_code_mask, 'current_rank'] = profiles['current_rank'].str.split(" - ").str[1]

    profiles['original_rank'] = profiles['current_rank']

    # rank fix just normalizes spelling of ranks, recode groups them more broadly
    with open(cons.rank_fix_file, "r") as f:
        rank_fix = yaml.load(f, Loader=yaml.SafeLoader)
    profiles['current_rank'] = profiles['current_rank'].replace(rank_fix)
    
    with open(cons.recode_file, "r") as f:
        rank_recode = yaml.load(f, Loader=yaml.SafeLoader)
    profiles['cleaned_rank'] = profiles['current_rank'].replace(rank_recode)

    missing_columns = [col for col in cons.column_order if col not in profiles.columns]
    assert not missing_columns, f"Missing columns {', '.join(missing_columns)}"

    # remove officers without a name
    profiles = profiles[profiles['first_name'].notnull() & profiles['last_name'].notnull()]

    # normalize current status to 0 and 1 
    profiles['current_status'] = profiles['current_status'].replace('True', 1).replace(True, 1).replace('N', 0).replace('0.0', 0)

    # revert current status to 0 if resignation date is present
    profiles.loc[(profiles['resignation_date'].notnull()) & (profiles['current_status'] != 0), 'current_status'] = 0

    # otherwise assume all cops are active 
    profiles['current_status'] = profiles['current_status'].fillna(1)

    # rollback status if above mandatory retirement age
    retirement_age_mask = (2024 - profiles['birth_year'] > 63) & (profiles['current_status'] == 1)

    profiles.loc[retirement_age_mask, 'current_status'] = 0

    profiles['current_rank'] = profiles['current_rank'].str.title()

    # fix roman numerals/other abbreviation title issues
    for old, fixed in {" Copa": " COPA", " Cpd": " CPD", "Pr ": "PR ",
                       'Ada-': "ADA-", " Swat": " SWAT",
                       " Iii": " III", " Ii": " II"," Iv": " IV"}.items():
        profiles['current_rank'] = profiles['current_rank'].str.replace(old, fixed)
    
    profiles['race'] = profiles['race'].str.title()
    profiles['gender'] = profiles['gender'].str[0]
    profiles.to_csv(cons.output_file, **cons.csv_opts)
