#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''clean script for complaints-accused_2000-2018_2018-03_18-060-157'''

import pandas as pd
import numpy as np
import __main__

from clean_functions import clean_data
from general_utils import keep_duplicates
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
        'input_file': 'input/complaints-accused_2000-2018_2018-03.csv.gz',
        'output_file': 'output/complaints-accused_2000-2018_2018-03.csv.gz'
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_csv(cons.input_file)
assert df['rank'].dropna().equals(df['rank2'].dropna())
log.info("Dropping redundant current_rank2 column")
df.drop("rank2", axis=1, inplace=True)
log.info("Creating complaint_category column from current_investigator_category + '-' + current_investigator_category_code")
df['complaint_category'] = df['current_investigator_category'] + '-' + df['current_investigator_category_code']
log.info("Creating final_finding_USE and final_outcome_USE columns")
df['FFN'] = df['final_finding_narrative'].str.contains('FINAL') == True
ffn_dict = {
    'FINAL- NO ACTION TAKEN' : 7,
    'FINAL- No Penalty' : 6,
    'FINAL- **PENALTY NOT SERVED' : 5,
    'FINAL' : 4,
    'FINAL- SUSPENDED OVER 30 DAYS' : 3,
    'FINAL- RESIGNED - NOT SERVED' : 2,
    'FINAL- REINSTATED BY POLICE BOARD' : 1
}
outcome_dict = {
    '0': 'Violation Noted',
    '100': 'Reprimand',
    '200': 'Suspended over 30 Days',
    '300': 'Administrative Termination',
    '400': 'Separation',
    '500': 'Reinstated by Police Board',
    '600': 'No Action Taken',
    '700': 'Reinstated by Court Action',
    '800': 'Resigned',
    '900': 'Penalty Not Served',
    '999': np.nan
}
df.loc[df['FFN'], 'FFNL'] = df.loc[df['FFN'], 'final_finding_narrative'].replace(ffn_dict)
df_FFN = df[
    ['accusation_id', 'cr_id', 'number_of_days', 'penalty_code',
    'final_finding', 'final_finding_narrative', 'FFN',
    'complaint_category', 'FFNL']
]\
    .query('FFN == True')\
    .drop_duplicates()\
    .sort_values(['cr_id', 'accusation_id', 'FFNL', 'penalty_code', 'number_of_days'])\
    .groupby(['cr_id', 'accusation_id'], as_index=False)\
    .first()
assert df_FFN.loc[df_FFN['penalty_code'].isnull(), 'number_of_days'].nunique() == 0
df_FFN['final_finding_USE'] = df_FFN['final_finding']

df_FFN.loc[(df_FFN['penalty_code'] == 'SUSPENSION'), 'final_outcome_USE'] = \
    df_FFN.loc[(df_FFN['penalty_code'] == 'SUSPENSION'), 'number_of_days'].map(lambda x: 'Suspension' if pd.isnull(x) else str(x) + ' Day Suspension')
loc_inds = df_FFN['number_of_days'].notnull().astype(str).isin(outcome_dict.keys())
df_FFN.loc[loc_inds, 'final_outcome_USE'] = df_FFN.loc[loc_inds, 'number_of_days'].astype(str).replace(outcome_dict)

df_FFN.loc[df_FFN['penalty_code'].notnull() & df_FFN['final_outcome_USE'].isnull(), 'final_outcome_USE'] = \
    df_FFN.loc[df_FFN['penalty_code'].notnull() & df_FFN['final_outcome_USE'].isnull(), 'penalty_code'].str.title()
df_FFN.loc[df_FFN.final_outcome_USE.isnull() &
           (df_FFN['final_finding']=='SUSTAINED') &
           (df_FFN['final_finding_narrative'].str.contains('No Penalty')),
           'final_outcome_USE'] = 'No Action Taken'
df_FFN.loc[df_FFN.final_outcome_USE.isnull() &
           (df_FFN['final_finding']=='SUSTAINED') &
           (df_FFN['final_finding_narrative'].str.contains('RESIGNED')),
           'final_outcome_USE'] = 'Resigned'

df_NFF = df.loc[~df.accusation_id.isin(df_FFN.accusation_id), ['accusation_id', 'cr_id', 'number_of_days', 'final_finding']].drop_duplicates()
assert keep_duplicates(df_NFF.drop('number_of_days', axis=1).drop_duplicates(), ['accusation_id', 'cr_id']).empty
df_NFF = df_NFF\
    .groupby(['accusation_id', 'cr_id', 'final_finding'], as_index=False)\
    .max()
df_NFF['final_finding_USE'] = df_NFF['final_finding']
loc_inds = df_NFF['number_of_days'].notnull().astype(str).isin(outcome_dict.keys())
df_NFF['final_outcome_USE'] = df_NFF['number_of_days'].map(lambda x: np.nan if pd.isnull(x) else str(x) + ' Day Suspension')
df_NFF.loc[loc_inds, 'final_outcome_USE'] = df_NFF.loc[loc_inds, 'number_of_days'].astype(str).replace(outcome_dict)

df_FO = df_NFF.append(df_FFN)[['accusation_id', 'cr_id', 'final_finding_USE', 'final_outcome_USE']]
df = df\
    .merge(df_FO, on=['accusation_id', 'cr_id'], how='left')\
    .drop(['FFN', 'FFNL'], axis=1)

df = clean_data(df, log)

df.to_csv(cons.output_file, **cons.csv_opts)
