#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute) / Ashwin Sharma (Invisible Institute)

'''fill missing Lieutenants and get rank/salary history for salary_2002-2017_2017-09'''

import pandas as pd
import __main__
import os
import yaml

import setup
from general_utils import keep_duplicates, remove_duplicates

def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'input_file': 'input/salaries.csv.gz',
        'input_profiles_file': 'input/final-profiles.csv.gz',
        'output_file' : 'output/salaries.csv.gz',
        'output_history_file' : 'output/salary_ranks.csv.gz',
        'rank_file' : 'hand/salary_rank_recode.yaml',
        'salary_ranks' : [
            'COMMANDER OF GENERAL SUPPORT DIVISION',
            'COMMANDER',
            'DEPUTY SUPERINTENDENT',
            'DEPUTY CHIEF',
            'CHIEF',
            'DIR OF MANAGEMENT/LABOR AFFAIRS',
            'FIRST DEPUTY SUPERINTENDENT',
            'DIR OF MOTOR AND ELECTRONICS MAINTENANCE',
            'ADMINISTRATIVE ASST - POLICE',
            'DIR OF HUMAN RESOURCES',
            "SUPERINTENDENT'S CHIEF OF STAFF",
            'ASSISTANT SUPERINTENDENT',
            'LIEUTENANT',
            'CAPTAIN'
            ],
        'profiles_ranks' : [
            'LIEUTENANT OF POLICE',
            'COMMANDER', 'CAPTAIN OF POLICE',
            'FIRST DEPUTY SUPT.',
            'DEP CHIEF',
            'ASST DEPUTY SUP',
            'DEPUTY SUPT.', 'CHIEF',
            'DIR PERSONNEL SERV',
            "SUPT'S CHIEF OF STAFF",
            'CMDR TAFFIC ENFORCE',
            'SUPT OF POLICE',
            'DIR LABOR RELATIONS',
            'DIR OF MOTOR MAINT',
            'COMM NEIGHBORHOOD RE',
            'DIST WATCH CO',
            'ASSISTANT SUPERINTENDENT',
            'DIRECTOR OF RECORDS',
            'COMMANDER SPEC SERV',
            'CHIEF SYS PROG IS'
            'DIR MGT/LAB AFFAIRS',
            'COOR /INVESTIGATIONS',
            'DIRECTOR OF CAPS',
            'LT',
            'CMDR'
            ]
        }

    assert args['input_file'].startswith('input/'),\
        'Input file is not correct.'
    assert args['output_file'].startswith('output/'),\
        'Output file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

### Helper functions
def find_gaps(x):
    return ((x['year'].max() - x['year'].min() + 1 != x['year'].nunique()) or
            (x['year'].max() < x['resignation_year'].iloc[0]))

def resolve_ranks(x):
    if x['spp_year'].count() == x.shape[0]:
        if set(x['spp_year']) & set(x['year']):
            return x[x['spp_year'] == x['year']].sort_values('spp_date', ascending=False).head(1)
        else:
            return x[x['spp_year'] != x['year']].sort_values('spp_date', ascending=False).head(1)
    elif all(x['spp_year'].isnull()) and x['pay_grade'].nunique() == 1 and x['salary'].nunique() == 1:
        return x.head(1)
    elif set(x['spp_year']) & set(x['year']):
        return x[x['spp_year'] == x['year']].sort_values('pay_grade', ascending=False).head(1)
    elif x['pay_grade'].nunique() == x['rank'].nunique() and x['salary'].nunique() == 1:
        return x.sort_values('pay_grade', ascending=False).head(1)
    else:
        # print(x)
        return x.sort_values("pay_grade", ascending=False).head(1)

### Fill Salary ranks with likely Lieutenants
df = pd.read_csv(cons.input_file)
fp = pd.read_csv(cons.input_profiles_file)
fp['resignation_year'] = pd.to_datetime(fp['resignation_date']).dt.year.fillna(2017)

# I think it was anyone who had been a sergeant
# and either had a gap or disappeared from the salary data before they should have
luids = df[df['rank'].isin(['SERGEANT', 'SERGEANT ( PER ARBTRN AGR)'])].UID
lt = df[df['UID'].isin(luids)]
rows = lt.shape[0]
lt = lt.merge(fp[['UID', 'resignation_year']], on='UID', how='left')

bks = lt[['UID', 'year', 'resignation_year']]\
    .groupby('UID')\
    .apply(find_gaps)\
    .reset_index()\
    .rename(columns={0:'gap'})
lt = lt.merge(bks, on='UID', how='inner')
assert rows == lt.shape[0]

hr = fp[fp['current_rank'].isin(cons.profiles_ranks)]

# and then who also appears with a final/current rank of lieutenant or higher
# (captain, commander, deputy chief, chief,
# any kind of superintendent/assistant superintedent)
lt = lt[(lt['rank'].isin(cons.salary_ranks)) | (lt.UID.isin(hr.UID))]
lt = lt.merge(hr[['UID', 'current_rank']], on='UID',how='left')

end_list = []
for uid in lt.UID.unique():
    g = lt[lt['UID']==uid]
    gaps = (set(range(g['year'].min(),
                     int(max(g['year'].max(),
                         g['resignation_year'].max())+1)))
            - set(g['year']))
    end_list.append(pd.DataFrame({'UID' : [uid]*len(gaps),
                  'year' : list(gaps),
                  'rank' : ['LIEUTENANT'] * len(gaps),
                  'gen' : [1] * len(gaps)}))
end_df = pd.concat(end_list)
df = df.append(end_df)

lt_luids = set(lt.UID)
df['gap'] = df['UID'].map(lambda x: 1 if x in lt_luids else 0)
# df.drop('row_id', axis=1).to_csv(cons.output_file, **cons.csv_opts)
# df.to_csv(cons.output_file, **cons.csv_opts)

### Create a salary-rank history with 1 row for UID/Year/Rank/Salary
df = df.drop_duplicates(subset=['UID', 'pay_grade', 'rank', 'salary', 'year', 'spp_date'])

df['spp_date'] = pd.to_datetime(df['spp_date'])
df['spp_year'] = df['spp_date'].dt.year
df['pay_grade'].fillna('', inplace=True)

# apply resolve ranks to duplicate UID, year rows, add them back to non duplicates
duplicated = df[df.duplicated(subset=['UID', 'year'], keep=False)] \
    .groupby(["UID", "year"], as_index=False) \
    .apply(resolve_ranks)

df = pd.concat([duplicated, df[~df.duplicated(subset=['UID', 'year'], keep=False)]])\
    .sort_values(['UID', 'year', 'spp_date'], ascending=False)

# df = keep_duplicates(df, ['UID', 'year'])\
#     .groupby(['UID', 'year'], as_index=False)\
#     .apply(resolve_ranks)\
#     .reset_index(drop=True)\
#     .append(remove_duplicates(df, ['UID', 'year']))\
#     .reset_index(drop=True)\
#     .drop('spp_year', axis=1)

rank_dict = yaml.safe_load(open(cons.rank_file))
df = df.rename(columns={"rank": "uncleaned_rank"})
df['rank'] = df['uncleaned_rank'].replace(rank_dict)
assert keep_duplicates(df, ['UID', 'year']).empty
df.to_csv(cons.output_file, **cons.csv_opts)
