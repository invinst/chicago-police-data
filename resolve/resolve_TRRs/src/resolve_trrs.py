#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute) / Ashwin Sharma

'''resolve_trrs generation script
    combines all trr main files
'''

import pandas as pd
import numpy as np
from general_utils import keep_duplicates, remove_duplicates, resolve, combine_ordered_dfs
from functools import reduce

import __main__
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
        "main_files": [
            "input/TRR-main_2004-2016_2016-09.csv.gz",
            "input/TRR-main_2004-2018_2018-08_p456008.csv.gz",
            "input/TRR-main_2017-2020_2020-06_p583646.csv.gz",
            "input/TRR-main_2020-2021_2021-06_p660692.csv.gz",
            "input/TRR-main_2021-2023_2023-10_p877988.csv.gz"],
        "officer_files": [
            "input/TRR-officers_2004-2016_2016-09.csv.gz",
            "input/TRR-officers_2004-2018_2018-08_p456008.csv.gz",
            "input/TRR-officers_2017-2020_2020-06_p583646.csv.gz",
            "input/TRR-officers_2020-2021_2021-06_p660692.csv.gz",
            "input/TRR-officers_2021-2023_2023-10_p877988.csv.gz"],
        'weapon_dicharges_file': 'input/trr_weapon_discharges.csv.gz',
        'actions_responses_file': 'input/trr_actions_responses.csv.gz',
        'subject_file': 'input/trr_subjects.csv.gz',
        'use_subject_cols': ['armed', 'injured', 'alleged_injury', 'age', 'birth_year', 'gender', 'race'],
        'use_officer_cols': ['assigned_beat', 'duty_status', 'in_uniform', 'injured', 'rank', 'unit', 'unit_detail'],
        'firearm_weapon_types': ['SEMI-AUTO PISTOL', 'REVOLVER', 'SHOTGUN', 'RIFLE', 'SEMI-AUTO PISTOL, TASER',
                                 'BATON / EXPANDABLE BATON, SEMI-AUTO PISTOL', 'RIFLE', 'REVOLVER', 'LESS LETHAL SHOTGUN'],
        'firearm_action_types': ['FIREARM', 'SEMI-AUTO PISTOL'],
        'output_main_file' : "output/trr_main.csv.gz",
        'output_officer_file' : "output/trr_officers.csv.gz",
        }

    return setup.do_setup(script_path, args)

cons, log = get_setup()

log.info("Reading in main and officer files.")
main_dfs = [pd.read_csv(main_file) for main_file in cons.main_files]
officer_dfs = [pd.read_csv(officer_file) for officer_file in cons.officer_files]

# add cv for keeping track of which row came from which file 
for num, df in enumerate(main_dfs):
    log.info(f"Loading main file {num+1} with {df.trr_id.nunique()} trr_ids and {df.rd_no.nunique()} rd_nos.")
    df.insert(0, 'cv', num+1)

    # add UID from officer id for recognizing interfile duplicates
    officer_df = officer_dfs[num]

    df = df.merge(officer_df[["UID", 'trr_id']], how='left', on=["trr_id"]) 

    if df.trr_id.dtype == 'int':
        df.trr_id = df.trr_id.astype(str)

    main_dfs[num] = df.drop_duplicates('trr_id').set_index(["trr_id"])

for num, df in enumerate(officer_dfs):
    log.info(f"Loading officer file {num+1} with {df.trr_id.nunique()} trr_ids and {df.UID.nunique()} officers.")
    df.insert(0, 'cv', num+1)
    if df.trr_id.dtype == 'int':
        df.trr_id = df.trr_id.astype(str)

    # add rd_no from main_df for recognizing interfile duplicates
    main_df = main_dfs[num]

    df = df.merge(main_df.reset_index()[["trr_id", "rd_no"]], how='left', on=['trr_id'])

    df = df.set_index(["trr_id"])

    officer_dfs[num] = df

# raw concats for later comparison
main_concat = pd.concat(main_dfs)
officer_concat = pd.concat(officer_dfs)

main_rd_nos = main_concat.reset_index().rd_no.unique()
officer_rd_nos = main_concat.reset_index().rd_no.unique()

log.info("Combining main and officer dfs for old format, preferring more recent data, filling in nulls")
main_df_old = combine_ordered_dfs(reversed(main_dfs[0:3])).reset_index()
officer_df_old = combine_ordered_dfs(reversed(officer_dfs[0:3])).reset_index()

log.info("Combining main and officer dfs for new format, preferring more recent data, filling in nulls")
main_df_new = combine_ordered_dfs(reversed(main_dfs[3:])).reset_index()
officer_df_new = combine_ordered_dfs(reversed(officer_dfs[3:])).reset_index()

# set back to null
main_df_new.replace("", np.nan, inplace=True)
main_df_old.replace("", np.nan, inplace=True)
officer_df_new.replace("", np.nan, inplace=True)
officer_df_old.replace("", np.nan, inplace=True)

log.info("Classifying trr_ids as showing up in new format, old format, or both")
main_merge = main_df_new[['rd_no', 'UID']].drop_duplicates().dropna() \
    .merge(main_df_old[['rd_no', 'UID']].drop_duplicates().dropna(), 
           on=['rd_no', "UID"], how='outer', indicator=True) \
    .rename(columns={'_merge': 'trr_format'}) \
    .assign(trr_format=lambda x: x['trr_format'].map({'both': 'both', 'left_only': 'new_only', 'right_only': 'old_only'}))

log.info("Preferring new trr format: use new trr data where available, otherwise use old trr data")
# split these out both to concat as well as for ease of manual checking
main_df_old_only = main_df_old.merge(main_merge.query("trr_format == 'old_only'"), on=['rd_no', 'UID'])
main_df_new_and_old = main_df_new.merge(main_merge.query("trr_format != 'old_only'"), on=['rd_no', 'UID'])

log.info("Add back in trr_ids that were missing from the UID merge (UID or rd_no is null)")
main_df_old_only = pd.concat([main_df_old_only, main_df_old.query("UID.isnull() or rd_no.isnull()")])
main_df_new_and_old = pd.concat([main_df_new_and_old, main_df_new.query("UID.isnull() or rd_no.isnull()")])

log.info("Getting corresponding officer data for new and old trr data respectively")
officer_df_old_only = officer_df_old.merge(main_df_old_only[['trr_id', 'trr_format']], on='trr_id')
officer_df_new_and_old = officer_df_new.merge(main_df_new_and_old[['trr_id', 'trr_format']], on='trr_id')

log.info("Combining old and new formats for main and officer data")
main_df = pd.concat([main_df_old_only, main_df_new_and_old])
officer_df = pd.concat([officer_df_old_only, officer_df_new_and_old])

log.info("Checking for duplicates on trr_id for main and trr_id and UID for officers.")
# assert pd.DataFrame(main_df[main_df.duplicated(['UID', 'rd_no'], keep=False)].groupby(['UID', 'rd_no']).cv.nunique()).query('cv > 1').shape[0] == 0
# assert pd.DataFrame(officer_df[officer_df.duplicated(['UID', 'rd_no'], keep=False)].groupby(['UID', 'rd_no']).cv.nunique()).query('cv > 1').shape[0] == 0

log.info("Checking for missing rd_no")
assert main_df.rd_no.isin(main_rd_nos).all()
assert officer_df.rd_no.isin(officer_rd_nos).all()

# some basic formatting of main file
main_df.trr_id = main_df.trr_id.astype(str)
main_df['trr_datetime'] = pd.to_datetime(main_df['trr_date'] + " " + main_df['trr_time'].replace("", "00:00:00"))

# adding in weapon related flags and counts
actions_responses = pd.read_csv(cons.actions_responses_file, dtype={'trr_id': str})
weapon_discharges = pd.read_csv(cons.weapon_dicharges_file, dtype={'trr_id': str})


# add taser indicator
action_taser_trr_ids = actions_responses[actions_responses.action.str.contains("TASER").fillna(False)] \
    .trr_id.unique()

weapon_taser_trr_ids = weapon_discharges[weapon_discharges.weapon_type.str.contains("TASER").fillna(False)] \
    .trr_id.unique()

taser_trr_ids = np.union1d(action_taser_trr_ids, weapon_taser_trr_ids)

main_df['taser'] = main_df.trr_id.map(lambda x: 1 if x in taser_trr_ids else 0)

# add firearm indicator
action_firearm_trr_ids = actions_responses[actions_responses.action.isin(cons.firearm_action_types).fillna(False)] \
    .trr_id.unique()
    
weapon_firearm_trr_ids = weapon_discharges[weapon_discharges.weapon_type.isin(cons.firearm_weapon_types).fillna(False)] \
    .trr_id.unique()

weapon_trr_ids = np.union1d(action_firearm_trr_ids, weapon_firearm_trr_ids)

main_df['firearm_used'] = main_df.trr_id.map(lambda x: 1 if x in weapon_trr_ids else 0)

# merge in number of shots
number_of_shots = weapon_discharges[['trr_id', 'total_number_of_shots']].groupby('trr_id', as_index=False).sum()
main_df = main_df.merge(number_of_shots, on='trr_id', how='left')

# adding in number of officers using a firearm
officers_using_firearm_count = main_df[['rd_no', 'firearm_used']].groupby('rd_no', as_index=False).sum() \
    .rename(columns={'firearm_used': 'number_of_officers_using_firearm'})

main_df = main_df.merge(officers_using_firearm_count, on='rd_no', how='left')

# adding in subjects
subjects = pd.read_csv(cons.subject_file)
subjects['trr_id'] = subjects['trr_id'].astype(str)

# add 'subject_id' as just a simple row id, not trying to deduplicate subjects as subject id is not used by site
subjects = subjects[['trr_id'] + cons.use_subject_cols] \
    .rename(columns={col: f"subject_{col}" for col in cons.use_subject_cols}) \
    .drop_duplicates(subset=['trr_id']) \
    .assign(subject_id=lambda x: np.arange(x.shape[0]))

main_df = main_df.merge(subjects, on='trr_id', how='left')

# adding in officer columns
officer_main_df = officer_df[['trr_id', 'UID'] + cons.use_officer_cols] \
    .rename(columns={col: f"officer_{col}" for col in cons.use_officer_cols}) \
    .rename(columns={'officer_duty_status': 'officer_on_duty', 'officer_unit': 'officer_unit_id',
                     'officer_unit_detail': 'officer_unit_detail_id'})

# format flags
officer_main_df['officer_on_duty'] = officer_main_df['officer_on_duty'].str[0].map(lambda x: 1 if x == 'Y' else 0)
officer_main_df['officer_injured'] = officer_main_df['officer_injured'].str[0].map(lambda x: 1 if x == 'Y' else 0)
officer_main_df['officer_in_uniform'] = officer_main_df['officer_in_uniform'].str[0].map(lambda x: 1 if x == 'Y' else 0)

main_df = main_df.merge(officer_main_df, on=['UID', 'trr_id'], how='left')

log.info(f"Writing main file  ({main_df['trr_id'].nunique()} trr_ids) and officer files ({officer_df['trr_id'].nunique()} trr_ids)")
main_df.to_csv(cons.output_main_file, **cons.csv_opts)
officer_df.to_csv(cons.output_officer_file, **cons.csv_opts)