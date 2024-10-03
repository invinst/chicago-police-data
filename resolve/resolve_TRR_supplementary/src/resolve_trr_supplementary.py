#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''resolve_TRR_supplementary generation script'''

import pandas as pd
import numpy as np
from general_utils import keep_duplicates, remove_duplicates, combine_ordered_dfs

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
        'trr_main_files': [
            'input/TRR-main_2004-2016_2016-09.csv.gz',
            'input/TRR-main_2004-2018_2018-08_p456008.csv.gz',
            'input/TRR-main_2017-2020_2020-06_p583646.csv.gz',
            'input/TRR-main_2020-2021_2021-06_p660692.csv.gz',
            'input/TRR-main_2021-2023_2023-10_p877988.csv.gz'],

        'trr_actions_responses_files': [
            'input/TRR-actions-responses_2004-2016_2016-09_p046360.csv.gz',
            'input/TRR-actions-responses_2004-2018_2018-08_p456008.csv.gz',
            'input/TRR-actions-responses_2017-2020_2020-06_p583646.csv.gz',
            'input/TRR-actions-responses_2017-2021_2021-06_p660692.csv.gz',
            'input/TRR-actions-responses_2021-2023_2023-10_p877988.csv.gz'
        ],

        "trr_status_files": [
            'input/TRR-statuses_2004-2016_2016-09.csv.gz',
            'input/TRR-statuses_2004-2018_2018-08_p456008.csv.gz',
            'input/TRR-statuses_2017-2020_2020-06_p583646.csv.gz',
            'input/TRR-statuses_2017-2021_2021-06_p660692.csv.gz',
            'input/TRR-statuses_2021-2023_2023-10_p877988.csv.gz'],

        "trr_subject_weapon_files": [
            'input/TRR-subject-weapons_2004-2016_2016-09.csv.gz',
            'input/TRR-subject-weapons_2004-2018_2018-08_p456008.csv.gz',
            'input/TRR-subject-weapons_2017-2020_2020-06_p583646.csv.gz',
            'input/TRR-subject-weapons_2017-2021_2021-06_p660692.csv.gz',
            'input/TRR-subject-weapons_2021-2023_2023-10_p877988.csv.gz'],

        "trr_subject_files":  [
            'input/TRR-subjects_2004-2016_2016-09.csv.gz',
            'input/TRR-subjects_2004-2018_2018-08_p456008.csv.gz',
            'input/TRR-subjects_2017-2020_2020-06_p583646.csv.gz',
            'input/TRR-subjects_2017-2021_2021-06_p660692.csv.gz',
            'input/TRR-subjects_2021-2023_2023-10_p877988.csv.gz'],

        'trr_weapon_discharge_files': [ 
            'input/TRR-weapon-discharges_2004-2016_2016-09.csv.gz',
            'input/TRR-weapon-discharges_2004-2018_2018-08_p456008.csv.gz',
            'input/TRR-weapon-discharges_2017-2020_2020-06_p583646.csv.gz',
            'input/TRR-weapon-discharges_2017-2021_2021-06_p660692.csv.gz',
            'input/TRR-weapon-discharges_2021-2023_2023-10_p877988.csv.gz'],
        }

    return setup.do_setup(script_path, args)

cons, log = get_setup()

def read_files(files):
    return [pd.read_csv(file).assign(cv=cv+1)
            for cv, file in enumerate(files)]

def read_supplemental_dfs(files, main_dfs, log, main_cols=['trr_id', 'cv', 'rd_no']):
    dfs = read_files(files)
    # for now, trrs are very nicely structured (1 main file for every supplemental) so using cv
    for idx, df in enumerate(dfs):
        main_df = main_dfs[idx]

        # drop rows that are null for all non main columns
        other_columns = df.columns[~df.columns.isin(main_cols + ['row_id'])]
        all_null_mask = df.replace("", np.nan) \
            [other_columns] \
            .isnull() \
            .all(axis=1)
        
        df = df[~all_null_mask]

        # only merge if necessary
        if "rd_no" not in df:
            dfs[idx] = merge_supplemental_df(df, main_df, log, main_cols)
        else:
            dfs[idx] = df

    return dfs

def merge_supplemental_df(df, main_df, log, main_cols):
    # filter main cols to what's actually in this main_col
    main_cols = [col for col in main_cols if col in main_df]

    df.trr_id = df.trr_id.astype(str)
    main_df.trr_id = main_df.trr_id.astype(str)
    df = df.merge(main_df[main_cols], on=['trr_id', 'cv'], how='left', indicator=True)

    left_only = df.query("_merge == 'left_only'") 
    if not left_only.empty:
        cv = df.cv.iloc[0]
        log.warning(f"Missing {left_only.shape[0]} supplemental trr_ids in main file {cv}. Dropping.")

    return df.query("_merge == 'both'").drop("_merge", axis=1).set_index("rd_no")

def add_missing_columns(df, cols):
    return df.reindex(columns=df.columns.to_list() + [col for col in cols if col not in df.reset_index()]).fillna("")

# main files are for merging to get rd_no as well as 
log.info("Reading in TRR main files")
main_dfs = read_files(cons.trr_main_files)

log.info("Loading in TRR actions responses")
actions_responses_dfs = read_supplemental_dfs(cons.trr_actions_responses_files, main_dfs, log)
actions_responses_id_columns = ['rd_no', 'person', 'action', 'other_description', 'resistance_type']
actions_responses_dfs = [add_missing_columns(df, actions_responses_id_columns) for df in actions_responses_dfs]
actions_responses = combine_ordered_dfs([actions_responses_df.reset_index().set_index(actions_responses_id_columns)
                                         for actions_responses_df in actions_responses_dfs]) \
    .reset_index()

# note that last status df/latest format doesn't actually store status
status_dfs = read_supplemental_dfs(cons.trr_status_files, main_dfs, log)
status_id_columns = ['rd_no', 'status', 'status_time', 'status_date']
statuses = combine_ordered_dfs([status_df.reset_index().set_index(status_id_columns) 
                                for status_df in status_dfs[:3]]) \
    .reset_index()

subject_weapon_dfs = read_supplemental_dfs(cons.trr_subject_weapon_files, main_dfs, log)
subject_weapon_id_columns = ['rd_no', 'weapon_type']
subject_weapons = combine_ordered_dfs([subject_weapon_df.reset_index().set_index(subject_weapon_id_columns)
                                      for subject_weapon_df in subject_weapon_dfs]) \
    .reset_index()

subject_dfs = read_supplemental_dfs(cons.trr_subject_files, main_dfs, log)
subject_dfs = [subject_df.assign(trr_id=lambda x: x['trr_id'].astype(str)) for subject_df in subject_dfs]
subject_id_columns = ['rd_no', 'subject_cb_no', 'gender', 'race', 'age', 'birth_year']
subject_dfs = [add_missing_columns(df, subject_id_columns) for df in subject_dfs]
subjects = combine_ordered_dfs([subject_df.reset_index().set_index(subject_id_columns)
                                for subject_df in subject_dfs]) \
    .reset_index()

weapon_discharge_dfs = read_supplemental_dfs(cons.trr_weapon_discharge_files, main_dfs, log)
weapon_dischage_id_columns = ['rd_no', 'trr_id', 'weapon_type']
weapon_discharges = combine_ordered_dfs([weapon_discharge_df.reset_index().set_index(weapon_dischage_id_columns)
                                          for weapon_discharge_df in weapon_discharge_dfs])  \
    .reset_index()
# weapon_discharges_old = combine_ordered_dfs([weapon_discharge_df.reset_index().set_index(weapon_dischage_id_columns)
#                                          for weapon_discharge_df in weapon_discharge_dfs[0:3]]) \
#     .reset_index()

# # need weaopn_type to combine, get it by using mappings from old data and filling in rest 
# # TODO: finish mapping weapon_type for what's missing (currently defaulting to pistol)
# weapon_discharges_new = weapon_discharge_dfs[3].reset_index()

# existing_firearm_types = weapon_discharges_old.reset_index() \
#     [['weapon_type', 'firearm_make', 'firearm_model']].drop_duplicates() \
#     .replace("", np.nan).dropna(how='any', axis=0)

# new_firearm_types = pd.DataFrame({
#     "weapon_type": ["RIFLE"],
#     "firearm_make": ["RUGER"],
#     "firearm_model": ["AR15"]
# }) 

# known_firearm_types = pd.concat([existing_firearm_types, new_firearm_types])

# firearm_mask = weapon_discharges_new.firearm_make.notnull()

# weapon_discharges_new.loc[firearm_mask, "weapon_type"] = weapon_discharges_new[firearm_mask] \
#     .merge(known_firearm_types, on=['firearm_make', 'firearm_model'], how='left')['weapon_type'] \
#     .fillna("SEMI-AUTO PISTOL")

# # TODO: look through codes, try and figure out if the different taser types map
# taser_probe_mask = weapon_discharges_new.taser_probe_dischrg_cd.notnull()
# weapon_discharges_new.loc[taser_probe_mask, "weapon_type"] = "TASER (PROBE DISCHARGE)"

# # drop everything else
# weapon_discharges_new = weapon_discharges_new[weapon_discharges_new.weapon_type.notnull()] \
#     .reset_index().drop("index", axis=1)

# weapon_discharges = combine_ordered_dfs([weapon_discharges_old.set_index(["rd_no", "weapon_type"]), weapon_discharges_new.set_index(["rd_no", "weapon_type"])]) \
#     .reset_index()

log.info("Outputting TRR supplemental files")
actions_responses.to_csv("output/trr_actions_responses.csv.gz", **cons.csv_opts)
statuses.to_csv("output/trr_statuses.csv.gz", **cons.csv_opts)
subject_weapons.to_csv("output/trr_subject_weapons.csv.gz", **cons.csv_opts)
subjects.to_csv("output/trr_subjects.csv.gz", **cons.csv_opts)
weapon_discharges.to_csv("output/trr_weapon_discharges.csv.gz", **cons.csv_opts)