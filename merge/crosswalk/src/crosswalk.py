#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''crosswalk script: maps old UIDs to new UIDs by data ids, prefers old UIDs to keep links stable'''

import pandas as pd
import numpy as np
import __main__

from reference_data import ReferenceData
from default_merges import (
    base_merge, 
    null_appointed_date_supplemental_merge, 
    null_appointed_date_reference_merge,
    null_appointed_date_and_birth_supplemental_merge, 
    null_appointed_date_and_birth_reference_merge,
    multirow_merge, 
    married_merge,
    remove_duplicate_merges_filter,
    remove_sup_duplicate_merges_filter,
    remove_ref_duplicate_merges_filter,
    crosswalk_merge,
    relaxed_test_merge
)
from merge_data import Merge
from general_utils import keep_duplicates, remove_duplicates
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
        'old_reference_file': 'input/old-officer-reference.csv',
        'new_reference_file': 'input/officer-reference.csv.gz',
        'old_profile_file': 'input/final-profiles.csv',
        'old_officer_file': 'input/officer_table.csv',
        'universal_id': 'UID',
        'output_file': 'output/crosswalk.csv.gz',
        }

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    old_df = pd.read_csv(cons.old_reference_file)
    new_df = pd.read_csv(cons.new_reference_file)

    old_df = old_df.dropna(subset=['first_name'])

    # profiles is the result of the last pipeline final profiles step
    # officers is an export from the existing officer table
    # we need to map id from the officer table to UID in the old final profiles first
    profiles = pd.read_csv(cons.old_profile_file)
    officers = pd.read_csv(cons.old_officer_file, delimiter=";")

    max_officer_id = officers['id'].max()

    # transform profiles to match officer table
    profiles = profiles.dropna(subset='last_name')
    profiles['gender'] = profiles['gender'].str[0]
    profiles['race'] = profiles['race'].replace({"ASIAN/PACIFIC ISLANDER": "ASIAN/PACIFIC"})
    profiles['race'] = profiles['race'].fillna("UNKNOWN")
    profiles['middle_initial'] = profiles['middle_initial'].replace(" ", "")

    # make all string cols upper on officer table to match profiles (easier this direction)
    string_cols = ['first_name', 'last_name', 'rank', 'gender', 'race', 'resignation_date']
    for col in string_cols:
        officers[col] = officers[col].str.upper()

    merge_cols = [col for col in officers.columns if col not in ['id', 'rank']]
    officer_profile_merge = officers[["id"] + merge_cols].fillna("") \
        .merge(profiles[["UID"] + merge_cols].fillna(""), 
               on=merge_cols, 
               how='outer', 
               indicator=True) \
        .rename(columns={"id": "officer_id"})

    assert (officer_profile_merge['_merge'] == 'both').all(), "Merge not complete"
    assert officer_profile_merge[['officer_id', 'UID']].duplicated().sum() == 0, "Duplicates in merge"

    # apply map from profiles UID to officer id from db to old reference
    old_df = old_df.merge(officer_profile_merge[['officer_id', 'UID']], 
                          left_on='UID', 
                          right_on='UID', 
                          how='left',
                          indicator=True)

    # crosswalk: merge old UIDs to new UIDs using a combination of data id + foia name and officer demographic data
    # first, split out/melt _ID columns into data id and foia name columns
    id_columns = [col for col in old_df.columns if col.endswith("_ID")]
    assert old_df[id_columns].notnull().sum(axis=1).max() == 1

    cols = ['first_name_NS', 'last_name_NS', 'appointed_date', 'star', 'birth_year', 'middle_initial', 'race', 'gender']
    
    old_matches = old_df[id_columns + cols + ['UID', 'officer_id']] \
        .melt(id_vars=['UID', 'officer_id'] + cols, var_name='foia_name', value_name='data_id') \
        .dropna(subset=['data_id']) \
        .sort_values('UID') 

    new_id_columns = [col for col in new_df.columns if col.endswith("_ID")]
    assert new_df[new_id_columns].notnull().sum(axis=1).max() == 1


    new_matches = new_df[new_id_columns + cols + ['UID', 'matched_on', 'merge_name']] \
        .melt(id_vars=['UID', 'matched_on', 'merge_name'] + cols, var_name='foia_name', value_name='data_id') \
        .dropna(subset=['data_id']) \
        .sort_values('UID') \
        .rename(columns={"UID": "new_UID"})
    
    # fill middle inital, race, and gender with '' string in both old and new matches
    old_matches['middle_initial'] = old_matches['middle_initial'].fillna("")
    old_matches['race'] = old_matches['race'].fillna("")
    old_matches['gender'] = old_matches['gender'].fillna("")
    old_matches['star'] = old_matches['star'].fillna("")

    new_matches['middle_initial'] = new_matches['middle_initial'].fillna("")
    new_matches['race'] = new_matches['race'].fillna('')
    new_matches['gender'] = new_matches['gender'].fillna('')
    new_matches['star'] = new_matches['star'].fillna('')
    
    data_id_merge = Merge("FOIA Data ID Merge", 
                      custom_merges=[['data_id', "foia_name"] + cols],
                    #   merge_postprocess=remove_ref_duplicate_merges_filter, 
                      check_duplicates=False,
                      filter_reference_merges_flag=False
                      )

    col_merge = Merge("Column FOIA Merge",
                    custom_merges=[['foia_name'] + cols,
                                   cols,
                                   [col for col in cols if col != 'birth_year'],
                                   [col for col in cols if col not in ['star', 'birth_year']],
                                   ['star', 'first_name_NS', 'last_name_NS', 'appointed_date', 'middle_initial', 'gender'],
                                   ['star', 'first_name_NS', 'last_name_NS', 'birth_year']
                                   ],
                    # merge_postprocess=remove_ref_duplicate_merges_filter,
                    filter_reference_merges_flag=False,
                    check_duplicates=False)

    relaxed_test_merge.merge_dict['last_name'] = ['last_name_NS']

    null_appointed_date_reference_merge.filter_reference_merges_flag = False
    null_appointed_date_reference_merge.check_duplicates = False

    null_appointed_date_and_birth_reference_merge.filter_reference_merges_flag = False
    null_appointed_date_and_birth_reference_merge.check_duplicates = False

    null_appointed_date_supplemental_merge.filter_referernce_merges_flag = False
    null_appointed_date_supplemental_merge.check_duplicates = False

    null_appointed_date_and_birth_supplemental_merge.filter_reference_merges_flag = False
    null_appointed_date_and_birth_supplemental_merge.check_duplicates = False

    rd = ReferenceData(new_matches, id='new_UID', add_cols=[], null_flag_cols=['appointed_date', 'birth_year']) \
        .add_sup_data(old_matches, data_id='officer_id', add_cols=[], one_to_one=False) \
        .loop_merge([data_id_merge, col_merge, null_appointed_date_supplemental_merge, null_appointed_date_and_birth_supplemental_merge,
                      null_appointed_date_reference_merge, null_appointed_date_and_birth_reference_merge])


    # multiple profiles that were consolidated to one UID in the new data pipeline
    consolidated_merge_mask = rd.merged_df.duplicated("new_UID", keep=False)
    consolidated_merges = rd.merged_df[consolidated_merge_mask].sort_values(["new_UID", 'officer_id'])

    consolidated_urls = consolidated_merges \
        .rename(columns={"new_UID": "UID"}) \
        .sort_values(["UID", 'officer_id']) \
        .assign(url=lambda x: x['officer_id'].apply(lambda y: f"https://cpdp.co/officer/{y}"),
                url_index=lambda x: x.groupby("UID").cumcount() + 1) \
        .pivot(index='UID', columns="url_index", values='url') \
        .rename(columns=lambda x: f"url_{x}") \
        .reset_index() 

    def unique_concat(series):
        return ', '.join(series.replace("", np.nan).dropna().astype(str).unique())

    consolidated_profiles = consolidated_merges[['officer_id', "new_UID"]].drop_duplicates() \
        .merge(rd.supplemental.df, on='officer_id')[["new_UID"] + cols] \
        .rename(columns={"new_UID": "UID"}) \
        .groupby("UID").agg({col: unique_concat for col in cols}) \
        .reset_index()

    # output for testing
    consolidated_profiles.merge(consolidated_urls, on="UID") \
        .to_excel("consolidated_officers.xlsx", index=False)

    # remove consolidated merges first; keep just first officer id for each uid, add them back in
    rd.merged_df = rd.merged_df[~consolidated_merge_mask]
    consolidated_merges = consolidated_merges.groupby('new_UID', as_index=False).first()
    rd.merged_df = pd.concat([rd.merged_df, consolidated_merges])

    # split profiles that were split into multiple UIDs in the new data pipeline
    split_merge_mask = rd.merged_df.duplicated("officer_id", keep=False)
    split_merges = rd.merged_df[split_merge_mask].sort_values(["officer_id", 'new_UID'])

    # remove split merges first; null out officer id for every UID but the first, add first back in
    rd.merged_df = rd.merged_df[~split_merge_mask]
    split_merges.loc[split_merges.duplicated("officer_id", keep='first'), 'officer_id'] = np.nan
    rd.merged_df = pd.concat([rd.merged_df, split_merges.loc[split_merges['officer_id'].notnull()]])

    new_officers = rd.reference.unmerged[['new_UID']].drop_duplicates() 

    # add nulled split merges to new_officers
    new_officers = pd.concat([new_officers, 
                            split_merges[split_merges['officer_id'].isnull()][['new_UID']].drop_duplicates()]) \
                    .sort_values("new_UID").reset_index(drop=True)

    # assign new officer ids for new officers
    new_officers['officer_id'] = new_officers.index + max_officer_id + 1
    
    crosswalk = pd.concat([rd.merged_df, new_officers])

    assert (crosswalk \
        .merge(new_df[["UID"]].drop_duplicates(), 
                left_on='new_UID', 
                right_on='UID', 
                how='outer', 
                indicator=True) \
        ["_merge"] == 'both').all(), "Missing new UID"

    assert (crosswalk.groupby('new_UID')['officer_id'].nunique() > 1).sum() == 0, "Multiple officers ids for one UID"
    assert (crosswalk.groupby('officer_id')['new_UID'].nunique() > 1).sum() == 0, "Multiple UIDs for one officer id"

    assert crosswalk['new_UID'].isnull().sum() == 0, "Missing UID"
    assert crosswalk['officer_id'].isnull().sum() == 0, "Missing officer id"

    crosswalk.rename(columns={"new_UID": "UID"}, inplace=True)

    crosswalk.to_csv(cons.output_file, index=False)