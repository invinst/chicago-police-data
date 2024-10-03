#!usr/bin/env python3
#
# Author(s):    Roman Rivera / Ashwin Sharma (Invisible Institute)

'''resolve_complaints-supplementary generation script'''

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
        "investigator_files": {
            'input/complaints-investigators_1967-1999_2016-12.csv.gz',
            'input/complaints-investigators_2000-2016_2016-11.csv.gz',
            'input/complaints-investigators_2000-2018_2018-07.csv.gz',
            'input/complaints-investigators-clear_2000-2019_2021-07_p21060299.csv.gz',
            'input/complaints-investigators-cms_2019-2021_2021-07_p21060299.csv.gz',
            'input/complaints-investigators-crms_2016-2021_2021-08_p675956.csv.gz',
            'input/complaints-investigators-cms_2019-2022_2022-09_p031093.csv.gz',
            'input/complaints-investigators_2000-2023_2023-08_p053540.csv.gz',
            'input/complaints-investigators_2000-2023_2023-09_p876849.csv.gz'},

        "investigator_columns": [
            'cr_id',
            'first_name', 'last_name', 'middle_initial', 'suffix_name',
            'appointed_date', 'current_star', 'current_rank', 'current_unit',  
            'investigating_agency', 'investigator_type', 'filename'],

        "investigator_columns_rename": {
            "agency": "investigating_agency",
            "star": "current_star",
            "unit": "current_unit"
        },

        "civilian_witnesses_files": [
            'input/complaints-civilian-witnesses_2000-2018_2018-03.csv.gz',
            'input/complaints-civilian-witnesses-clear_2000-2020_2021-07_p21060299.csv.gz',
            'input/complaints-civilian-witnesses-cms_2019-2021_2021-07_p21060299.csv.gz',
            'input/complaints-civilian-witnesses-cms_2019-2022_2022-09_p031093.csv.gz',
            'input/complaints-civilian-witnesses_2000-2023_2023-08_p053540.csv.gz',
            'input/complaints-civilian-witnesses_2000-2023_2023-09_p876849.csv.gz'],

        'civilian_witness_columns': [
            "cr_id", "birth_year", "current_age", "gender", "race", "filename"
        ],

        "cpd_witnesses_files": [
            'input/complaints-witnesses_2000-2016_2016-11.csv.gz',
            'input/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz',
            'input/complaints-CPD-witnesses-clear_2000-2020_2021-07_p21060299.csv.gz',
            'input/complaints-CPD-witnesses-cms_2019-2021_2021-07_p21060299.csv.gz',
            'input/complaints-CPD-witnesses-cms_2019-2022_2022-09_p031093.csv.gz',
            'input/complaints-CPD-witnesses_2000-2023_2023-08_p053540.csv.gz',
            'input/complaints-CPD-witnesses_2000-2023_2023-09_p876849.csv.gz'],

        'cpd_witness_columns': [
            'cr_id', 'appointed_date', 'assigned_unit', 'birth_year', 'current_unit', 'current_unit_detail',
            'first_name', 'last_name', 'middle_initial', 'race', 'rank', 'star', 'suffix_name', 'unit_detail', 'filename'
        ],

        "combined_witnesses_file": 'input/complaints-witnesses_1967-1999_2016-12.csv.gz',

        "complainant_files": [
            'input/complaints-complainants_1967-1999_2016-12.csv.gz',
            'input/complaints-complainants_2000-2016_2016-11.csv.gz',
            'input/complaints-complainants_2000-2018_2018-07.csv.gz',
            'input/complaints-complainants-clear_2000-2020_2021-07_p21060299.csv.gz',
            'input/complaints-complainants-cms_2019-2021_2021-07_p21060299.csv.gz',
            'input/complaints-complainants-cms_2019-2022_2022-09_p031093.csv.gz',
            'input/complaints-complainants_2000-2023_2023-08_p053540.csv.gz',
            'input/complaints-complainants_2000-2023_2023-09_p876849.csv.gz'],

        'complainant_columns': [
            'cr_id', 'age', 'birth_year', 'gender', 'race', 'iad_ops', 'party_type', 'party_subtype', 
            'filename'
        ],

        "victims_files": [
            'input/complaints-victims_2000-2016_2016-11.csv.gz',
            'input/complaints-victims_2000-2018-07.csv.gz',
            'input/complaints-victims-clear_2000-2019_2021-07_p21060299.csv.gz',
            'input/complaints-victims-cms_2019-2021_2021-07_p21060299.csv.gz',
            'input/complaints-victims-cms_2019-2022_2022-09_p031093.csv.gz',
            'input/complaints-victims_2000-2023_2023-08_p053540.csv.gz',
            'input/complaints-victims_2000-2023_2023-09_p876849.csv.gz'
        ],

        'victim_columns': [
            'cr_id', 'age', 'birth_year', 'gender', 'race', 'injury_condition', 'filename'
        ],

        "complaint_file": 'input/complaints-complaints.csv.gz',

        "complaint_filenames":    [
            "1967-1999_2016-12",
            "2000-2016_2016-11",
            "2000-2018_2018-07",
            "cms_2016-2021_2021-09_p675956",
            "cms_2019-2021_2021-07",
            "crms_2016-2021_2021-09_p675956",
            "clear_2000-2020_2020-07"]
        }

    return setup.do_setup(script_path, args)


cons, log = get_setup()

def read_dfs(files):
    dfs = []
    for idx, file in enumerate(files, start=1):
        filename = file.split("input/")[-1].split(".csv.gz")[0]
        df = pd.read_csv(file)
        if df['cr_id'].dtype == np.dtype('float64'):
            df['cr_id'] = df['cr_id'].astype(int).astype(str)
        else:
            df['cr_id'] = df['cr_id'].astype(str)
        df.set_index('cr_id', inplace=True)
        df = df.replace("", np.nan).dropna(how='all', axis=0)
        df['filename'] = filename
        df['cv'] = idx
        dfs.append(df)
    return dfs

def resolve_and_validate(dfs, file_category, complaints, log, keep_cols=[]):
    log.info(f"Resolving {file_category} files")
    resolved_df = combine_ordered_dfs(reversed(dfs)).reset_index()

    all_df = pd.concat(dfs)

    assert(set(all_df.reset_index()['cr_id'].unique()) == set(resolved_df.cr_id.dropna())), f"Missing cr_id for {file_category} after resolve"
    if not set(resolved_df.cr_id).issubset(complaints.cr_id.astype(str)): 
        f"cr_id in {file_category} not in main complaint"

    # all nulls
    non_id_cols = [col for col in resolved_df 
                   if (col != 'cr_id') and (col != 'filename') 
                   and (col != 'row_id') and 
                   not (col.endswith("_ID"))]

    all_null_mask = resolved_df.replace("", np.nan)[non_id_cols].isnull().all(axis=1)

    return resolved_df[~all_null_mask]

log.info("Reading in resolved complaint file for comparison")
complaints = pd.read_csv(cons.complaint_file)

log.info("Reading in investigator files")
investigator_dfs = read_dfs(cons.investigator_files)

for idx, df in enumerate(investigator_dfs):
    investigator_dfs[idx] = df.assign(null_count=df.isnull().sum(axis=1)) \
                                .reset_index() \
                                .sort_values('null_count') \
                                .drop_duplicates(['cr_id', 'first_name', 'last_name'], keep='first') \
                                .set_index(['cr_id', 'first_name', 'last_name']) \
                                .rename(columns=cons.investigator_columns_rename, errors='ignore') \
                                .drop('null_count', axis=1) 
investigators = resolve_and_validate(investigator_dfs, "investigator", complaints, log)

log.info("Reading in complainant files")
complainant_dfs = read_dfs(cons.complainant_files)
all_complainants = pd.concat(complainant_dfs)

for idx, df in enumerate(complainant_dfs):
    complainant_dfs[idx] = df.reset_index()[['cr_id', 'cv']].drop_duplicates().set_index('cr_id')

complainant_cr_ids = resolve_and_validate(complainant_dfs, "complainant", complaints, log)

complainants = all_complainants.merge(complainant_cr_ids.reset_index(), on=['cr_id', 'cv'])

complainants = complainants.dropna(subset=['gender', 'race', 'birth_year', 'age'], how='all')

log.info("Reading in separate civilian and cpd witness files")
civilian_witnesses_dfs = read_dfs(cons.civilian_witnesses_files)
cpd_witnesses_dfs = read_dfs(cons.cpd_witnesses_files)

log.info("Reading in combined witness file")
combined_witnesses_df = pd.read_csv(cons.combined_witnesses_file) \
    .assign(filename="complaints-witnesses_1967-1999_2016-12")

log.info("Splitting witness files, adding them to respective lists")
civilian_witnesses_df = combined_witnesses_df.query("officer_or_non == 'COMPLAINING_WITNESS'") \
    .set_index('cr_id') 
civilian_witnesses_dfs.insert(0, civilian_witnesses_df)
civilian_witnesses = resolve_and_validate(civilian_witnesses_dfs, "civilian witnesses", complaints, log)

cpd_witnesses_df = combined_witnesses_df.query("officer_or_non == 'OFFICER_WITNESS'") \
    .set_index('cr_id')
cpd_witnesses_df = cpd_witnesses_df.dropna(subset=['star']).drop(["row_id", "birth_year", "officer_or_non"], axis=1)
cpd_witnesses_dfs.insert(0, cpd_witnesses_df)
cpd_witnesses = resolve_and_validate(cpd_witnesses_dfs, "cpd witnesses", complaints, log)

log.info("Reading in victim files")
victims_dfs = read_dfs(cons.victims_files)
victims = resolve_and_validate(victims_dfs, "victims", complaints, log)

log.info("Outputting supplementary files")
investigators[cons.investigator_columns].drop_duplicates().to_csv("output/investigators.csv.gz", **cons.csv_opts)
complainants[cons.complainant_columns].drop_duplicates().to_csv("output/complainants.csv.gz", **cons.csv_opts)
civilian_witnesses[cons.civilian_witness_columns].drop_duplicates().to_csv("output/civilian_witnesses.csv.gz", **cons.csv_opts)
cpd_witnesses[cons.cpd_witness_columns].drop_duplicates().to_csv("output/cpd_witnesses.csv.gz", **cons.csv_opts)
victims[cons.victim_columns].drop_duplicates().to_csv("output/victims.csv.gz", **cons.csv_opts)