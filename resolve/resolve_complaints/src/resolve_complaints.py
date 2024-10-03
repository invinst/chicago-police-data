#!usr/bin/env python3
#
# Author(s):    Roman Rivera / Ashwin Sharma (Invisible Institute)

'''resolve_complaints generation script
    complaint files are separated into date and address fields, then merged. 
'''

import pandas as pd
import numpy as np
from general_utils import keep_duplicates, remove_duplicates, resolve, combine_ordered_dfs, get_data_changes

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
        "complaint_files": [
            "input/complaints-complaints_1967-1999_2016-12.csv.gz",
            "input/complaints-complaints_2000-2016_2016-11.csv.gz",
            "input/complaints-complaints_2000-2018_2018-07.csv.gz",
            "input/complaints-complaints-cms_2016-2021_2021-09_p675956.csv.gz",
            "input/complaints-complaints-cms_2019-2021_2021-07.csv.gz",
            "input/complaints-complaints-crms_2016-2021_2021-09_p675956.csv.gz",
            "input/complaints-complaints-clear_2000-2020_2020-07.csv.gz",
            "input/complaints-complaints_2000-2023_2023-08_p053540.csv.gz",
            "input/complaints-complaints_2000-2023_2023-09_p876849.csv.gz"],

        "accused_files": [
            "input/complaints-accused_1967-1999_2016-12.csv.gz",
            "input/complaints-accused_2000-2016_2016-11.csv.gz",
            "input/complaints-accused_2000-2018_2018-07.csv.gz",
            "input/complaints-accused-cms_2016-2021_2021-09_p675956.csv.gz",
            "input/complaints-accused-cms_2019-2021_2021-07.csv.gz",
            "input/complaints-accused-crms_2016-2021_2021-09_p675956.csv.gz",
            "input/complaints-accused-clear_2000-2021_2021-07.csv.gz",
            "input/complaints-accused_2000-2023_2023-08_p053540.csv.gz",
            "input/complaints-accused_2000-2023_2023-09_p867849.csv.gz"],

        'date_cols' : ['incident_date', 'complaint_date', 'closed_date'],
        'address_cols': ['address',  'street_number', 'street_name', 'city', 'state', 'zip', 'apartment_number', 'location', 'beat', 'location_code'],

        'penalty_list' : [
                '000 - VIOLATION NOTED', 
                '001 - 1 DAY SUSPENSION',
                '002 - 2 DAY SUSPENSION',
                '003 - 3 DAY SUSPENSION',
                '004 - 4 DAY SUSPENSION',
                '005 - 5 DAY SUSPENSION',
                '006 - 6 DAY SUSPENSION',
                '007 - 7 DAY SUSPENSION',
                '008 - 8 DAY SUSPENSION',
                '009 - 9 DAY SUSPENSION',
                '010 - 10 DAY SUSPENSION', 
                '011 - 11 DAY SUSPENSION', 
                '012 - 12 DAY SUSPENSION', 
                '014 - 14 DAY SUSPENSION', 
                '015 - 15 DAY SUSPENSION',
                '020 - 20 DAY SUSPENSION',
                '021 - 21 DAY SUSPENSION',
                '025 - 25 DAY SUSPENSION',
                '027 - 27 DAY SUSPENSION',
                '028 - 28 DAY SUSPENSION', 
                '029 - 29 DAY SUSPENSION', 
                '030 - 30 DAY SUSPENSION',
                '100 - REPRIMAND',
                '200 - SUSPENDED OVER 30 DAYS', 
                '300 - ADMINISTRATIVE TERMINATION',
                '301 - ADMINISTRATIVELY CLOSED', 
                '400 - SEPARATION',
                '600 - NO ACTION TAKEN / NOT SUSTAINED / EXONERATED / UNFOUNDED',
                '601 - NO ACTION TAKEN / NO AFFIDAVIT',
                '800 - RESIGNED-NOT SERVED', 
                '900 - PENALTY NOT SERVED',
                '999 - DECEASED', 'SUSPENSION'],

        'outcome_fixes': {
            "Reinstated Court Act": "Reinstated By Court Action",
            "Reinstated Police Bd": "Reinstated By Police Board",
            "Suspen'D Indefinit'Y": "Suspended Indefinitely",
            "Admin. Termination": "Administrative Termination"
        },

        'not_discplined_outcomes': [
            'No Action Taken', 
            'No Action Taken / Not Sustained / Exonerated / Unfounded',
            'Unknown',
            'Administratively Closed',
            'No Action Taken / No Affidavit',
            'Penalty Not Served',
            'Not Served',
            'Violation Noted',
            'Resigned',
            'Deceased',
            'Sustained',
            'Reinstated By Court Action',
            'Reinstated By Police Board',
            ''
        ],

        'final_finding_map': {
            'ADMINISTRATIVELY CLOSED': "EX",
            'NO AFFIDAVIT': "NAF",
            "ADMINISTRATIVELY CLOSED": "AC",
            "ADMINISTRATIVELY TERMINATED": "EX",
            "NOT SUSTAINED": "NS",
            "UNFOUNDED": "UN",
            "SUSTAINED": "SU",
            "EXONERATED": "EX",
            "EXPUNGED": "EX",
        },
        'output_accused_file' : "output/complaints-accused.csv.gz",
        'output_complaints_file' : "output/complaints-complaints.csv.gz",
        }

    return setup.do_setup(script_path, args)

if __name__ == "__main__":

    cons, log = get_setup()

    complaint_dfs = [pd.read_csv(complaint_file, parse_dates=cons.date_cols) for complaint_file in cons.complaint_files]
    accused_dfs = [pd.read_csv(accused_file) for accused_file in cons.accused_files]

    # add cv for keeping track of which file is which in later merging/resolving
    for num, df in enumerate(complaint_dfs):
        df.insert(0, 'cv', num+1)
        df.insert(0, 'filename', cons.complaint_files[num].split("/")[-1].split(".csv.gz")[0])

        if df['cr_id'].dtype == np.dtype('float64'):
            df['cr_id'] = df['cr_id'].astype(int).astype(str)
        else:
            df['cr_id'] = df['cr_id'].astype(str)
        

        duplicates = df.duplicated().sum()

        if duplicates > 0:
            print(f"{cons.complaint_files[num]} has {duplicates} duplicates: dropping")

        df.drop_duplicates(inplace=True)
        df.dropna(subset=['cr_id'], inplace=True)
        df.set_index('cr_id', inplace=True)


    for num, df in enumerate(accused_dfs):
        df.insert(0, 'cv', num+1)
        df.insert(0, 'filename', cons.accused_files[num].split("/")[-1].split(".csv.gz")[0])

        if df['cr_id'].dtype == np.dtype('float64'):
            df['cr_id'] = df['cr_id'].astype(int).astype(str)
        else:
            df['cr_id'] = df['cr_id'].astype(str)
        df.set_index(['cr_id', 'UID'], inplace=True)

        # TODO: move this to individual, but for now, renaming recommended to recc to match original dataformat 
        # same with penalty to outcome
        df.columns = [col.replace('recommended', 'recc') for col in df.columns]
        df.columns = [col.replace('penalty', 'outcome') for col in df.columns]

    # remove expunged cases from clear file, cv 7 
    expunged_cases = complaint_dfs[6]['current_status'] == 'EXPUNGED'

    log.info(f"Removing {expunged_cases.sum()} expunged cases from complaints-complaints-clear_2000-2020_2020-07 file")
    complaint_dfs[6] = complaint_dfs[6][~expunged_cases]

    # raw concats
    all_complaints = pd.concat(complaint_dfs)
    all_accused = pd.concat(accused_dfs)

    log.info('Combining complaints')

    complaint_cr_ids = set(all_complaints.index.astype(str))

    # replace empty strings with nulls so that combine ordered dfs can replace nulls with non-nulls
    for df in complaint_dfs:
        df.replace('', np.nan, inplace=True)

    # go through complaint dfs from most recent to oldest, for each cr_id, prefer dates from newer data, replace null columns, add new rows for non-overlaping cr-ids
    complaints = combine_ordered_dfs(reversed(complaint_dfs)).reset_index()

    complaints['complainant_type'] = complaints['complainant_type'].str.upper().replace("CITIZEN", "CIVILIAN")

    # ensure no cr_ids were lost, that above combinations has the same rows as the raw concat
    assert (set(complaints.cr_id) == set(all_complaints.index))

    log.info('Combining accused')
    # go through accused from most recent to oldest, for each cr_id, prefer resolution/category from newer data, replace null columns, add new rows
    accused = combine_ordered_dfs(reversed(accused_dfs)).reset_index()
    accused_cr_ids = set(all_accused.reset_index()["cr_id"].astype(str))

    log.info('Finding and readding any missing accused')
    filtered_accused = accused.reset_index().merge(complaints.reset_index(), left_on="cr_id", right_on='cr_id')
    all_missing_crs = set(accused.cr_id) - set(filtered_accused.cr_id)

    # get per cr id
    missing_crs = [df[df.index.get_level_values('cr_id').isin(set(df.index.get_level_values('cr_id')) & all_missing_crs)].reset_index() for df in accused_dfs]

    # accused missing from complaints
    missing_accused = combine_ordered_dfs(missing_crs, keep_cols=['cv', 'cr_id'])

    missing_complaints = all_complaints.merge(missing_accused, on=['cv', 'cr_id']).drop_duplicates()
    # complaints = complaints.append(missing_complaints.loc[~missing_complaints.cr_id.isin(complaints.cr_id)])

    assert set(accused.cr_id.astype(str)) == accused_cr_ids
    assert set(complaints.cr_id.astype(str)) == complaint_cr_ids

    def join_unique(x):
        return ", ".join(x.astype(str).unique())

    # remerge in aggregates, just for easier comparison
    accused_aggs = all_accused.reset_index().sort_values('cv') \
        .groupby(['cr_id', "UID"], sort=False) \
        .agg(cvs=('cv', join_unique),
             filenames=('filename', join_unique),
             complaint_codes=('complaint_code', join_unique),
             complaint_categories=('complaint_category', join_unique),
        )
    accused = accused.merge(accused_aggs, how='left', left_on=['cr_id', "UID"], right_index=True)

    complaints_aggs = all_complaints.reset_index().sort_values('cv') \
        .groupby(['cr_id']) \
        .agg(cvs=('cv', join_unique), 
             investigating_agencies=('investigating_agency', join_unique),
             filenames=('filename', join_unique)
             )
    complaints = complaints.merge(complaints_aggs, how='left', left_on=['cr_id'], right_index=True) 

    accused_only = set(accused.cr_id.astype(str)).difference(set(complaints.cr_id.astype(str)))
    complaints_only = set(complaints.cr_id.astype(str)).difference(set(accused.cr_id.astype(str)))

    if accused_only:
        log.warning(f"Found {len(accused_only)} cr_ids in accused but not in complaints.")

    if complaints_only:
        log.warning(f"Found {len(complaints_only)} cr_ids in complaints but not in accused.")

    # TODO: move this to individual, for now formatting to keep some outcomes consistent
    accused['final_outcome'] = accused['final_outcome'].str.upper() \
        .str.replace('**', '') 
    
    penalty_code_map = {penalty.split(" - ")[0]:penalty for penalty in cons.penalty_list}
    accused['final_outcome'] = accused.assign(penalty_code=accused['final_outcome'].str.split("-").str[0].str.strip()) \
        ['penalty_code'].map(lambda x: penalty_code_map[x] if x in penalty_code_map else x)

    accused['final_finding'] = accused['final_finding'].map(lambda x: cons.final_finding_map[x] if x in cons.final_finding_map else x)

    accused['recc_outcome'] = accused['recc_outcome'].str.upper()

    # drop accused without a UID: either not officer ranks, or fully redacted
    log.warning(f"Dropping {accused['UID'].isnull().sum()} rows from accused for not having a UID")
    accused = accused[accused['UID'].notnull()]

    # TODO: move this to individuals, standardizing final_outcome 
    hyphenated_outcomes = accused['final_outcome'].str.contains("-")

    accused.loc[hyphenated_outcomes, 'final_outcome'] = accused.loc[hyphenated_outcomes, 'final_outcome'].str.split("-").str[-1].str.strip()

    accused['final_outcome'] = accused['final_outcome'].str.title()
    accused['recc_outcome'] = accused['recc_outcome'].str.title()

    # add days back to outcome string when outcome is just 'Suspension'
    suspension_day_mask = accused.days.notnull() & (accused.final_outcome == 'Suspension')
    accused.loc[suspension_day_mask, 'final_outcome'] = accused[suspension_day_mask]['days'].astype(int).astype(str) + ' Day Suspension'

    # TODO: move this to individual, standardizing some final outcome values
    accused['final_outcome'] = accused['final_outcome'].replace(cons.outcome_fixes)

    accused['disciplined'] = accused['final_outcome'].map(lambda x: x not in cons.not_discplined_outcomes)

    log.info('Writing complaints (%d CRs) and accused (%d CRs)' % (len(complaint_cr_ids), len(accused_cr_ids)))
    accused.to_csv(cons.output_accused_file, **cons.csv_opts)
    complaints.to_csv(cons.output_complaints_file, **cons.csv_opts)

    all_complaints.reset_index().to_csv("output/all_complaints.csv.gz", **cons.csv_opts)
    all_accused.reset_index().to_csv("output/all_accused.csv.gz", **cons.csv_opts)