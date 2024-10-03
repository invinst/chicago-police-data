#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma

'''resolve_awards generation script
'''

import pandas as pd
import numpy as np
import yaml
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
        "award_files": [
            "input/awards_1967-2017_2017-08.csv.gz",
            "input/awards_2017-2018_2018-08_p456632.csv.gz",
            "input/awards_2000-2020_2020-06_p583900.csv.gz"],
        "award_id_columns": { # columns to convert to just award id, match up between files
            "input/awards_1967-2017_2017-08.csv.gz": "pps_award_detail_id",
            "input/awards_2017-2018_2018-08_p456632.csv.gz": "award_id",
            "input/awards_2000-2020_2020-06_p583900.csv.gz": "tracking_no"
        },
        "award_columns": ["UID", "award_id", "award_type", 'current_award_status', 'award_start_date', 'award_request_date',
                          "award_end_date", "ceremony_date", "requester_full_name"],
        'output_file' : "output/awards.csv.gz",
        }

    return setup.do_setup(script_path, args)

cons, log = get_setup()

log.info("Reading in award files.")
award_dfs = [pd.read_csv(award_file) for award_file in cons.award_files ]

# add cv for keeping track of which row came from which file 
for num, df in enumerate(award_dfs):
    log.info(f"Loading award file {num+1} with {df.UID.nunique()} officers.")
    award_id_column = cons.award_id_columns[cons.award_files[num]]
    df.rename(columns={award_id_column: "award_id"}, inplace=True)
    df.insert(0, 'cv', num+1)
    df.set_index(["UID", "award_id"], inplace=True)

award_dfs[2] = award_dfs[2].rename(columns={"requester_name": "requester_full_name"})
log.info("Combining award files.")

# raw concat for comparison
all_awards = pd.concat(award_dfs).reset_index()

# read in police officer ranks again to validate that nothing is being lost in this step
# was previously used in merging or not merging, here will be dropping
with open("hand/award_po_ranks.yaml", "r") as f:
    po_ranks = yaml.safe_load(f)
with open("hand/maybe_po_ranks.yaml", "r") as f:
    maybe_po_ranks = yaml.safe_load(f)

all_awards['appointed_date'] = pd.to_datetime(all_awards['appointed_date'])

officer_rank_mask = all_awards['rank'].isin(po_ranks) | \
    (all_awards['rank'].isin(maybe_po_ranks) & (all_awards['appointed_date'] < '01-01-2010'))

officers = all_awards[officer_rank_mask].UID.unique()

# go through dfs from most recent to oldest, prefer newer data, keep one instace per UID, award_id
# also fills in nulls for columns
awards = combine_ordered_dfs(reversed(award_dfs)).reset_index()

log.info("Dropping non-police officer awards, logging ranks with counts.")
non_police_ranks = awards.loc[awards.UID.isnull(), 'rank'].value_counts()
log.info(f"Dropped {non_police_ranks.shape[0]} ranks, counts below")

awards = awards[awards.UID.notnull()]

# check for missing officers
missing_officers = np.setdiff1d(officers, awards.UID.unique())
# assert missing_officers.shape[0] == 0, f"Missing {missing_officers.shape[0]} officers"
# assert np.setdiff1d(awards.UID.unique(), officers).shape[0] == 0, "Gained officers?"

log.info("Exporting awards.")
awards[cons.award_columns].to_csv(cons.output_file, **cons.csv_opts)