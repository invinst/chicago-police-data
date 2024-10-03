#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''resolve_salary generation script
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
        "salary_files": [
            "input/salary_2002-2017_2017-09.csv.gz",
            "input/salary_2017-2023_2023-02.csv.gz"],
        "salary_columns": ["UID", "pay_grade", "rank", "salary", "employee_status", "org_hire_date", 
                           "spp_date", "start_date", "year", "age_at_hire"],
        'output_file' : "output/salaries.csv.gz",
        }

    return setup.do_setup(script_path, args)

cons, log = get_setup()

log.info("Reading in salary files.")
salary_dfs = [pd.read_csv(salary_file) for salary_file in cons.salary_files]

# add cv for keeping track of which row came from which file 
for num, df in enumerate(salary_dfs):
    log.info(f"Loading salary file {num+1} with {df.UID.nunique()} officers.")
    df.insert(0, 'cv', num+1)
    # dropping salaries without UID or year
    df.dropna(subset=["UID", "year"], inplace=True)
    df.set_index(["UID", "year"], inplace=True)

# Replacing spp_date with appointed_date in second salary file
salary_dfs[1] = salary_dfs[1].drop("spp_date", axis=1).rename(columns={"appointed_date": "spp_date"})

log.info("Combining salary files.")

# raw concat for comparison
all_salaries = pd.concat(salary_dfs).reset_index()
officers = all_salaries.UID.unique()
officer_years = all_salaries[["UID", "year"]].drop_duplicates()

# go through dfs from most recent to oldest, prefer newer data, keep one instace per UID, year
salaries = combine_ordered_dfs(reversed(salary_dfs)).reset_index()

log.info("Checking for missing officers.")
missing_officers = np.setdiff1d(officers, salaries.UID.unique())
assert missing_officers.shape[0] == 0, f"Missing {missing_officers.shape[0]} officers"
assert np.setdiff1d(salaries.UID.unique(), officers).shape[0] == 0, "Gained officers?"

log.info("Checking for missing officer years.")
missing_officer_years = officer_years.merge(salaries, left_on=["UID", "year"], right_on=["UID", "year"], 
                                            how="left", indicator=True) \
                                    .query("_merge == 'left_only'").drop_duplicates()

assert missing_officer_years.shape[0] == 0, f"Missing {missing_officer_years.shape[0]} officer years"

log.info("Exporting salaries.")
salaries[cons.salary_columns].to_csv(cons.output_file, **cons.csv_opts)