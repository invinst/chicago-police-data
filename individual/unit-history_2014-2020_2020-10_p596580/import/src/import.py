#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''import script for unit-history_2014-2020_2020-10_p596580'''


import pandas as pd
import __main__

from import_functions import standardize_columns, collect_metadata
from assign_unique_ids_functions import get_most_recent_rows
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
        'input_file': 'input/15840-P596580-Stecklow-Rank-Star-Unit.xlsx',
        'output_file': 'output/unit-history_2014-2020_2020-10_p596580.csv.gz',
        'star_output_file': 'output/star-unit-history_2014-2020_2020-10_p596580.csv.gz',
        'metadata_file': 'output/metadata_unit-history_2014-2020_2020-10_p596580.csv.gz',
        'column_names_key': 'unit-history_2014-2020_2020-10_p596580',
        'sheet_name': 'Unit',
        'star_sheet_name': 'Star',
        "id_cols": ['first_name', 'middle_initial', 'last_name', 'appointed_date', 'birth_year', 'race', 'gender']
        }
    

    assert args['input_file'].startswith('input/'),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()


df = pd.read_excel(cons.input_file, sheet_name=cons.sheet_name)

df.columns = standardize_columns(df.columns, cons.column_names_key)
df.insert(0, 'row_id', df.index + 1)

# stars are in a separate execl sheet
# note that id cols should be stable between sheets as they are at the same point in time/data source
# so just merging directly on like columns, rather than doing a loop merge
star_df = pd.read_excel(cons.input_file, sheet_name=cons.star_sheet_name)

star_df.columns = standardize_columns(star_df.columns, cons.column_names_key)
star_df = star_df.rename(columns={"unit_start_date": "star_start_date", "unit_end_date": "star_end_date"})

# get a temporary unique id for each profile in the star df (set all columns to string and fill so that all id cols are filled/comparable)
star_df["star_id"] = star_df.astype(str).fillna("").groupby(cons.id_cols).ngroup()

# most recent_stars
current_stars = get_most_recent_rows(star_df, id_col='star_id', date_col="star_end_date") \
    .rename(columns={'star': "current_star"})

# number of stars per profile for pivoting
star_df['star_count'] = star_df.sort_values(['star_id', 'star_start_date']).groupby("star_id", sort=False)['star'].cumcount() + 1
star_pivot = star_df.pivot(index='star_id', columns='star_count', values='star') \
    .rename(columns=lambda x: f"star{x}")

star_df = current_stars.merge(star_pivot, left_on='star_id', right_index=True) \
    .drop('star_id', axis=1)

df = df.merge(star_df, left_on=cons.id_cols, right_on=cons.id_cols, how='left')

df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)

