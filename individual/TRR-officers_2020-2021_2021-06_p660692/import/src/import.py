#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''import script for TRR-officers_2020-2021_2021-06_p660692


NOTE: it looks like appointed_date may not be stable for the main trr sheet
They do however appear to be accurate for the star and title sheet

That means there are some manual fixes in here for appointed date

the associated notebok has some of the work, but basically:
- look at instances where appointed date from main sheet does not match back to star sheet
- merge those without appointed date, and manually check for accurate appointed date
- correct them here in the appointed_date_fixes list
'''
import pandas as pd
import __main__

from import_functions import standardize_columns, collect_metadata
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
        'input_file': 'input/17300-P660692-Trr-Jun2020-May2021_R (1).xlsx',
        'output_file': 'output/TRR-officers_2020-2021_2021-06_p660692.csv.gz',
        'metadata_file': 'output/metadata_TRR-officers_2020-2021_2021-06_p660692.csv.gz',
        'main_sheet': 'June 2020 - May 20, 2021',
        'star_sheet': 'Star',
        'main_keep_columns': [
            'UFE REPORT NO', "MEMBER FIRST NAME", "MEMBER MIDDLE INITIAL", "MEMBER LAST NAME",
             "MEMBERSEX", "MEMBERRACE", "member birth year", "UNIT", "Appointed Date",
             "MEMBERPOSITION", "MEMBERHEIGHT", "MEMBERWEIGHT", "INCIDENTDATETIME",
             'MEMBER ON DUTY', "MEMBER IN UNIFORM", "MEMBER INJURED"],
        'merge_cols': ['first_name', 'middle_initial', 'last_name', 'appointed_date'],
        'appointed_date_fixes': [
            {'first_name': 'DAVID', 'last_name': 'MARINEZ', 'appointed_date': '2010-09-10', 'new_appointed_date': '2010-09-01'},
            {'first_name': 'VICTOR', 'last_name': 'GONZALEZ', 'appointed_date': '2016-04-18', 'new_appointed_date': '2018-12-27'},
            {'first_name': 'KAILAH', 'last_name': 'SANDERS', 'appointed_date': '2018-09-21', 'new_appointed_date': '2018-09-27'},
            {'first_name': 'BRANDON', 'last_name': 'KIRBY', 'appointed_date': '2012-04-02', 'new_appointed_date': '2013-02-19'},
            {'first_name': 'LEON', 'last_name': 'COLEMAN', 'appointed_date': '2010-04-26', 'new_appointed_date': '2017-08-16'}
        ],
        'main_column_names_key': 'TRR-officers_2020-2021_2021-06_p660692',
        'star_column_names_key': 'TRR-star_2020-2021_2021-06_p660692'
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

# notes_df = pd.read_excel(cons.input_file, sheet_name=cons.notes_sheet,
#                          header=None)
# notes = '\n'.join(notes_df.loc[notes_df[0].isin([cons.main_sheet]),
#                               1].dropna())
# cons.write_yamlvar('Notes', notes)
# log.info('Notes written to cons: {}'.format(notes))

main_df = pd.read_excel(cons.input_file, sheet_name=cons.main_sheet)
main_df = main_df[cons.main_keep_columns]
log.info('{} columns selected from main sheet.'.format(cons.main_keep_columns))
main_df.columns = standardize_columns(main_df.columns, cons.main_column_names_key)

# for merging and accurate grouping, doesn't handle nulls well
main_df['appointed_date'] = pd.to_datetime(main_df['appointed_date'])
main_df['middle_initial'] = main_df['middle_initial'].fillna("")

# manual fixes for bad appointed date in main sheet
for fix in cons.appointed_date_fixes:
    main_df.loc[(main_df['first_name'] == fix['first_name']) 
                & (main_df['last_name'] == fix['last_name']) 
                & (main_df['appointed_date'] == fix['appointed_date']), 
                'appointed_date'] = fix['new_appointed_date']

star_df = pd.read_excel(cons.input_file, sheet_name=cons.star_sheet)
star_df.columns = standardize_columns(star_df.columns, cons.star_column_names_key)
star_df['middle_initial'] = star_df['middle_initial'].fillna("")

# get just most recent stars by merge cols, reassign nulls to max date to use idxmax/keep other columns after groupby more easily
max_date = star_df['end_date'].max() + pd.to_timedelta(1, 'day')
most_recent_star_idx = star_df \
    .assign(end_date_max=lambda x: x['end_date'].fillna(max_date)) \
    .groupby(cons.merge_cols)['end_date_max'].idxmax()
most_recent_star_df = star_df.loc[most_recent_star_idx].rename(columns={"star": "current_star"})

# reshape long to wide to get all other stars as columns labelled star1, star2, ...
all_stars = star_df.assign(star_count=star_df.groupby(cons.merge_cols)['star'].cumcount() + 1) \
    .assign(star_column=lambda x: "star" + x['star_count'].astype(str)) \
    .pivot(index=cons.merge_cols, columns='star_column', values='star') \
    .reset_index() 

# combine both to get most recent star and all stars as columns
star_df = most_recent_star_df.merge(all_stars, left_on=cons.merge_cols, right_on=cons.merge_cols)

df = main_df.merge(star_df, left_on=cons.merge_cols, right_on=cons.merge_cols, how='left')

df.insert(0, 'row_id', df.index+1)
df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)

