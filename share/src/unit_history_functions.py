#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''functions used in unit history combination and panel data generation'''

import time
import pandas as pd
import numpy as np
from assign_unique_ids_functions import keep_duplicates,\
                                        remove_duplicates,\
                                        aggregate_data


def combine_histories(uh_list, resignation_df,
                      uid_col='UID', unit_col='Unit',
                      start_col='Start.Date', end_col='End.Date',
                      resignation_col='Resignation.Date'):
    '''returns pandas dataframe
       containing unique unit movements for individuals,
       and removing/filling non-sensical data
    '''
    # Initialize empty pandas dataframe
    uh_df = pd.DataFrame()
    # Loop over list of unit history data
    for df in uh_list:
        # Select relevant columns
        df = df.loc[:, [uid_col, unit_col, start_col, end_col]]
        # Drop any rows that are missing essential information
        df.dropna(subset=[unit_col, uid_col, start_col],
                  how='any', inplace=True)
        # Print out number of rows with non-sensical start/end date information
        # with existing end dates but end dates before start dates
        print(('{} rows with non-NA end date and end date '
               'before/equal to start date'
               '').format(df[(df[end_col].notnull()) &
                             (df[end_col] <= df[start_col])].shape[0]))
        # Replace non-sensical end dates with null values
        df.loc[(df[end_col].notnull()) &
               (df[end_col] <= df[start_col]),
               end_col] = np.nan
        # Append dataframe to full unit history dataframe
        uh_df = uh_df.append(df)

    # Remove duplicate information
    uh_df.drop_duplicates(inplace=True)
    # Give resignation date information to unit history
    uh_df = uh_df.merge(resignation_df,
                        on=uid_col, how='left')
    # Select indexes with non-null resignation date and null end date
    # and start dates before resignation dates
    indexes = ((uh_df[resignation_col].notnull()) &
               (uh_df[end_col].isnull()) &
               (uh_df[start_col] < uh_df[resignation_col]))
    # Replace these end dates with resignation dates
    uh_df.loc[indexes, end_col] = uh_df.loc[indexes, resignation_col]

    # Drop resignation date column
    uh_df.drop(resignation_col, axis=1, inplace=True)

    # Collect uid-start date-unit combinations that do not have duplicates
    uh_rd = remove_duplicates(uh_df, [uid_col, start_col, unit_col])
    # Collect uid-start date-unit combinations that do have duplicates
    uh_kd = keep_duplicates(uh_df, [uid_col, start_col, unit_col])
    # Then take the maximum of the end dates (usually a non-null over a null)
    uh_kd = aggregate_data(uh_kd,
                           uid=uid_col, id_cols=[start_col, unit_col],
                           max_cols=[end_col])
    # Ensure no information was discarded
    assert uh_rd.shape[0] + uh_kd.shape[0] ==\
        uh_df[[uid_col, unit_col, start_col]].drop_duplicates().shape[0],\
        'Data set lost information after split and aggregation.'
    # Append no-duplicates data with aggregated duplicated data
    # and reassign to unit history dataframe
    uh_df = uh_rd.append(uh_kd)
    # Sort dataframe values for output
    uh_df.sort_values([uid_col, start_col, unit_col], inplace=True)
    # Reset indexes
    uh_df.reset_index(drop=True, inplace=True)

    # Return combined unit history data
    return uh_df


def to_first_dates(dates, delta_type):
    '''returns array of dates
       all set to the beginning of the designated delta_type

       EX: to_first_date(series('2011-01-20'), 'MS') = array('2011-01')
    '''
    # Initializes delta type dictionary
    dtype_dict = {'MS': 'M', 'AS': 'Y', 'D': 'D'}
    # Change delta_type to the corrected value in dtype_dict
    delta_type = dtype_dict[delta_type]
    # Initialize delta as 1 unit of designated delta type
    delta = np.array([1], dtype='timedelta64[{}]'.format(delta_type))
    # Change dates to datetime objects with up to specified delta
    # Then add and subtract delta
    # resetting values to first date of delta type
    dates = dates.values.astype('datetime64[{}]'.format(delta_type))\
        - delta + delta

    # Return dates
    return dates


def expand_history(sd, ed, unit, uid, freq):
    '''returns pandas dataframe
       generates dates between start and end date at a given frequency
       and numbers the events, and includes unit and uid columns

       EX: expand_history('2000-01-01', '2000-01-03', 10, 1, 'MS')
           -> Date        Event Unit UID
              2000-01-01    1    10   1
              2000-01-02    0    10   1
              2000-01-03    2    10   1
    '''
    outdict = {'Date': pd.date_range(start=sd,
                                     end=ed,
                                     freq=freq)}
    event = [0] * len(outdict['Date'])
    event[0] = 1
    event[-1] = 2
    outdict['Event'] = event
    outdict['Unit'] = unit
    outdict['UID'] = uid
    return pd.DataFrame(outdict)


def history_to_panel(hist_df, frequency, max_date, min_date,
                     start_col='Start.Date', end_col='End.Date',
                     unit_col='Unit', uid_col='UID'):

    # Replace start and end date columns with pandas datetime values
    hist_df[start_col] = pd.to_datetime(hist_df[start_col])
    hist_df[end_col] = pd.to_datetime(hist_df[end_col])
    # If there is a max_date specified
    if max_date:
        # Convert max date to datetime
        max_date = pd.to_datetime(max_date)
    # If no max_date is given
    else:
        # Take max_date to be equal to the max start date
        max_date = hist_df[start_col].max()

    # Drop rows with unit start date after max date
    hist_df = hist_df[hist_df[start_col] <= max_date]
    # Replace null end dates or end dates after max date with max date
    hist_df.loc[(hist_df[end_col].isnull()) |
                (hist_df[end_col] > max_date),
                end_col] = max_date

    # If there is a min_date specified
    if min_date:
        # Convert min date to datetime then string
        min_date = pd.to_datetime(min_date)
        # Drop rows with unit end date before minimum date
        hist_df = hist_df[hist_df[end_col] >= min_date]

    # Initialize freq_dict
    freq_dict = {'Month': 'MS', 'Day': 'D', 'Year': 'AS'}
    # Ensure the given frequency is in the frequency dictionary keys
    assert frequency in list(freq_dict.keys()),\
        "freq must be 'Day', 'Month', or 'Year'"
    # Get freq for pandas date_range from freq_dict
    freq = freq_dict[frequency]

    # Replace start and end cols with the first date of the given frequency
    hist_df[start_col] = to_first_dates(hist_df[start_col], freq)
    hist_df[end_col] = to_first_dates(hist_df[end_col], freq)

    # Assign rows with equal start and end dates to equal_df
    equal_df = hist_df[hist_df[start_col] == hist_df[end_col]].copy()
    equal_df['Event'] = 3   # Create Event column equal to 3
    # Reorder columns to match those of expand_history output
    equal_df = equal_df[[start_col, 'Event', unit_col, uid_col]]
    equal_df.drop_duplicates(inplace=True)  # Drop duplicate rows
    # Rename start col to the frequency col to match expand_history output
    equal_df.rename(columns={start_col: frequency}, inplace=True)

    # Remove rows in hist_df in equal_df (start date not equal end date)
    hist_df = hist_df[hist_df[start_col] != hist_df[end_col]]
    # Print message indicating beginning of panel data generation
    print('Generating panel data. May take a few minutes.')
    start_time = time.time()    # Store start time of generation
    # Iterate over rows in hist_df
    # and applying expand_history function to row
    # then concatenate resulting list of pandas dataframe
    panel_df = pd.concat([expand_history(r[start_col], r[end_col],
                                         r[unit_col], r[uid_col],
                                         freq)
                          for i, r in hist_df.iterrows()])
    stop_time = time.time()  # Store stop time of generation
    # Print message indicating end of panel data generation and elapsed time
    print(('Generating panel data took {} seconds.'
           '').format(round(stop_time-start_time, 2)))
    # Rename Date column to be the given frequency
    panel_df.rename(columns={'Date': frequency}, inplace=True)
    # Append equal_df to end of generated panel data
    panel_df = panel_df.append(equal_df)

    # If a minimum date was specified
    if min_date:
        # Remove rows with dates before minimum date
        panel_df = panel_df[panel_df[frequency] >= min_date]

    # Return panel_df
    return panel_df
