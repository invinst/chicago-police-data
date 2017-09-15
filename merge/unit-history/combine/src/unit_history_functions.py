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


def history_to_panel(hist_df, frequency, max_date, min_date,
                     start_col='Start.Date', end_col='End.Date', unit_col='Unit', uid_col='UID'):
    
    hist_df.replace(start_col, pd.to_datetime(hist_df[start_col]), inplace=True) 
    hist_df.replace(end_col, pd.to_datetime(hist_df[end_col]), inplace=True) 
    
    if max_date:
        max_date = pd.to_datetime(max_date)
        assert isinstance(max_date, pd.Timestamp), 'max_date given is not a proper date'
    else:
        max_date = pd.to_datetime(max(hist_df[start_col]))

    hist_df = hist_df[hist_df[start_col] <= max_date]
    hist_df.loc[(hist_df[end_col].isnull()) |
              (hist_df[end_col] > max_date),
               end_col] = max_date

    if min_date:
        min_date = pd.to_datetime(min_date)
        assert isinstance(min_date, pd.Timestamp), 'min_date given is not a proper date'
        hist_df = hist_df[hist_df[end_col] >= min_date]

    hist_df = add_date_bounds(hist_df,
                              max_date=max_date, min_date=min_date,
                              start_col=start_col, end_col=end_col)
    
    freq_dict = {'Month':'MS', 'Day':'D', 'Year':'AS'}
    
    assert frequency in list(freq_dict.keys()), "freq must be 'Day', 'Month', or 'Year'"
    freq = freq_dict[frequency]
    
    hist_df[start_col] = to_start_date(hist_df[start_col], freq)
    hist_df[end_col] = to_start_date(hist_df[end_col], freq)
    
    easy_df = hist_df[hist_df[start_col] == hist_df[end_col]].copy()
    easy_df['Event'] = 3
    easy_df = easy_df[[start_col, 'Event', unit_col, uid_col]].drop_duplicates()
    easy_df.rename(columns={start_col: frequency}, inplace=True)
    
    hist_df = hist_df[hist_df[start_col] != hist_df[end_col]]
    print('Generating panel data. May take a few minutes.')
    start_time = time.time()
    panel_df = pd.concat([expand_times(r[start_col], r[end_col], r[unit_col], r[uid_col], freq)
                          for i,r in hist_df.iterrows()])
    end_time = time.time()
    print('Generating panel data took {} seconds.'.format(round(end_time-start_time, 2)))
    panel_df.rename(columns={'Date': frequency}, inplace=True)

    panel_df = panel_df.append(easy_df)
    
    if min_date:
        panel_df = panel_df[panel_df[frequency] >= min_date]
    
    return panel_df
