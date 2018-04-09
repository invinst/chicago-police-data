#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''functions used in unit history combination and panel data generation'''

import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from general_utils import keep_duplicates, remove_duplicates

def combine_histories(uh_list, resignation_df, log,
                      uid='UID', unit='unit',
                      start='unit_start_date',
                      end='unit_end_date',
                      resignation_col='resignation_date'):
    """Combines multiple unit history dataframes into one
       containing unique unit movements for individuals,
       removing non-sensical data and filling missing data

    Parameters
    ----------
    uh_list : list
        List of unit history pandas DataFrame
    resignation_df : pandas DataFrame
        Contains data on resignation dates
    log : logging object
    uid : str
        Name of unique ID column in unit history and resignation date DataFrames
    unit : str
        Name of unit column in unit history DataFrames in uh_list
    start : str
        Name of unit start date column in unit history DataFrames in uh_list
    end : str
        Name of unit end date column in unit history DataFrames in uh_list
    resignation_col : str
        Name of resignation date column in resignation_df DataFrames

    Returns
    -------
    uh_df : pandas DataFrame
    """
    from assign_unique_ids_functions import aggregate_data

    uh_df = pd.DataFrame()

    for df in uh_list:
        df = df.loc[:, [uid, unit, start, end]]
        df.dropna(subset=[unit, uid, start],
                  how='any', inplace=True)
        log.info(('%d rows with non-NA end date and end date '
                  'before/equal to start date'
                  ''), df[(df[end].notnull()) &
                          (df[end] <= df[start])].shape[0])
        df.loc[(df[end].notnull()) &
               (df[end] <= df[start]),
               end] = np.nan
        uh_df = uh_df.append(df)

    uh_df.drop_duplicates(inplace=True)
    uh_df = uh_df.merge(resignation_df,
                        on=uid, how='left')
    indexes = ((uh_df[resignation_col].notnull()) &
               (uh_df[end].isnull()) &
               (uh_df[start] < uh_df[resignation_col]))
    uh_df.loc[indexes, end] = uh_df.loc[indexes, resignation_col]

    uh_df.drop(resignation_col, axis=1, inplace=True)

    uh_rd = remove_duplicates(uh_df, [uid, start, unit])
    uh_kd = keep_duplicates(uh_df, [uid, start, unit])
    uh_kd = aggregate_data(uh_kd,
                           uid=uid, id_cols=[start, unit],
                           max_cols=[end])

    assert uh_rd.shape[0] + uh_kd.shape[0] ==\
        uh_df[[uid, unit, start]].drop_duplicates().shape[0],\
        'Data set lost information after split and aggregation.'

    uh_df = uh_rd.append(uh_kd)
    uh_df.sort_values([uid, start, unit], inplace=True)
    uh_df.reset_index(drop=True, inplace=True)

    return uh_df


def to_first_dates(dates, delta_type):
    """Sets all dates to the beginning of the designated delta_type

    Parameters
    ----------
    dates : pandas Series
        List of unit history pandas DataFrame
    delta_type : str
        must be 'MS' for month, 'AS' for year, or 'D' for day

    Returns
    -------
    dates : numpy array
    """
    dtype_dict = {'MS': 'M', 'AS': 'Y', 'D': 'D'}
    delta_type = dtype_dict[delta_type]
    delta = np.array([1], dtype='timedelta64[{}]'.format(delta_type))
    # Change dates to datetime objects with up to specified delta
    # Then add and subtract delta
    # resetting values to first date of delta type
    dates = dates.values.astype('datetime64[{}]'.format(delta_type))\
            - delta + delta

    return dates


def expand_history(sd, ed, unit, uid, freq):
    """Generates dates between start and end date at a given frequency
       and numbers the events, and includes unit and uid columns

    Parameters
    ----------
    sd : str
        String of start date (must be suitable for pd.date_range start)
    ed : str
        String of end date (must be suitable for pd.date_range end)
    unit : str/float/int/etc.
    uid : str/float/int/etc.
    freq : str
        String for interval frequency (must be suitable for pd.date_range freq)

    Returns
    -------
     : pandas DataFrame
    """
    outdict = {'date': pd.date_range(start=sd,
                                     end=ed,
                                     freq=freq)}
    event = [0] * len(outdict['date'])
    event[0] = 1
    event[-1] = 2
    outdict['event'] = event
    outdict['unit'] = unit
    outdict['UID'] = uid
    return pd.DataFrame(outdict)


def history_to_panel(hist_df, frequency, max_date, min_date, log,
                     start='unit_start_date',
                     end='unit_end_date',
                     unit='unit', uid='UID'):
    """Converts historical information (wide) to panel data (long).

    Parameters
    ----------
    hist_df : pandas DataFrame
    frequency : str
        String for interval frequency of panel data ('month', 'day', or 'year')
    max_date : str
        String in pandas datetime acceptable form, upper bound for panel data
        Can be empty string
    min_date : str
        String in pandas datetime acceptable form, lower bound for panel data
        Can be empty string
    log : logging object
    start : str
        Name of unit start date column in unit history DataFrames in uh_list
    end : str
        Name of unit end date column in unit history DataFrames in uh_list
    uid : str
        Name of unique ID column in unit history and resignation date DataFrames
    unit : str
        Name of unit column in unit history DataFrames in uh_list

    Returns
    -------
    panel_df : pandas DataFrame
    """
    hist_df[start] = pd.to_datetime(hist_df[start])
    hist_df[end] = pd.to_datetime(hist_df[end])
    if max_date:
        max_date = pd.to_datetime(max_date)
    else:
        max_date = hist_df[start].max()

    hist_df = hist_df[hist_df[start] <= max_date]
    hist_df.loc[(hist_df[end].isnull()) |
                (hist_df[end] > max_date),
                end] = max_date

    if min_date:
        min_date = pd.to_datetime(min_date)
        hist_df = hist_df[hist_df[end] >= min_date]

    freq_dict = {'month': 'MS', 'day': 'D', 'year': 'AS'}
    assert frequency in list(freq_dict.keys()),\
        "freq must be 'day', 'month', or 'year'"
    freq = freq_dict[frequency]

    hist_df[start] = to_first_dates(hist_df[start], freq)
    hist_df[end] = to_first_dates(hist_df[end], freq)

    equal_df = hist_df[hist_df[start] == hist_df[end]].copy()
    equal_df['event'] = 3   # Create Event column equal to 3
    equal_df = equal_df[[start, 'event', unit, uid]]
    equal_df.drop_duplicates(inplace=True)
    equal_df.rename(columns={start: frequency}, inplace=True)

    hist_df = hist_df[hist_df[start] != hist_df[end]]
    log.info('Generating panel data. May take a few minutes.')
    start_time = time.time()    # Store start time of generation
    panel_df = pd.concat([expand_history(r[start], r[end],
                                         r[unit], r[uid],
                                         freq)
                          for i, r in hist_df.iterrows()])
    stop_time = time.time()  # Store stop time of generation
    log.info('Generating panel data took %d seconds.',
             round(stop_time-start_time, 2))

    panel_df.rename(columns={'date': frequency}, inplace=True)
    panel_df = panel_df.append(equal_df)

    if min_date:
        panel_df = panel_df[panel_df[frequency] >= min_date]

    return panel_df.reset_index(drop=True)


def check_overlaps(x, start='unit_start_date', end='unit_end_date'):
    """Determines if any unit history rows are overlapping.

    Parameters
    ----------
    df : pandas DataFrame
    start : str
    end : str

    Returns
    -------
    overlap : bool
    """
    if x.shape[0] == 1:
        overlap = False
    else:
        x = x.sort_values(start)
        overlap = any(x.iloc[i][end] > x.iloc[i+1][start]
                      for i in range(len(x.index)-1))
    return overlap

TODAY = datetime.today().date()

def resolve_units(udf,
                  start='unit_start_date', end='unit_end_date',
                  unit='unit', uid='UID'):
    """Resolves unit history conflicts (overlaps) for a single
       individual's unit history in non-panel format.

    Parameters
    ----------
    udf : pandas DataFrame
        DataFrame of a single individual's unit history
    start : str
        Name of unit start date column in unit history DataFrames in uh_list
    end : str
        Name of unit end date column in unit history DataFrames in uh_list
    uid : str
        Name of unique ID column in unit history
    unit : str
        Name of unit column in unit history DataFrames in uh_list

    Returns
    -------
    udf : pandas DataFrame
    """
    CODE = 'CODE'
    DAY = timedelta(days=1)
    udf = udf[[uid, unit, start, end]]\
        .sort_values([start, end])\
        .reset_index(drop=True)
    udf[CODE] = udf[end].map(lambda x: 1 - int(pd.isnull(x)))
    udf[end] = pd.to_datetime(udf[end].fillna(TODAY))
    udf[start] = pd.to_datetime(udf[start])
    i = 0

    while i < len(udf.index) - 1:
        udf = udf[udf[end] > udf[start]]
        udf = udf.sort_values([start, end]).reset_index(drop=True)
        if udf.shape[0] == 1 or udf.index[-1] == i:
            break
        tdf = udf.iloc[[i,i+1]]

        if check_overlaps(tdf, start, end):
            if tdf[tdf.CODE < 1].shape[0] == 1:
                if (tdf[tdf.CODE < 1][start].min() <
                    tdf[tdf.CODE==1][start].min()):
                    new_row = tdf[tdf.CODE < 1]
                    new_row.loc[:,[start, CODE]] = \
                        (tdf[tdf.CODE == 1][end].iloc[0] + DAY,
                         new_row[CODE] - 1)
                    udf.loc[tdf[tdf.CODE < 1].index, end] = \
                        tdf.loc[tdf.CODE==1, start].iloc[0] - DAY
                    udf = udf.append(new_row)
                else:
                    udf.loc[tdf[tdf.CODE < 1].index, [start, CODE]] = \
                        (tdf.loc[tdf.CODE==1, end].iloc[0] + DAY,
                         udf.loc[tdf[tdf.CODE < 1].index, CODE] - 1)
            elif tdf[tdf.CODE < 1].shape[0] == 0:
                if tdf[end].nunique() == 1:
                    if tdf[start].nunique() != 1:
                        udf.loc[tdf.index[0], end] = tdf[start].iloc[1] - DAY
                    else:
                        udf = udf.drop(tdf.iloc[[1]].index)
                else:
                    new_row = tdf.iloc[[0]]
                    new_row.loc[:,[start, CODE]] = \
                        (tdf[end].iloc[1] + DAY, 0)
                    udf.loc[tdf.index[0], end] = \
                        tdf[start].iloc[1] - DAY
                    udf = udf.append(new_row)

            elif tdf[tdf.CODE < 1].shape[0] == 2:
                if tdf[start].nunique() == 1:
                    if tdf[CODE].nunique() > 1:
                        udf = udf.drop(tdf.sort_values(CODE).index[0])
                    else:
                        udf = udf.drop(tdf.sort_values(unit).index[1])
                elif tdf[unit].nunique() == 1:
                    udf = udf.drop(tdf.index[1])
                else:
                    udf.loc[tdf.index[0], end] = tdf[start].iloc[1] - DAY

            else:
                raise 'Well this should not happen.'
        else:
            i+=1
    return udf.reset_index(drop=True)
