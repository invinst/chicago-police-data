#!usr/bin/env python3
#
# Author(s):  Roman Rivera (Invisible Institute)

'''script containing functions for assigning unique ids and aggregating data'''

import pandas as pd
import warnings
import numpy as np
from scipy import stats

from general_utils import keep_duplicates, remove_duplicates, list_diff

def generate_uid_report(full, unique, conflicts, resolved, uids):
    return (('Total rows: {0}\n'
             'Unique id + conflict rows: {1}\n'
             'Conflict rows: {2}\n'
             'Resolved conflcit UID count: {3}\n'
             'Total unique ids: {4}\n'
             '').format(full, unique, conflicts, resolved, uids))

def resolve_conflicts(df, id_cols, conflict_cols,
                      uid, start_uid):
    """Determines if multiple observations are conflicting
       and assigns same IDs based on id_cols and no conflicts.

       Groups data by id_cols, then iterates over groups and
       determines if conflict_cols contain non-NA conflicting information.
       If no conflicting information is found, group is determined to be same.
       If conflicting information is found and sub-groups can be formed,
       resolve_conflicts is called recursively at most once per group,
       otherwise each observation in group is distinct.
       Once distinct-ness is determined, uids are assigned based on number of
       distinct observations found before in the loop + start_uid.

    Parameters
    ----------
    df : pandas DataFrame
    id_cols : list
        List of column names used for grouping
    conflict_cols : list
        List of column names used for conflict evaluation
    uid : str
        Name of unique ID column
    start_uid : int, float, etc.
        Number after which new unique IDs should be assigned

    Returns
    -------
    out_df : pandas DataFrame
    start_uid : int, float, etc.
        Start uid for next resolve_conflicts
    Examples
    --------
    """
    temp_fillna=-9999
    out_df = pd.DataFrame()

    df.fillna(temp_fillna, inplace=True)

    df.reset_index(drop=True, inplace=True)
    group_df = df.groupby(id_cols, as_index=False)

    for key, group in group_df:
        group = group.reset_index(drop=True)
        recur = False
        conflicts = 0
        for col in conflict_cols:
            non_nan = group[col]
            non_nan = non_nan[non_nan != temp_fillna]
            if (non_nan.nunique() > 1 and
                    non_nan.nunique() < group.shape[0] and
                    recur == False and
                    non_nan.size == group.shape[0]):
                recur=True
                group, start_uid = resolve_conflicts(group.copy(),
                                                     id_cols + [col],
                                                     list_diff(conflict_cols,
                                                               [col]),
                                                     uid, start_uid)

            elif non_nan.nunique() > 1:
                conflicts += 1
        if recur:
            pass
        elif conflicts == 0:
            group.insert(0, uid, start_uid + 1)
            start_uid += 1
        else:
            group.insert(0, uid, start_uid + group.index + 1)
            start_uid += group.index.nunique()
        out_df = out_df.append(group)

    out_df[out_df == temp_fillna] = np.nan
    return out_df, start_uid


def assign_unique_ids(df, uid, id_cols, conflict_cols=[], log=None):
    """Assigns unique IDs (uid) to dataframe based on id_cols groupings.
       If conflict_cols are specified, conflicts will be resolved
       to determine if id_cols groupings with differing conflict_cols
       information are actually distinct.

    Parameters
    ----------
    df : pandas DataFrame
    uid : str
        Name of unique ID column
    id_cols : list
        List of column names used for grouping
    conflict_cols : list
        List of column names used for conflict evaluation
    log : logger
        If given, uid_report will be generated and logged as info
    Returns
    -------
    out_df : pandas DataFrame

    Examples
    --------
    """
    dfu = df[id_cols + conflict_cols].drop_duplicates()
    dfu.reset_index(drop=True, inplace=True)

    full_row_count = df.shape[0]
    unique_rows = dfu.shape[0]
    conflict_rows = 0
    conflicts_resolved = 0

    if conflict_cols:
        rd_df = remove_duplicates(dfu, id_cols).reset_index(drop=True)
        rd_df.insert(0, uid, rd_df.index + 1)

        kd_df = keep_duplicates(dfu, id_cols).reset_index(drop=True)
        conflict_rows = kd_df.shape[0]
        rc_df, ending_uid = resolve_conflicts(kd_df, id_cols, conflict_cols,
                                              uid, max(rd_df[uid]))
        if rc_df.shape[0] == 0:
            conflicts_resolved = 0
        else:
            conflicts_resolved = rc_df[uid].nunique()

        df = df.merge(rd_df.append(rc_df),
                      on=id_cols + conflict_cols,
                      how='left')

    else:
        dfu[uid] = dfu.index + 1
        df = df.merge(dfu, on=id_cols, how='left')

    assert df[df[uid].isnull()].shape[0] == 0,\
        print('Some unique IDs are null')
    assert max(df[uid]) == df[uid].nunique(),\
        print('Unique IDs are not correctly scaled')

    uid_count = max(df[uid])
    uid_report = generate_uid_report(full_row_count, unique_rows,
                                     conflict_rows, conflicts_resolved,
                                     uid_count)
    if log: log.info(uid_report)
    else: print(uid_report)
    return df


def order_aggregate(df, id_cols,
                    agg_cols, order_cols,
                    minimum=False):
    """Aggregates pandas dataframe within groups, specified by id_cols,
       and order columns for aggregation (agg_cols) by order_cols,
       taking either first (minimum=False) or last (minimum=True) (non-NA)
       observation in each grouping.

    Parameters
    ----------
    df : pandas DataFrame
    id_cols : list
        List of column names used for grouping
    agg_cols : list
        List of column names of columns to be aggregated
    order_cols : list
        List of column names of columns used to determine ordering
    minimum : bool
        If True order_aggregate will take last observations
        If False order_aggregate will take first observations

    Returns
    -------
    oa_df : pandas DataFrame

    Examples
    --------
    """
    oa_df = df[id_cols].drop_duplicates()
    for ac in agg_cols:
        ac_df = df[id_cols + order_cols + [ac]]\
                    .dropna(axis=0, how='any')\
                    .drop_duplicates()\
                    .sort_values(order_cols, ascending=minimum)\
                    .drop(order_cols, axis=1)\
                    .groupby(id_cols, as_index=False)\
                    .agg(lambda x: x.iloc[0])
        oa_df = oa_df.merge(ac_df, on=id_cols, how='outer')
    return oa_df


def max_aggregate(df, uid, col):
    """Aggregates pandas dataframe by grouping by uid
       and taking maximum (non-NA) observation in col for each group.
       uids with only NA values in col are not returned in aggregated dataframe.
    Parameters
    ----------
    df : pandas DataFrame
    uid : str
        Name of unique ID column
    col : str
        Name of column to be aggregated

    Returns
    -------
    ma_df : pandas DataFrame

    Examples
    --------
    """
    df = df[[uid, col]].drop_duplicates()
    df.dropna(axis=0, how='any', inplace=True)
    kd_df = keep_duplicates(df, uid)
    if kd_df.empty:
        ma_df = df
    else:
        df = df[~df[uid].isin(kd_df[uid])]
        groups = kd_df.groupby(uid, as_index=False)
        groups = groups.agg(np.nanmax)
        ma_df = df.append(groups).reset_index(drop=True)
    return ma_df

def mode_aggregate(df, uid, col):
    """Aggregates pandas dataframe by grouping by uid
       and taking most common (non-NA) observation in col for each group.
       uids with only NA values in col are not returned in
       aggregated dataframe. If there are multiple mode values
       for a group the first (last observed) is returned.

    Parameters
    ----------
    df : pandas DataFrame
    uid : str
        Name of unique ID column
    col : str
        Name of column to be aggregated

    Returns
    -------
    ma_df : pandas DataFrame

    Examples
    --------
    """
    warnings.filterwarnings("ignore") # Ignore nan filtering warning
    df = df[[uid, col]]
    df.dropna(axis=0, how='any', inplace=True)
    kd_df = keep_duplicates(df, uid)
    if kd_df.empty:
        ma_df = df
    else:
        df = df[~df[uid].isin(kd_df[uid])]
        groups = kd_df.groupby(uid, as_index=False)
        groups = groups.aggregate(lambda x:
                                  stats.mode(x,
                                             nan_policy='omit').mode[0])
        groups = pd.DataFrame(groups, columns=[uid, col])
        ma_df =  df.append(groups).reset_index(drop=True)
    return ma_df


def aggregate_data(df, uid, id_cols=[],
                   mode_cols=[], max_cols=[],
                   current_cols=[], time_col="",
                   merge_cols=[], merge_on_cols=[]):
    """Aggregates pandas dataframe by grouping by uid and id_cols.
       Utilizes various forms of aggregation for specified columns:
       mode (most common value), max (maximum value),
       current (most current based on time column),
       and column values (merge_cols) that should be merged to a previously
       aggregated column (merge_on_cols) rather than being aggregated.

    Parameters
    ----------
    df : pandas DataFrame
    uid : str
        Name of unique ID column
    id_cols : list
        List of column names used for grouping
    mode_cols : list
        List of columns to be used in mode_aggregate
    max_cols : list
        List of columns to be used in max_aggregate
    current_cols : list
        List of columns to be used in order_aggregate with time_col for ordering
        agg_df will have 'current_' + name for each current_col aggregated
    time_col : str
        Name of column used for ordering current_cols in order_aggregate
    merge_cols : list
        List of columns to be merged after aggregation of merge_on_cols
    merge_on_cols : list
        List of columns that were aggregated and used for merging

    Returns
    -------
    agg_df : pandas DataFrame

    Examples
    --------
    """
    uid_col = [uid]
    agg_df = df[uid_col + id_cols].drop_duplicates()
    agg_df.reset_index(drop=True, inplace=True)

    for col in mode_cols:
        agg_df = agg_df.merge(mode_aggregate(df[[uid, col]], uid, col),
                              on=uid, how='left')

    for col in max_cols:
        agg_df = agg_df.merge(max_aggregate(df[[uid, col]], uid, col),
                              on=uid, how='left')

    if current_cols and time_col:
        df[time_col] = pd.to_datetime(df[time_col])
        oa_df = order_aggregate(df[uid_col + [time_col] + current_cols],
                                uid_col, current_cols, [time_col])
        agg_df = agg_df.merge(oa_df, on=uid, how='left')
        agg_df.columns = ['current_' + col.replace('current_', '')
                          if col in current_cols else col
                          for col in agg_df.columns]

    if merge_cols and merge_on_cols:
        assert set(merge_on_cols) < set(agg_df.columns),\
            "Some merge_on_cols are not in the aggregated data"
        merge_df = df[uid_col + merge_cols + merge_on_cols].drop_duplicates()
        agg_df = agg_df.merge(merge_df, on=uid_col + merge_on_cols, how='left')
        assert agg_df.shape[0] == df[uid].nunique(),\
            "Some uids were gained or lost in merge cols step"
    return agg_df
