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


def split_group(dfm):
    """Use closure check_conflicts() to recursively determine if
       records in a dataframe are same, distinct, or not unresolved (NA)

    Parameters
    ----------
    dfm : pandas DataFrame

    Returns
    -------
    rl : list (of lists)
    """
    rl = []
    strt=0
    def check_conflicts(df):
        """Recursively finds conflicts, gives non-conflicted indexes same id
        """
        nonlocal strt
        nonlocal rl
        for col in df.columns:
            if df[col].nunique == df[col].size:
                for ind, num in zip(df.index, range(df.shape[0])):
                    rl.append([ind, strt])
                    strt +=1
            elif df[col].nunique() > 1:
                if df[col].notnull().all():
                    for i,g in df.loc[:,col:].groupby(col, as_index=False):
                        check_conflicts(g)
                else:
                    for ind in df.index:
                        rl.append([ind, np.nan])
            elif col == df.columns[-1]:
                for ind, num in zip(df.index, range(df.shape[0])):
                        rl.append([ind, strt])
                strt +=1
            else:
                continue
            break
    check_conflicts(dfm)
    return rl


def resolve_conflicts(df, id_cols, conflict_cols, uid='id', start_uid=0):
    """Iterates over groups with conflicting records,
       passing them into split_group to resolve conflicts,
       and determines uids based on start_uid

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
    """
    sg_lst = []
    for k,grp in df.groupby(id_cols, as_index=False):
        sg_lst.append(split_group(grp[sorted(conflict_cols,
                                             key=lambda x: grp[x].count(),
                                             reverse=True)]))
    fl = []
    for sg in sg_lst:
        tsa = np.array(sg, dtype=np.float64)
        tsa[:,1] += start_uid
        fl.append(tsa)
        if not np.isnan(tsa[:,1]).all():
            start_uid = np.nanmax(tsa[:,1]) + 1
    odf = pd.DataFrame(np.concatenate(fl,axis=0),
                       columns=['ind', uid])
    assert odf.shape[0] == df.shape[0]
    assert set(odf.ind) == set(df.index)
    df = df.merge(odf, left_index=True, right_on='ind', how='inner')\
        .drop('ind',axis=1)
    return df


def manual_resolve(gdf, uid, start_uid):
    """Resolves conflicts in groupings based on user input.

    Parameters
    ----------
    gdf : pandas DataFrame
    uid : str
        Name of unique ID column
    start_uid : int

    Returns
    -------
    gdf : pandas DataFrame
    """
    print('Current group (rows = %d): \n %s' % (gdf.shape[0], gdf))
    user_uids = input("Assign uids:\n"
                      "(0 to n-1) in separated by commas,\n"
                      "'same' = all same,\n"
                      "'distinct' = all distinct\n"
                      "'quit' = quit\n"
                      "input: ")
    if user_uids == 'same':
        gdf[uid] = start_uid
    elif user_uids == 'distinct':
        gdf[uid] = np.arange(start_uid, start_uid + gdf.shape[0])
    elif user_uids == 'quit':
        gdf[uid] = np.nan
    else:
        try:
            gdf[uid] = [int(i) + start_uid for i in user_uids.split(',')]
        except:
             print("Sorry bad input. Try that again.")
             print(gdf)
             gdf = manual_resolve(gdf, uid, start_uid)
    return gdf


def assign_unique_ids(df, uid, id_cols, conflict_cols=None,
                      log=None, unresolved_policy = 'distinct'):
    """Assigns unique IDs (uid) to dataframe based on id_cols groupings.
       If conflict_cols are specified, conflicts will be resolved
       to determine if id_cols groupings with differing conflict_cols
       information are actually distinct, unresolved conflicts can be
       handled as 'distinct', 'same', or 'manual'.

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
    unresolved_policy: str
        Determine how to handle unresolved conflicts
        'distinct' = each unresolved in a group is distinct,
        'same' = each unresolved in a group is the same,
        'manual' = send unresolved groups to manual_resolve()

    Returns
    -------
    out_df : pandas DataFrame
    """
    if conflict_cols is None: conflict_cols = []
    dfu = df[id_cols + conflict_cols].drop_duplicates()
    dfu.reset_index(drop=True, inplace=True)

    full_row_count = df.shape[0]
    unique_rows = dfu.shape[0]
    conflict_rows = 0
    conflicts_resolved = 0

    if conflict_cols and dfu.shape[0] > dfu[id_cols].drop_duplicates().shape[0]:
        rd_df = remove_duplicates(dfu, id_cols).reset_index(drop=True)
        rd_df.insert(0, uid, rd_df.index + 1)

        kd_df = keep_duplicates(dfu, id_cols).reset_index(drop=True)
        kd_df[id_cols] = kd_df[id_cols].fillna(value=-9999)

        conflict_rows = kd_df.shape[0]
        next_uid = 1 if rd_df[uid].dropna().empty else rd_df[uid].max() + 1

        rc_df = resolve_conflicts(kd_df, id_cols, conflict_cols,
                                  uid, next_uid)

        if log:
            log.info('%d resolved conflicts. %d unresolved conflicts'
                     % (rc_df[uid].count(),
                        rc_df[uid].size - rc_df[uid].count()))

        if not rc_df[uid].dropna().empty:
            next_uid = rc_df[uid].max() + 1
        if rc_df[uid].isnull().sum() > 0:
            if unresolved_policy == 'distinct':
                rc_df.loc[rc_df[uid].isnull(), uid] = \
                    np.arange(next_uid, next_uid + rc_df[uid].isnull().sum())
            elif unresolved_policy == 'same':
                sdf = pd.DataFrame()
                for k, g in rc_df[rc_df[uid].isnull()].groupby(id_cols):
                    g[uid] = next_uid
                    next_uid += 1
                    sdf = sdf.append(g)
                rc_df = rc_df.dropna(subset=[uid]).append(sdf)

            elif unresolved_policy == 'manual':
                mr_df = pd.DataFrame()
                for k, g in rc_df\
                    .loc[rc_df[uid].isnull(), id_cols + conflict_cols]\
                    .groupby(id_cols, as_index=False):
                        g = manual_resolve(g, uid, next_uid)
                        next_uid = g[uid].max() + 1
                        mr_df = mr_df.append(g)
                if log:
                    log.info('Unresolved conflicts resolved by "%s" into %d ids'
                             % (unresolved_policy, mr_df[uid].nunique()))
                rc_df = rc_df.dropna(subset=[uid]).append(mr_df)

        rc_df[id_cols] = rc_df[id_cols].replace(-9999, np.nan)

        if rc_df.shape[0] == 0:
            conflicts_resolved = 0
        else:
            conflicts_resolved = rc_df[uid].nunique()
        df = df.merge(rd_df.append(rc_df),
                      on=id_cols+conflict_cols, how='left')

    else:
        dfu[uid] = dfu.index + 1
        df = df.merge(dfu, on=id_cols + conflict_cols, how='left')

    assert keep_duplicates(df[[uid] + id_cols].drop_duplicates(), uid).empty,\
        'This should not happen. Same uids between id_col groupings.'
    assert df[df[uid].isnull()].shape[0] == 0,\
        'Some unique IDs are null:\n%s' % df[df[uid].isnull()]
    assert max(df[uid]) == df[uid].nunique(),\
        'Unique IDs are not correctly scaled'

    uid_count = max(df[uid])
    uid_report = generate_uid_report(full_row_count, unique_rows,
                                     conflict_rows, conflicts_resolved,
                                     uid_count)
    if log:
        log.info(uid_report)
    else:
        print(uid_report)

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
        df = remove_duplicates(df, uid)
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
        df = remove_duplicates(df, uid)
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
        dfu = df[uid_col + [time_col] + current_cols].drop_duplicates()
        dfu[time_col] = pd.to_datetime(dfu[time_col])
        oa_df = order_aggregate(dfu, uid_col, current_cols, [time_col])
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
