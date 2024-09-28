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
            # column is different for every row, 
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
        except Exception as e:
             print(f"Sorry bad input. Try that again. Exception: {e}")
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
                    sdf = pd.concat([sdf, g])
                rc_df = pd.concat([rc_df.dropna(subset=[uid]), sdf])

            elif unresolved_policy == 'manual':
                mr_df = pd.DataFrame()
                for k, g in rc_df\
                    .loc[rc_df[uid].isnull(), id_cols + conflict_cols]\
                    .groupby(id_cols, as_index=False):
                        g = manual_resolve(g, uid, next_uid)
                        next_uid = g[uid].max() + 1
                        mr_df = pd.concat([mr_df, g])
                if log:
                    log.info('Unresolved conflicts resolved by "%s" into %d ids'
                             % (unresolved_policy, mr_df[uid].nunique()))
                rc_df = pd.concat([rc_df.dropna(subset=[uid]), mr_df])

        rc_df[id_cols] = rc_df[id_cols].replace(-9999, np.nan)

        if rc_df.shape[0] == 0:
            conflicts_resolved = 0
        else:
            conflicts_resolved = rc_df[uid].nunique()
        df = df.merge(pd.concat([rd_df, rc_df]),
                      on=id_cols+conflict_cols, how='left')

    else:
        dfu[uid] = dfu.index + 1
        df = df.merge(dfu, on=id_cols + conflict_cols, how='left')

    assert keep_duplicates(df[[uid] + id_cols].drop_duplicates(), uid).empty,\
        'This should not happen. Same uids between id_col groupings.'
    assert df[df[uid].isnull()].shape[0] == 0,\
        'Some unique IDs are null:\n%s' % df[df[uid].isnull()]
    assert max(df[uid]) == df[uid].nunique(),\
        f'Unique IDs are not correctly scaled, {df.to_string()}'


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

def sorted_order_aggregate(df, uid, col, sort_order):
    """Aggregates pandas dataframe by grouping by uid
        and taking first non-NA observation in col in order by sort_order.
    Parameters
    ----------
    df : pandas DataFrame
    id_cols : list
        List of column names used for grouping
    col : str
        Column name to be aggregated
    sort_order : list
        Ordered list of values from col for custom sort: 
        values not in sort_order will be dropped/not included

    Returns
    -------
    soa_df : pandas DataFrame

    Examples
    --------
    """
    assert sort_order.issubset(df[col].unique)


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
        ma_df = pd.concat([df, groups]).reset_index(drop=True)
    return ma_df

def min_aggregate(df, uid, col):
    """Aggregates pandas dataframe by grouping by uid
       and taking minimum (non-NA) observation in col for each group.
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
        groups = groups.agg(np.nanmin)
        ma_df = pd.concat([df, groups]).reset_index(drop=True)
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
        groups = groups.aggregate(lambda x: x.mode().iloc[0])
        groups = pd.DataFrame(groups, columns=[uid, col])
        ma_df =  pd.concat([df, groups]).reset_index(drop=True)
    return ma_df

def list_aggregate(df, uid, col):
    """Aggregates by adding new numbered columns for every unique value of col per uid.
    Just a pivot, useful for getting id columns per profile and then reshaping back on merge.

    Parameters
    ----------
    df : pandas DataFrame
    uid : str
        Name of unique ID column
    col : str
        Name of column to be aggregated

    Examples
    --------
    """
    la_df = df[[uid, col]].drop_duplicates()

    return la_df.assign(cumcount=col + (df.groupby(uid)[col].cumcount() + 1).astype(str)) \
            .pivot(index=uid, columns='cumcount', values=col)

def func_aggregate(df, uid, col, func):
    """Aggregate column of df group by uid on col by applying passed func"""
    f_df = df.groupby(uid, as_index=False)[col].agg(func)

    return f_df

def first_aggregate(df, uid, col):
    f_df = df.groupby(uid, as_index=False)[col].first()

    return f_df

def aggregate_data(df, uid, id_cols=[],
                   mode_cols=[], max_cols=[], min_cols=[],
                   current_cols=[], sorted_first_instance_cols=[], time_col="", list_cols=[],
                   func_cols={}, merge_cols=[], merge_on_cols=[]):
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
    sorted_first_instance_cols: list
        List of columns to be used in first_aggregate
        Note: assumes df has already been pre-sorted so that first instance is as desired
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

    for col in min_cols:
        agg_df = agg_df.merge(min_aggregate(df[[uid, col]], uid, col), 
                              on=uid, how='left')

    for col in list_cols:
        agg_df = agg_df.merge(list_aggregate(df[[uid, col]], uid, col),
                              on=uid, how='left')

    for col, func in func_cols.items():
        agg_df = agg_df.merge(func_aggregate(df[[uid, col]], uid, col, func),
                              on=uid, how='left')

    for col in sorted_first_instance_cols:
        agg_df = agg_df.merge(first_aggregate(df[[uid, col]], uid, col),
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

def calculate_possible_ages_as_of_foia_date(df, age_col, date_col, foia_year):
    """Given a age_col that is point in time (age as of date_col), returns age as of target date. 


    Parameters
    ----------
    df : DataFrame
        dataframe with age_col, date_col
    age_col : str
        column with point in time age
    date_col : str
        column with point in time
    foia_date : str/datetime
        date foia was received on, to convert age as of
    
    Returns
    -------
    DataFrame
        df with added current_age column
    """    
    df[date_col] = pd.to_datetime(df[date_col]) 

    year_diff = foia_year - df[date_col].dt.year
    possible_current_age = df[age_col] + year_diff

    df['current_age'] = possible_current_age

    return df


def get_most_recent_rows(df, id_col, date_col):
    """Given dataframe, id col and column representing date, returns just rows representing most recent date_col (either current or most recent)
       Assumes that date_col is something like and "end_date", and gets consider max to be either a null (not terminated) value,
       or the most recent non-null value. 

       Returns full dataframe filtered to just these rows
     """
    # cast just in case
    df[date_col] = pd.to_datetime(df[date_col])

    # get max value, add to it to construct new known max; makes using existing functions (idxmax) possible while properly handling nulls
    max_date_value = df[date_col].max() + pd.Timedelta(days=100)

    # get index representing most recent date col
    max_date_idxs = df.assign(filled_date_col = lambda x: x[date_col].fillna(max_date_value)) \
        .groupby(id_col, as_index=False) \
        .agg(max_date_idx = ('filled_date_col', lambda x: x.idxmax()))

    # merge back to filter just most recent date cols
    return df.reset_index() \
        .merge(max_date_idxs, left_on=[id_col, 'index'], right_on=[id_col, 'max_date_idx']) \
        .drop(['index', 'max_date_idx'], axis=1)

def intrafile_dedupe(df, id, id_cols, change_cols, change_filters, 
                    change_whitelist={}, change_blacklist={}, add_cols={}, log=None):
    """Deduplicates file from intrafile id changes (e.g., if an officer last name changes between rows of the same file)
    
    Parameters
    ----------
    df: dataframe 
        df with interfile changes
    id: str
        column name reprsenting initial attempt at assigning id
    id_cols: list
        relatively stable columns, those relatively unlikely to change (e.g., last_name_NS)
        likely used for generating initial id
    incident_cols: list
        columns more likely to change (e.g., current_age)
    allowed changes: list
        optional, list of changes to actually allow through. 

    Returns
    -------
    possible_changes: dictionary mapping columns to possible changes on that column
    """
    possible_changes = get_intrafile_changes(df, id, id_cols, change_cols, add_cols)

    return apply_changes(df, id, possible_changes, change_filters, change_whitelist, change_blacklist, log)

def get_intrafile_changes(df, id, id_cols, change_cols, add_cols={}):
    """Used for getting possible changes within a single df. 
    Effectively applies blocking then a leave one out self match with stable columns, and a leave 2 out with less stable matches. 

    Parameters
    ----------
    df: dataframe 
        df with interfile changes
    id: str
        column name reprsenting initial attempt at assigning id
    id_cols: list
        relatively stable columns, those relatively unlikely to change (e.g., last_name_NS)
        likely used for generating initial id
    incident_cols: list
        columns more likely to change (e.g., current_age)
    allowed changes: list
        optional, list of changes to actually allow through. 

    Returns
    -------
    possible_changes: dictionary mapping columns to possible changes on that column
    """
    set_join = lambda x: ', '.join(set(x.astype(str)))

    all_cols = list(set(id_cols + change_cols))

    change_ids = []

    # leave one column out at a time, get possible changes within file
    changes = {}
    for change_col in change_cols:
        id_cols = [col for col in all_cols if col != change_col]

        agg_dict = {
            "ids": (id, set_join),
            **{change_col: (change_col, set_join)},
            "change_count": (id, pd.Series.nunique),
            "null_count": (change_col, lambda x: x.replace("", np.nan).isnull().sum())}

        if change_col in add_cols:
            agg_dict.update(add_cols[change_col])

        changes[change_col] = df \
            .loc[~df[id].isin(change_ids), all_cols + [id]].fillna("") \
            .groupby(id_cols, as_index=False) \
            .agg(**agg_dict) \
            .assign(change_col=change_col) \
            .query("change_count > 1")

        change_ids += list(changes[change_col].ids.str.split(", ").explode().astype(int).values)

    return changes

def apply_change_to_row(row, id, df):
    row_df = pd.DataFrame(row).T

    ids = row_df.ids.str.split(", ").explode().astype(int).values
    first_id = ids[0]
    other_ids = ids[1:]

    # set other ids to first id
    df.loc[df[id].isin(other_ids), id] = first_id
    return df

def filter_changes(changes, change_filters={}, change_whitelist={}, change_blacklist={}):
    filtered_changes = {}
    for change_col in changes:
        if change_col in change_filters:
            change_df = changes[change_col].query(change_filters[change_col])
        else:
            change_df = pd.DataFrame()

        if change_col in change_whitelist:
            whitelist_df = changes[change_col].query(f"`{change_col}`.isin({change_whitelist[change_col]})", engine='python')
            change_df = pd.concat([whitelist_df, change_df], axis=0).drop_duplicates()

        if change_col in change_blacklist:
            change_df = change_df.query(f"~`{change_col}`.isin({change_blacklist[change_col]})", engine="python")

        filtered_changes[change_col] = change_df

    return filtered_changes

def apply_changes(df, id, changes, log=None):
    """Apply changes to actual df"""
    change_counts = {}

    for change_col, change_df in changes.items():
        for _, row in change_df.iterrows():
            df = apply_change_to_row(row, id, df)

        change_counts[change_col] = change_df.shape[0]

    if log:
        log.info("Applied change counts:\n" + pd.Series(change_counts, name="Change Counts").to_string())

    return df


class Foia:
    def __init__(self, foia_string):
        self.foia_string = foia_string
        self.parse(foia_string)

    def parse(self, foia_string):
        if foia_string.count("_") == 2:
            foia_string = foia_string + "_"
        elif foia_string.count("_") < 2:
            raise ValueError(f"Unable to parse foia string {foia_string}, less than 2 underscores")
        
        self.type, self.date_range, self.date_received, self.foia_number = foia_string.split("_")
        self.subtype = ""

        self.split_type_subtype()        

    def split_type_subtype(self):
        types = ['complaints', 'TRR']

        for type in types: 
            if type in self.type:
                self.subtype = "-".join(self.type.split("-")[1:])
                self.type = type
                break