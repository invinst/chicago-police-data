#!usr/bin/env python3
#
# Authors:  Roman Rivera

'''A script containing functions for
assigning unique or universal ids and aggregating data
'''
import pandas as pd
import numpy as np
from scipy import stats


def remove_duplicates(df, cols=[]):
    '''returns pandas dataframe

       excluding rows that contain duplicate values
       in the specified columns
       EX: remove_duplicates(..., cols = ['A', 'B'])
        A B C     A B C
        1 2 1     1 3 3
        1 2 3 ->
        1 3 3
    '''
    # Check if cols is empty
    if not cols:
        # Set cols to be all columns in dataframe
        cols = df.columns.tolist()
    # Return subset of input dataframe
    # dropping rows which are duplicates in specified cols
    return df[~df.duplicated(subset=cols, keep=False)].sort_values(cols)


def keep_duplicates(df, cols=[]):
    '''returns pandas dataframe
       including only rows that contain duplicate values
       in specified columns
       EX: keep_duplicates(..., cols = ['A', 'B'])
           A B C     A B C
           1 2 1     1 2 1
           1 2 3 ->  1 2 3
           1 3 3
    '''
    # Check if cols is empty
    if not cols:
        # Set cols to be all columns in dataframe
        cols = df.columns.tolist()
    # Return subset of input dataframe
    # keeping rows which are duplicates in specified cols
    return df[df.duplicated(subset=cols, keep=False)].sort_values(cols)


def resolve_conflicts(df, id_cols, conflict_cols,
                      uid, starting_uid,
                      temp_fillna=-9999):
    ''' returns pandas dataframe
        after checking for conflicting values in conflict_cols
        and adding new unique ids if there are conflicts
        Ex: resolve_conflicts(..., id_cols = ['A'], conflict_cols=['B', 'C'],
                              uid='ID', starting_uid=10)
            A B   C        A B   C   ID
            1 2   NaN      1 2   NaN 11
            1 NaN 5    ->  1 NaN 5   11
            2 3   NaN      2 3   NaN 12
            2 4   NaN      2 4   NaN 13
    '''
    # Create out_df which will be returned
    out_df = pd.DataFrame()

    # Reset index in input dataframe
    df.reset_index(drop=True, inplace=True)
    # Fill any NaN values with temp_fillna value
    # or groupby will not work properly
    df.fillna(temp_fillna, inplace=True)
    # Group dataframe by specified id_cols
    group_df = df.groupby(id_cols, as_index=False)

    # Iterate over groups
    for key, group in group_df:
        group = group.reset_index(drop=True)    # Reset index
        conflicts = 0   # Set conflict counter at 0

        # Iterate over specified conflict_cols
        for col in conflict_cols:
            non_nan = group[col]    # Select column
            # Drop temporary NaN values
            non_nan = non_nan[non_nan != temp_fillna]
            # If there are more than 1 unique, non NaN, values in column
            if len(non_nan.unique()) > 1:
                # Then add 1 to conflict counter
                conflicts += 1

        # If there are no conflicts
        if conflicts == 0:
            # All rows in group are considered to be from same individual
            # So, insert the specified uid column used in assign_unique_ids()
            # Since there is only 1 individual in the group,
            # Set uid column to the starting_uid + 1
            group.insert(0, uid, starting_uid + 1)
            # Add 1 to starting uid for next group
            starting_uid += 1
        # If there are conflicts
        else:
            # All rows in group are considered to be from different individuals
            # So, insert the specified uid column used in assign_unique_ids()
            # Since all rows in the group are unique individuals,
            # Set uid column equal to starting_uid + group.index + 1
            group.insert(0, uid, starting_uid + group.index + 1)
            # Add the number of uids added to the starting_uid for next group
            starting_uid += len(group.index)
        # Append the group with resolved conflicts to the out_df
        out_df = out_df.append(group)

    # Reset the temporary NaN values in the out_df back to NaN
    out_df[out_df == temp_fillna] = np.nan
    return out_df


def generate_uid_report(full, unique, conflicts, resolved, uids):
    return (('Total rows: {0}\n'
             'Unique id + conflict rows: {1}\n'
             'Conflict rows: {2}\n'
             'Resolved conflcit UID count: {3}\n'
             'Total unique ids: {4}\n').format(full,
                                               unique,
                                               conflicts,
                                               resolved,
                                               uids))


def assign_unique_ids(df, uid, id_cols, conflict_cols=[]):
    '''returns pandas dataframe
       with unique ids assigned to rows which share id_cols values
       and lack conflicting information in the conflict_cols
       rows with conflicting conflict_cols will be at end of dataframe
       EX: assign_unique_ids(..., 'ID', ['A'], ['B', 'C'])
       A B   C        A B   C   ID
       1 2   2        2 3   3    1
       1 NaN NaN      3 4   1    2
       1 2   NaN      3 4   1    2
       2 3   3        1 2   2    3
       3 4   1    ->  1 2   NaN  3
       3 4   1        1 NaN NaN  3
       4 2   NaN      4 2   NaN  4
       4 NaN 5        4 NaN 5    4
       5 3   NaN      5 3   NaN  5
       5 4   NaN      5 4   NaN  6
    '''
    full_rows = df.shape[0]  # Store total row count
    # Create unique dataframe of relevant columns
    dfu = df[id_cols + conflict_cols].drop_duplicates()
    # Reset index on unique dataframe
    dfu.reset_index(drop=True, inplace=True)
    unique_rows = dfu.shape[0]  # Store unique rows
    conflict_rows = 0   # Initialize conflict rows
    conflicts_resolved = 0  # Initialize conflicts resolved
    # If conflict columns are specified
    if conflict_cols:
        # Create a subset of the input dataframe with no duplicates
        # and reset the index
        rd_df = remove_duplicates(dfu, id_cols).reset_index(drop=True)
        # Insert a uid column in this subset equal to each row's index + 1
        rd_df.insert(0, uid, rd_df.index + 1)

        # Create a subset of the input dataframe with only duplicates
        # These are the rows which contain the same id_cols information
        # But have different conflict_cols information
        # and reset the index
        kd_df = keep_duplicates(dfu, id_cols).reset_index(drop=True)
        conflict_rows = kd_df.shape[0]  # Store conflict rows
        # Create resolved conflicts dataframe using resolve_conflict()
        # input the maximum uid from the rd_df as the starting_uid
        rc_df = resolve_conflicts(kd_df, id_cols, conflict_cols,
                                  uid=uid, starting_uid=max(rd_df[uid]))

        # Check if there are no conflicts
        if rc_df.shape[0] == 0:
            conflicts_resolved = 0
        else:
            # Assign conflicts resolved count to number of unique uids
            conflicts_resolved = len(rc_df[uid].unique())
        # Append resolved conflicts dataframe to remove duplicates dataframe
        # and merge the resulting dataframe to input dataframe
        # on id_cols and conflict_cols, thus giving input df uids
        df = df.merge(rd_df.append(rc_df),
                      on=id_cols + conflict_cols,
                      how='left')

    # If no conflict columns are specified
    else:
        # Assign a unique ids to each row of unique id_cols
        # such that the specified uid column is equal to its index + 1
        dfu[uid] = dfu.index + 1
        # Merge the unique dataframe with the uid back to the input dataframe
        # on the unique columns, thus giving each row in the full data a uid
        df = df.merge(dfu, on=id_cols, how='left')

    # Assert that no uids can be null and max uid = number of uids
    assert df[df[uid].isnull()].shape[0] == 0,\
        print('Some unique IDs are null')
    assert max(df[uid]) == len(df[uid].drop_duplicates()),\
        print('Unique IDs are not correctly scaled')

    uid_count = max(df[uid])    # Store number of uids
    # Generate unique id report
    uid_report = generate_uid_report(full_rows, unique_rows,
                                     conflict_rows, conflicts_resolved,
                                     uid_count)
    # Print out unique id report
    print(uid_report)

    # Return dataframe and report
    return df, uid_report


def order_aggregate(df, id_cols,
                    agg_cols, order_cols,
                    minimum=False):
    '''returns an aggregated pandas dataframe
       after, in each group, specified by id_cols,
       the agg_cols are ordered by the order_cols
       and the agg_col value that corresponds with the
       maximimum (or minimum) values in order_cols is selected.

       EX: order_aggregate(..., ['A'], ['B' ,'C'], ['D'])
       A B C D     A B C
       1 2 6 3     1 5 1
       1 5 1 7  -> 2 2 1
       1 3 3 1
       2 3 4 NaN
       2 2 1 2
    '''
    # Initialize ordered/aggregated df
    oa_df = df[id_cols].drop_duplicates()
    # Iterate over aggregate columns
    for ac in agg_cols:
        # Initialize ac_df of id_cols, order cols, and agg column
        ac_df = df[id_cols + order_cols + [ac]]
        # Drop rows that have any NaN values
        ac_df = ac_df.dropna(axis=0, how='any')
        # Keep only unique rows
        ac_df = ac_df.drop_duplicates()
        # Sort data by the order_cols
        ac_df = ac_df.sort_values(order_cols, ascending=minimum)
        # Drop the order cols columns
        ac_df = ac_df.drop(order_cols, axis=1)
        # Group data by id_cols
        ac_df = ac_df.groupby(id_cols, as_index=False)
        # Aggregate groups by taking first row
        ac_df = ac_df.agg(lambda x: x.iloc[0])
        # Merge aggregated column to ordered/aggregated df on id_cols
        oa_df = oa_df.merge(ac_df, on=id_cols, how='outer')

    # Return ordered and aggregated data
    return oa_df


def max_aggregate(df, uid, col):
    '''returns 2 column pandas dataframe
       with unique id being first column
       and max of each uid's col values in second column
    '''
    # Initialize unique dataframe with uids and specified column only
    df = df[[uid, col]].drop_duplicates()
    # Drop any rows with missing data
    df.dropna(axis=0, how='any', inplace=True)
    # Initialize dataframe of duplicates
    kd_df = keep_duplicates(df, uid)
    # If duplciate dataframe is empty
    if kd_df.empty:
        # No aggregation is neccessary on dfu since all values are unique
        return df  # Return unique dataframe
    # If duplicate dataframe is not empty
    else:
        # Drop duplicate rows from df
        df = df[~df[uid].isin(kd_df[uid])]
        # Group duplicate dataframe by uid
        groups = kd_df.groupby(uid, as_index=False)
        # Take max of each group, excluding NaNs
        groups = groups.agg(np.nanmax)
        # Return unique uid data appended to aggregated uid data
        return df.append(groups)


def mode_aggregate(df, uid, col):
    '''returns 2 column pandas dataframe
       with unique id being first column
       and most common of each uid's col values in second column
    '''
    # Initialize dataframe for specifically this column
    df = df[[uid, col]]
    # Drop null values
    df.dropna(axis=0, how='any', inplace=True)
    # Initialize dataframe of uids which have multiple values
    kd_df = keep_duplicates(df, uid)
    # If duplicates dataframe is empty
    if kd_df.empty:
        # Aggregation is not neccessary since each uid only has one row
        return df  # Return df
    # If there are duplicates
    else:
        # Ensure dfc only contains rows with uids that only appear once
        df = df[~df[uid].isin(kd_df[uid])]
        # Group duplicate dataframe by uid
        groups = kd_df.groupby(uid, as_index=False)
        # If col is specified in mode_cols
        # then aggregate groups by (first) most common value
        groups = groups.aggregate(lambda x: stats.mode(x).mode[0])
        # Recombine list of lists into dataframe
        groups = pd.DataFrame(groups, columns=[uid, col])
        # Return dataframe of unique uids appended to aggregated uid data
        return df.append(groups)


def aggregate_data(df, uid, id_cols=[],
                   mode_cols=[], max_cols=[],
                   current_cols=[], time_col="",
                   merge_cols=[], merge_on_cols=[]):
    '''returns an aggregated pandas dataframe
       with one entry per specified uid (and id_cols combination)
       columns specified for aggregation can be aggregated by
       mode (finding most common value in a column for each uid),
       max (finding largest value in a column for each uid),
       or current (finding most recent value in the column
       using order_aggregate and using a specified time_col for ordering).
       The resulting data can have merge columns added to it by specifying
       the merge_on_cols (which unique ids will automatically be added to)
       such that the result of column aggregation can have information added.

       EX: aggregate_data(..., 'A', ['B'],
                          ['C'], ['D'],
                          [], [],
                          ['E'],['C','D'])
       A B C D E      A B C D E
       7 3 1 2 'a'    7 3 1 3 'y'
       7 3 1 2 'a' -> 3 4 2 0 'g'
       7 3 1 0 'x'
       7 3 5 3 'y'
       3 4 2 0 'g'
    '''
    uid_col = [uid]  # Create list of specified uid
    # Initialize agg_df by taking unique rows (uids and id_cols)
    agg_df = df[uid_col + id_cols].drop_duplicates()
    agg_df.reset_index(drop=True, inplace=True)  # Reset index

    # Iterate over mode cols
    for col in mode_cols:
        # Merge mode_aggregated column to agg_df
        agg_df = agg_df.merge(mode_aggregate(df[[uid, col]], uid, col),
                              on=uid, how='left')
    # Iterate over max cols
    for col in max_cols:
        # Merge max_aggregated column to agg_df
        agg_df = agg_df.merge(max_aggregate(df[[uid, col]], uid, col),
                              on=uid, how='left')

    # If current_cols and time_col are specified
    if current_cols and time_col:
        # Ensure that time_col is a pandas datetime object
        df[time_col] = pd.to_datetime(df[time_col])
        # Aggregate data by taking most recent (by time_col)
        # occurence of the current_cols specified
        oa_df = order_aggregate(df[uid_col + [time_col] + current_cols],
                                uid_col, current_cols, [time_col])
        # Merge order aggregated data to agg_df on uid
        agg_df = agg_df.merge(oa_df, on=uid, how='left')
        # Now that the current_cols are 'current', the name must be changed
        # Add the prefix 'Current' to the current_cols in agg_df
        agg_df.columns = ['current.' + col.replace('current.', '')
                          if col in current_cols else col
                          for col in agg_df.columns]

    # If merge and merge_on cols are specified
    if merge_cols and merge_on_cols:
        # Ensure that the merge_on_cols exist in agg_df
        assert set(merge_on_cols) < set(agg_df.columns),\
            "Some merge_on_cols are not in the aggregated data"
        # Initialize merge dataframe as unique rows containing
        # uid, merge, and merge_on cols
        merge_df = df[uid_col + merge_cols + merge_on_cols].drop_duplicates()
        # Merge merge_df onto the agg_df on uid and merge_on_cols
        agg_df = agg_df.merge(merge_df, on=uid_col + merge_on_cols, how='left')
        # Ensure no rows were lost or gained
        assert agg_df.shape[0] == len(df[uid].unique()),\
            "Some uids were gained or lost in merge cols step"
    return agg_df
