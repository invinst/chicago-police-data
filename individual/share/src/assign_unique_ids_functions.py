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


def keep_duplicates(df, cols):
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
            # Drop temporary NaN values (-999 used in integer columns)
            non_nan = non_nan[(non_nan != -999) &
                              (non_nan != temp_fillna)]
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
    # Create unique dataframe of relevant columns
    dfu = df[id_cols + conflict_cols].drop_duplicates()
    # Reset index on unique dataframe
    dfu.reset_index(drop=True, inplace=True)

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
        # Create resolved conflicts dataframe using resolve_conflict()
        # input the maximum uid from the rd_df as the starting_uid
        rc_df = resolve_conflicts(kd_df, id_cols, conflict_cols,
                                  uid=uid, starting_uid=max(rd_df[uid]))
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

    return df


def order_aggregate(df, id_cols,
                    agg_cols, order_cols,
                    minimum=False):
    ''' returns an aggregated pandas dataframe
        after, in each group, specified by id_cols,
        the agg_cols are ordered by the order_cols
        and the agg_col value that corresponds with the
        maximimum (or minimum) values in order_cols is selected.

        for example: getting each officer's most recent star number
    '''
    df = df.dropna(axis=0, subset=order_cols)
    df = df.drop_duplicates()
    df.sort_values(order_cols, ascending=minimum, inplace=True)
    df.drop(order_cols, axis=1, inplace=True)
    df = df.groupby(id_cols, as_index=False)[agg_cols]
    return df.agg(lambda x: x.iloc[0])


def aggregate_data(df, uid, id_cols=[],
                   mode_cols=[], max_cols=[],
                   current_cols=[], time_col=""):
    ''' returns an aggregated pandas dataframe
        with one entry per specified uid (and id_cols combination)
        columns specified for aggregation can be aggregated by
        mode (finding most common value in a column for each uid),
        max (finding largest value in a column for each uid),
        or current (finding most recent value in the column
        using order_aggregate and using a specified time_col for ordering)
   '''
    uid_col = [uid]
    agg_df = df[uid_col + id_cols].drop_duplicates()
    agg_df.reset_index(drop=True, inplace=True)

    for col in mode_cols + max_cols:
        dfu = df[[uid, col]].drop_duplicates().dropna()
        kd_df = keep_duplicates(dfu, uid)
        dfu = dfu[~dfu[uid].isin(kd_df[uid])]

        if kd_df.empty:
            agg_df = agg_df.merge(dfu, on=uid, how='left')
        else:
            groups = kd_df.groupby(uid, as_index=False)

            if col in mode_cols:
                print('Mode Aggregating {} column'.format(col))
                groups = [[k,
                           stats.mode(g[col],
                                      nan_policy='propagate').mode[0]]
                          for k, g in groups]
                groups = pd.DataFrame(groups, columns=[uid, col])

            if col in max_cols:
                print('Max Aggregating {} column'.format(col))
                groups = groups.agg(np.nanmax)

            groups = pd.concat([groups, dfu])
            agg_df = agg_df.merge(groups, on=uid, how='left')

    if current_cols and time_col:
        df[time_col] = pd.to_datetime(df[time_col])
        agg_df = agg_df.merge(
                    order_aggregate(
                        df[uid_col + [time_col] + current_cols],
                        uid_col, current_cols, [time_col]),
                    on=uid, how='left')
        agg_df.rename(columns=dict(
                            zip(current_cols,
                                ["Current." + tc for tc in current_cols])),
                      inplace=True)

    return agg_df
