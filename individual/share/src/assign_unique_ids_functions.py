#!usr/bin/env python3
# Authors:  Roman Rivera

'''A script containing functions for
assigning unique or universal ids and aggregating data
'''
import pandas as pd
import numpy as np
from scipy import stats


def remove_duplicates(df, cols=[]):
    ''' returns pandas dataframe
        excluding rows that contain duplicate values
        in the specified columns
    '''
    if not cols:
        cols = df.columns.tolist()
    return df[~df.duplicated(subset=cols, keep=False)].sort_values(cols)


def keep_duplicates(df, cols):
    ''' returns pandas dataframe
        including only rows that contain duplicate values
        in specified columns
    '''
    return df[df.duplicated(subset=cols, keep=False)].sort_values(cols)


def resolve_conflicts(df, id_cols, conflict_cols,
                      uid, starting_uid,
                      temp_fillna=-9999):
    ''' returns pandas dataframe
        after checking for conflicting values in conflict_cols
        and adding new unique ids if there are conflicts
    '''
    out_df = pd.DataFrame()

    df.reset_index(drop=True, inplace=True)
    df.fillna(temp_fillna, inplace=True)

    group_df = df.groupby(id_cols, as_index=False)

    for key, group in group_df:
        group = group.reset_index(drop=True)
        conflicts = 0

        for col in conflict_cols:
            non_nan = group[col]
            non_nan = non_nan[(non_nan != -999) &
                              (non_nan != temp_fillna)]
            if len(non_nan.unique()) > 1:
                conflicts += 1
        if conflicts == 0:
            group.insert(0, uid, starting_uid + 1)
            starting_uid += 1
        else:
            group.insert(0, uid, starting_uid + group.index + 1)
            starting_uid += max(group.index + 1)
        out_df = out_df.append(group)

    out_df[out_df == temp_fillna] = np.nan
    return out_df


def assign_unique_ids(df, uid, id_cols, conflict_cols=[]):
    ''' returns pandas dataframe
        with unique ids assigned to rows which share id_cols values
        and lack conflicting information in the conflict_cols
    '''
    dfu = df[id_cols + conflict_cols].drop_duplicates()
    dfu.reset_index(drop=True, inplace=True)
    if conflict_cols:
        rd_df = remove_duplicates(dfu, id_cols).reset_index(drop=True)
        rd_df.insert(0, uid, rd_df.index + 1)

        kd_df = keep_duplicates(dfu, id_cols).reset_index(drop=True)
        rc_df = resolve_conflicts(kd_df, id_cols, conflict_cols,
                                  uid=uid, starting_uid=max(rd_df[uid]))
        rc_df = rd_df.append(rc_df)
        df = df.merge(rc_df,
                      on=id_cols + conflict_cols,
                      how='left')

        assert df[df[uid].isnull()].shape[0] == 0,\
            print('Some unique IDs are null')
        assert max(df[uid]) == len(df[uid].drop_duplicates()),\
            print('Unique IDs are not correctly scaled')
    else:
        dfu[uid] = dfu.index + 1
        df = df.merge(dfu, on=id_cols, how='left')

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
