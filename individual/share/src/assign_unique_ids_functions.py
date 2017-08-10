import pandas as pd


def remove_duplicates(df, cols=[]):
    if not cols:
        cols = df.columns.tolist()
    return df[~df.duplicated(subset=cols, keep=False)].sort_values(cols)


def keep_duplicates(df, cols):
    return df[df.duplicated(subset=cols, keep=False)].sort_values(cols)


def assign_unique_ids(df, uid, id_cols):
    dfu = df[id_cols].drop_duplicates()
    dfu.reset_index(drop=True, inplace=True)
    dfu[uid] = dfu.index + 1
    return df.merge(dfu, on=id_cols, how='left')


def max_aggregate(df, id_cols, max_cols):
    df = df.drop_duplicates()
    df = df.groupby(id_cols, as_index=False)[max_cols]
    return df.agg(max)


def order_aggregate(df, id_cols,
                    agg_cols, order_cols,
                    minimum=False):
    df = df.dropna(axis=0, subset=order_cols)
    df = df.drop_duplicates()
    df.sort_values(order_cols, ascending=minimum, inplace=True)
    df.drop(order_cols, axis=1, inplace=True)
    df = df.groupby(id_cols, as_index=False)[agg_cols]
    return df.agg(lambda x: x.iloc[0])


def aggregate_data(df, uid, id_cols=[],
                   mode_cols=[], max_cols=[],
                   current_cols=[], time_col="",
                   make_uid=True):
    uid_col = [uid]
    agg_df = df[uid_col + id_cols].drop_duplicates()
    agg_df.reset_index(drop=True, inplace=True)

    if max_cols:
        agg_df = agg_df.merge(
                        max_aggregate(
                            df[uid_col + max_cols],
                            uid_col, max_cols),
                        on=uid, how='left')

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
