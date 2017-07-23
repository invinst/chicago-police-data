import pandas as pd

def RemoveDuplicates(df, cols):
    return df[~ df.duplicated(subset = cols, keep=False)]

def KeepDuplicates(df, cols):
    return df[df.duplicated(subset = cols, keep=False)].sort_values(cols)

def AssignUniqueIDs(df, id_cols, uid):
    dfu = (df[id_cols]
        .drop_duplicates()
        .reset_index(drop=True))
    dfu['TID'] = dfu.index + 1
    return df.merge(dfu, on = id_cols, how = 'left')

def AggregateData(df, uid, id_cols = [], mode_cols = [], max_cols = [], time_dict = {}):
    dfu = df[id_cols + mode_cols + max_cols].drop_duplicates()
    dfu = AssignUniqueIDs(dfu, id_cols, uid)

    uid_col = [uid]
    df[uid_col + max_cols].group_by(uid_col, as_index=False)[max_cols].agg(max)

