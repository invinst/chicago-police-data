import pandas as pd

def RemoveDuplicates(df, cols):
    return df[~ df.duplicated(subset = cols, keep=False)]

def KeepDuplicates(df, cols):
    return df[df.duplicated(subset = cols, keep=False)].sort_values(cols)

def AssignUniqueIDs(df, id_cols):
    dfu = (df[id_cols]
        .drop_duplicates()
        .reset_index(drop=True))
    dfu['TID'] = dfu.index + 1
    return df.merge(dfu, on = id_cols, how = 'left')

def ModeAggregate(df, id_col, keep_cols):
    groups = df[[id_col] + keep_cols].groupby(id_col)
    dfu = pd.concat(
                [(g.agg(lambda x: x.value_counts(dropna=False).idxmax())
                    .to_frame()
                    .transpose())
                for n,g in groups],
                axis = 0)
    return dfu
