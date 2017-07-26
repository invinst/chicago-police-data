import pandas as pd

def RemoveDuplicates(df, cols=[]):
    if not cols: cols = df.columns.tolist()
    return df[~ df.duplicated(subset = cols, keep=False)]

def KeepDuplicates(df, cols):
    return df[df.duplicated(subset = cols, keep=False)].sort_values(cols)

def AssignUniqueIDs(df, id_cols, uid):
    dfu = (df[id_cols]
        .drop_duplicates()
        .reset_index(drop=True))
    dfu[uid] = dfu.index + 1
    return df.merge(dfu, on = id_cols, how = 'left')

def AggregateData(df, uid, id_cols = [], mode_cols = [], max_cols = [], current_cols = [], time_col = []):
    if uid not in df.columns:
        df = AssignUniqueIDs(df, id_cols, uid)
    
    uid_col = [uid]
    agg_df = df[uid_col + id_cols].drop_duplicates().reset_index(drop=True) 
    if max_cols:
        agg_df = agg_df.merge((df[uid_col + max_cols]
                                .drop_duplicates()
                                .groupby(uid_col, as_index=False)[max_cols]
                                .agg(max)),
                            on = uid,
                            how = 'left')
    '''if mode_full:
        mode_df = df[uid_col + mode_cols]
    else:
        mode_df = dfu[uid_col + mode_cols]
    agg_df = agg_df.merge((mode_df
                            .groupby(uid_col, as_index=False)[mode_cols]
                            .agg(mode)'''
    if current_cols and time_col:
        df[time_col] = pd.to_datetime(df[time_col])
        agg_df = agg_df.merge((df[uid_col + [time_col] + current_cols]
                                .dropna(axis=0, subset=[time_col])
                                .drop_duplicates()
                                .sort_values(time_col, ascending = False)
                                .drop(time_col, axis=1)
                                .groupby(uid_col, as_index=False)[current_cols]
                                .agg(lambda x: x.iloc[0])),
                        on = uid,
                        how = 'left')
        agg_df.rename(columns = dict(zip(current_cols, ["Current" + tc for tc in current_cols])), inplace=True)
    return agg_df 
