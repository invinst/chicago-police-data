import pandas as pd

def RemoveDuplicates(df, cols=[]):
    if not cols:
        cols = df.columns.tolist()
    return df[~ df.duplicated(subset = cols, keep=False)]

def LoopMerge(df1, df2, on_list, keep_columns = ['ID1','ID2']):
    dfm = pd.DataFrame(columns= keep_columns + ['Matched.On'])
    # Generate on_list?
    for mc in on_list:
        df1t = RemoveDuplicates(df1[keep_columns[:1] + mc], mc)
        df2t = RemoveDuplicates(df2[keep_columns[1:] + mc], mc)

        dfmt = df1t.merge(df2t, on=mc, how='inner')

        if dfmt.shape[0]:
            dfmt['Matched.On'] = '-'.join(mc)
            dfm = dfm.append(dfmt[keep_columns + ['Matched.On']].reset_index(drop=True))
            
            df1 = df1.loc[~df1[keep_columns[0]].isin(dfm[keep_columns[0]])]
            df2 = df2.loc[~df2[keep_columns[1]].isin(dfm[keep_columns[1]])]

    return (dfm.reset_index(drop=True), df1, df2)
