import pandas as pd
import re

def CleanGender(g, gd = {'M':'MALE', 'F':'FEMALE', 'X':'UNKNOWN'}):
    if g in gd.values():
        return g
    else:
        return gd[g]

def CleanRace(r, rd = {'WHI':'WHITE', 'BLK':'BLACK', 'S':'WHITE', 'API':'API', 'I':'OTHER', 'U':'UNKNOWN', 'WWH':'HISPANIC', 'ASIAN/PACIFIC ISLANDER' : 'API', 'BLACK HISPANIC': 'BLACK', 'WHITE HISPANIC' : 'HISPANIC', 'AMER IND/ALASKAN NATIVE' : 'OTHER'}):
    if r in rd.values():
        return r
    else:
        return rd[r]

def CleanDates(df):
    df_cols = df.columns.values
    dt_df = pd.DataFrame()
    for col in df_cols:
        col_suffix = col.split('.')[0]
        dt_df[col_suffix + ".Date"] = pd.to_datetime(df[col]).dt.date
        if 'time' in col:
            dt_df[col_suffix + '.Time'] = pd.to_datetime(df[col]).dt.time
    return dt_df

def ExtractSuffixName(x):
    suffixes = ('II', 'III', 'IV', 'JR', 'SR')
    suffix = [w for w in x.split(" ") if w in suffixes]
    return suffix[0] if suffix else ""

def ExtractMiddleInitial(x):
    xs = x.split(' ')
    if len(xs) > 1 and len(xs[0]) == 1 and len(xs[1]) > 1:
        return xs[0]
    else:
        return ""

def StripName(x):
    x = re.sub(r'[^\w\s]', '', x)
    return ' '.join(x.split())

def CleanLastName(x):
    x = StripName(x)
    suffix = ExtractSuffixName(x)
    x = x.replace(suffix, "")

    return [''.join(x.split()), suffix]

def CleanFirstName(x):
    x = StripName(x)
    MI = ExtractMiddleInitial(x)
    return [''.join(x.replace(MI+" ", "").split()), MI]

def FullNameSplit(names, ln='Last.Name', fn='First.Name'):
    return (pd.DataFrame(names
                        .fillna(",")
                        .map(lambda x: x if re.search('[a-zA-Z]', x) else ",")
                        .map(lambda x: x.rsplit(',',1))
                        .values.tolist(),
                     columns = [ln, fn]))

def LastNameSplit(names, ln='Last.Name', sn='Suffix.Name'):
    return (pd.DataFrame(names
                        .map(str.upper)
                        .map(CleanLastName).values.tolist(),
                    columns = [ln, sn]))

def FirstNameSplit(names, fn='First.Name', mi='Middle.Initial'):
    return (pd.DataFrame(names
                        .map(str.upper)
                        .map(CleanFirstName).values.tolist(),
                        columns = [fn, mi]))
def CleanNames(df):
    df_cols = df.columns.values
    if 'Full.Name' in df_cols and 'Last.Name' not in df_cols: 
        LN_FN = FullNameSplit(df['Full.Name'])
        LN = LastNameSplit(LN_FN['Last.Name'])
        FN = FirstNameSplit(LN_FN['First.Name'])
    else:
        df = df.fillna("")
        LN = LastNameSplit(df['Last.Name'])
        FN = FirstNameSplit(df['First.Name'])
    
    new_df = LN.join(FN)
    
    if 'Middle.Initial' in df_cols: 
        print('Middle Initial Conflicts:', new_df.ix[(df['Middle.Initial'] != '') & (new_df['Middle.Initial'] != '')])
        new_df.ix[df['Middle.Initial'] != '','Middle.Initial'] = df.ix[df['Middle.Initial'] != '', 'Middle.Initial']
    if 'Suffix.Name' in df_cols: 
        print('Suffix Name Conflicts:', new_df.ix[(df['Suffix.Name'] != '') & (new_df['Suffix.Name'] != '')])
        new_df.ix[df['Suffix.Name'] != '','Suffix.Name'] = df.ix[df['Suffix.Name'] != '', 'Suffix.Name']

    return new_df

def CleanData(df, skip_cols = []):
    col_df = pd.read_csv('Column_Dictionary.csv')
    name_cols = col_df.loc[col_df["Type"]=='Name', 'Column'].tolist()
    int_cols = col_df.loc[col_df["Type"]=='Int', 'Column'].tolist()
    df_cols = df.columns.values
    
    if 'Gender' in df_cols and 'Gender' not in skip_cols:
        df['Gender'] = df['Gender'].apply(str.upper).apply(CleanGender)
    
    if 'Race' in df_cols and 'Race' not in skip_cols:
        df['Race'] = df['Race'].apply(str.upper).apply(CleanRace)
    
    for col in [IC for IC in df_cols if IC in int_cols and IC not in skip_cols]:
        # Doesn't work... not sure why
        df[col] = pd.to_numeric(df[col], errors = 'coerce', downcast = 'integer')
    
    if [col for col in df_cols if 'Date' in col]:
        dt_df = df[[DC for DC in df_cols if 'Date' in  DC]]
        df = df[list(set(df.columns.values) - set(dt_df.columns.values))].join(CleanDates(dt_df))
    
    if [col for col in df_cols if col in name_cols]:
        name_df = df[[col for col in df_cols if col in name_cols]]
        df = df[list(set(df.columns.values) - set(name_df.columns.values))].join(CleanNames(name_df))
    
    return df
