import pandas as pd
import re
import numpy as np

input_opts = {'compression' : 'gzip'}
output_opts = {'index' : False, 'compression' : 'gzip'}

def list_diff(l1,l2):
    return list(set(l1) - set(l2))

def clean_int(x, na_value = -999):
    if isinstance(x,str):
        if re.match('[a-zA-Z]',x):
            return na_value
        else:
            return int(float(x))
    elif np.isfinite(x):
        return int(float(x))
    else:
        return na_value
def clean_gender(x, gender_dict ={'M':'MALE', 'F':'FEMALE', 'X':'UNKNOWN'}):
    x = x.upper()
    if x in gender_dict.values():
        return x
    else:
        return gender_dict[x]

def clean_race(x, race_dict = {'WHI':'WHITE', 'BLK':'BLACK', 'S':'WHITE', 'API':'API', 'I':'OTHER', 'U':'UNKNOWN', 'WWH':'HISPANIC', 'ASIAN/PACIFIC ISLANDER' : 'API', 'BLACK HISPANIC': 'BLACK', 'WHITE HISPANIC' : 'HISPANIC', 'AMER IND/ALASKAN NATIVE' : 'OTHER'}):
    x = x.upper()
    if x in race_dict.values():
        return x
    else:
        return race_dict[x]

def clean_dates(df):
    df_cols = df.columns.values
    dt_df = pd.DataFrame()
    for col in df_cols:
        col_suffix = col.split('.')[:-1]
        dt_df['.'.join(col_suffix + ["Date"])] = pd.to_datetime(df[col]).dt.date
        if 'time' in col:
            dt_df['.'.join(col_suffix + ["Time"])] = pd.to_datetime(df[col]).dt.time
    return dt_df

def extract_suffix_names(x):
    suffixes = ('II', 'III', 'IV', 'JR', 'SR')
    suffix = [w for w in x.split(" ") if w in suffixes]
    return suffix[0] if suffix else ""

def extract_middle_initial(x):
    xs = x.split(' ')
    if len(xs) > 1 and len(xs[0]) == 1 and len(xs[1]) > 1:
        return xs[0]
    else:
        return ""

def strip_names(x):
    x = re.sub(r'[^\w\s]', '', x)
    return ' '.join(x.split())

def clean_last_names(x):
    x = strip_names(x)
    suffix = extract_suffix_names(x)
    x = x.replace(suffix, "")
    return [''.join(x.split()), suffix]

def clean_first_names(x):
    x = strip_names(x)
    mi = extract_middle_initial(x)
    return [''.join(x.replace(mi+" ", "").split()), mi]

def split_full_names(names, ln='Last.Name', fn='First.Name'):
    names.fillna(",", inplace=True)
    names = names.map(lambda x: x if re.search('[a-zA-Z]', x) else ",")
    names = names.map(lambda x: x.rsplit(',', 1))
    return pd.DataFrame(names.values.tolist(),
                            columns = [ln, fn])


def split_names(names, main_name, sub_name, clean_func):
    names = names.map(str.upper)
    names = names.map(clean_func)
    return pd.DataFrame(names.values.tolist(),
                        columns = [main_name, sub_name])


def clean_names(df):
    df_cols = df.columns.values
    if 'Full.Name' in df_cols and 'Last.Name' not in df_cols: 
        name_df = split_full_names(df['Full.Name'])
        last = split_names(name_df['Last.Name'],
                        'Last.Name', 'Suffix.Name',
                        clean_last_names)
        first = split_names(name_df['First.Name'], 
                        'First.Name', 'Middle.Initial', 
                        clean_first_names)
    else:
        df = df.fillna("")
        last = split_names(df['Last.Name'],
                        'Last.Name', 'Suffix.Name',
                        clean_last_names)
        first = split_names(df['First.Name'],
                            'First.Name', 'Middle.Initial',
                            clean_first_names)

    name_df = last.join(first)

    if 'Middle.Initial' in df_cols:
        print('Middle Initial Conflicts:',
                name_df.ix[(df['Middle.Initial'] != '') & 
                (name_df['Middle.Initial'] != '')])
        name_df.ix[df['Middle.Initial'] != '','Middle.Initial'] = \
        df.ix[df['Middle.Initial'] != '', 'Middle.Initial']

    if 'Suffix.Name' in df_cols:
        print('Suffix Name Conflicts:',
                name_df.ix[(df['Suffix.Name'] != '') & 
                (name_df['Suffix.Name'] != '')])
        name_df.ix[df['Suffix.Name'] != '','Suffix.Name'] = \
        df.ix[df['Suffix.Name'] != '', 'Suffix.Name']

    return name_df

def clean_data(df, skip_cols = []):
    col_df = pd.read_csv('hand/column_dictionary.csv')
    name_cols = col_df.loc[col_df["Type"]=='Name', 'Column'].tolist()
    int_cols = col_df.loc[col_df["Type"]=='Int', 'Column'].tolist()
    df_cols = df.columns.values

    if 'Gender' in df_cols and 'Gender' not in skip_cols:
        df['Gender'] = df['Gender'].map(clean_gender)

    if 'Race' in df_cols and 'Race' not in skip_cols:
        df['Race'] = df['Race'].map(clean_race)

    for col in [ic for ic in df_cols
                if ic in int_cols and ic not in skip_cols]:
        df[col] = df[col].map(clean_int)

    if [col for col in df_cols if 'Date' in col]:
        dt_df = df[[dc for dc in df_cols if 'Date' in  dc]]
        df = df[list_diff(df.columns, dt_df.columns)].join(clean_dates(dt_df))

    if [col for col in df_cols if col in name_cols]:
        name_df = df[[col for col in df_cols if col in name_cols]]
        df = df[list_diff(df.columns, name_df.columns)].join(clean_names(name_df))

    return df
