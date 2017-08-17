import re
import numpy as np
import pandas as pd

gender_df = pd.read_csv('hand/gender_dictionary.csv')
gender_dict = dict(zip(gender_df.Original, gender_df.Standard))

race_df = pd.read_csv('hand/race_dictionary.csv')
race_dict = dict(zip(race_df.Original, race_df.Standard))


def list_diff(l1, l2):
    return list(set(l1) - set(l2))


def clean_int(x, na_value=-999):
    if isinstance(x, str):
        if re.search('^[0-9,.]*$', x):
            return int(float(x))
        else:
            return na_value
    elif np.isfinite(x):
        return int(float(x))
    else:
        return na_value


def clean_gender(x):
    if isinstance(x, str):
        x = x.upper()
        if x in gender_dict.values():
            return x
        else:
            return gender_dict[x]
    else:
        return gender_dict['NAN']


def clean_race(x):
    if isinstance(x, str):
        x = x.upper()
        if x in race_dict.values():
            return x
        else:
            return race_dict[x]
    else:
        return race_dict['NAN']


def clean_dates(df):
    df_cols = df.columns.values
    dt_df = pd.DataFrame()
    for col in df_cols:
        col_suffix = col.split('.')[:-1]
        try:
            dt_df['.'.join(col_suffix + ["Date"])] = \
            pd.to_datetime(df[col]).dt.date
        except:
            print('Some errors in {}. Returned as NaT.'.format(col))
            dt_df['.'.join(col_suffix + ["Date"])] = \
            pd.to_datetime(df[col], errors='coerce').dt.date

        if 'time' in col:
            try:
                dt_df['.'.join(col_suffix + ["Time"])] = \
                pd.to_datetime(df[col]).dt.time
            except:
                print('Some errors in {}. Returned as NaT.'.format(col))
                dt_df['.'.join(col_suffix + ["Time"])] = \
                pd.to_datetime(df[col], errors='coerce').dt.date
    return dt_df


def extract_suffix_names(x):
    suffixes = ('II', 'III', 'IV', 'JR', 'SR')
    suffix = [w for w in x.split(" ") if w in suffixes]
    return suffix[0] if suffix else ""


def extract_middle_initial(x):
    xs = x.split(' ')
    if (len(xs) > 1 and
            len(xs[0]) == 1 and
            len(xs[1]) > 1):
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
    return [''.join(x.replace(mi + " ", "").split()), mi]


def split_full_names(names, ln='Last.Name', fn='First.Name'):
    names = names.fillna(",")
    names = names.map(lambda x: x if re.search('[a-zA-Z]', x) else ",")
    names = names.map(lambda x: x.rsplit(',', 1))
    names = pd.DataFrame(names.values.tolist(),
                         columns=[ln, fn])
    return names


def split_names(names, main_name, sub_name, clean_func):
    names = names.map(str.upper)
    names = names.map(clean_func)
    return pd.DataFrame(names.values.tolist(),
                        columns=[main_name, sub_name])


def clean_names(df):
    df_cols = df.columns.values
    if ('Full.Name' in df_cols and
            'Last.Name' not in df_cols):
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
        name_df.ix[df['Middle.Initial'] != '', 'Middle.Initial'] = \
            df.ix[df['Middle.Initial'] != '', 'Middle.Initial']

    if 'Suffix.Name' in df_cols:
        print('Suffix Name Conflicts:',
              name_df.ix[(df['Suffix.Name'] != '') &
                         (name_df['Suffix.Name'] != '')])
        name_df.ix[df['Suffix.Name'] != '', 'Suffix.Name'] = \
            df.ix[df['Suffix.Name'] != '', 'Suffix.Name']

    name_df[name_df == ' '] = ''
    return name_df


def clean_data(df, skip_cols=[]):
    col_df = pd.read_csv('hand/column_dictionary.csv')
    name_cols = col_df.loc[col_df["Type"] == 'Name', 'Standard'].tolist()
    int_cols = col_df.loc[col_df["Type"] == 'Int', 'Standard'].tolist()
    df_cols = df.columns.values

    if 'Gender' in df_cols and 'Gender' not in skip_cols:
        df['Gender'] = df['Gender'].map(clean_gender)

    if 'Race' in df_cols and 'Race' not in skip_cols:
        df['Race'] = df['Race'].map(clean_race)

    for col in [ic for ic in df_cols
                if ic in int_cols and ic not in skip_cols]:
        df[col] = df[col].map(clean_int)

    if [col for col in df_cols if 'Date' in col]:
        dt_df = df[[dc for dc in df_cols if 'Date' in dc]]
        df = df[list_diff(df.columns,
                dt_df.columns)].join(clean_dates(dt_df))

    if [col for col in df_cols if col in name_cols]:
        name_df = df[[col for col in df_cols if col in name_cols]]
        df = df[list_diff(df.columns,
                name_df.columns)].join(clean_names(name_df))

    return df
