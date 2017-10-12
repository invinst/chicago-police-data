#! usr/bin/env python3
#
# Author:   Roman Rivera

'''functions used in the cleaning step'''

import re
import yaml
import numpy as np
import pandas as pd


def list_diff(list1, list2):
    '''returns list after taking set difference of two lists
    '''
    return list(set(list1) - set(list2))


def intersect(list1, list2):
    '''returns list after taking set intersection of two lists'''
    return list(set(list1) & set(list2))


def clean_int(integer,
              upper=0, lower=0, inclusive=True,
              na_value=np.nan):
    '''returns an integer from an object if possible,
       else returns an na_value

       >>> clean_int("12.09")
       12
       >>> clean_int("ab21")
       nan
       >>> clean_int(20.0)
       20
       >>> clean_int("-267.12571")
       -267
       >>> clean_int("-1246")
       -1246
       >>> clean_int(-9126, 100, 0)
       nan
       >>> clean_int("26.06", 100, 0)
       26
    '''
    # Either get integer to int form or return na_value
    if isinstance(integer, str):
        if '.' in integer:
            # Check to see if it the string may be a float
            try:
                integer = int(float(integer))
            except ValueError:
                return na_value
        else:
            try:
                integer = int(integer)
            except ValueError:
                return na_value

    elif np.isfinite(integer):
        integer = int(float(integer))

    else:
        return na_value

    # If upper and lower bounds are actually bounds
    # then correct integer properly
    if upper > lower and inclusive:
        return integer if lower <= integer <= upper else na_value
    elif upper > lower:
        return integer if lower < integer < upper else na_value
    else:
        return integer


def clean_date_df(df):
    '''returns pandas dataframe of cleaned date and time columns
       splits datetime columns into date and time,
       ensures any errors are returned as null.
    '''
    # Store column names
    # All columns must end with .Date or .Datetime
    df_cols = df.columns.values  # Store column names
    dt_df = pd.DataFrame()  # Create empty dataframe

    # Iterate over stored date(time) columns
    for col in df_cols:
        # Store column suffix as list, removing .Date(time) ending
        # 'Org_Hire_Datetime' -> ['Org', 'Hire']
        col_suffix = col.split('_')[:-1]

        # Initial date and time column names
        date_name = '_'.join(col_suffix + ['date'])
        time_name = '_'.join(col_suffix + ['time'])
        # Try to convert column to datetime
        # And create column in dt_df ending with .Date
        try:
            dt_df[date_name] = \
                pd.to_datetime(df[col], errors='raise').dt.date
        # If there were errors, notify the user,
        # And repeat above but with coercing errors to NaT
        except:
            print('Some errors in {}. Returned as NaT.'.format(col))
            dt_df[date_name] = \
                pd.to_datetime(df[col], errors='coerce').dt.date

        # Ensure dates are not past the current date
        today = pd.to_datetime('today').date()  # Get current date
        # If date is after the current date, subtract 100 from the year
        # Common problem where 01/01/55 -> 2055-01-01 when should be 1955
        dt_df[date_name] = \
            dt_df[date_name].map(lambda x:
                                 x.replace(year=x.year-100)
                                 if (pd.notnull(x) and x >= today)
                                 else x)

        # If time is in column, repeat above
        # Except convert to time not date
        if 'time' in col:
            try:
                dt_df[time_name] = \
                    pd.to_datetime(df[col]).dt.time
            except:
                print('Some errors in {}. Returned as NaT.'.format(col))
                dt_df[time_name] = \
                    pd.to_datetime(df[col], errors='coerce').dt.date

    # EX: df columns = ['Org_Hire_Datetime', 'Start_Date']
    #     dt_df columns = ['Org_Hire_Date', 'Org_Hire_Time', 'Start_Date']
    return dt_df


def full_strip_name(x):
    '''returns string after
       removing any redundant whitespace or punctuation from string
       >>> full_strip_name("KIM-TOY")
       'KIMTOY'
       >>> full_strip_name("LUQUE-.ROSALES")
       'LUQUEROSALES'
    '''
    x = re.sub(r'[^\w\s]', '', x)
    return ' '.join(x.split())


def basic_strip_name(x):
    '''returns string after
       removing any redundant whitespace or period from string
       >>> basic_strip_name("Mary-Ellen.")
       'Mary-Ellen'
       >>> basic_strip_name("     SADOWSKY,  J.R")
       'SADOWSKY JR'
    '''
    x = re.sub('\s\s+', ' ',
               re.sub(r'^[ \s]*|\.|\,|\s$', '', x))
    return x


def split_full_names(names, ln='last_name', fn='first_name'):
    '''returns pandas dataframe of last and first name columns
       made from splitting input pandas series of full names.
       See test_clean_functions.py for testing and more details.
    '''
    # Fill NaN names with ',' for ease of use
    names = names.fillna(",")
    # Fill names that do not contain letter ('-') as ',' for ease of use
    names = names.map(lambda x: x if re.search('[a-zA-Z]', x) else ",")
    # Split names after last comma into list of length = 2
    # Ex: 'Jones, JR, Bob' -> ['Jones, JR', 'Bob']
    names = names.map(lambda x: x.rsplit(',', 1))
    # Turn series of lists into dataframe
    # with last and first name columns
    names = pd.DataFrame(names.values.tolist(),
                         columns=[ln, fn])
    return names


def extract_suffix_name(x, suffixes):
    '''returns the suffix name in a given string
       >>> extract_suffix_name('BURKE SR J', suffixes=['SR'])
       ('BURKE J', 'SR')
       >>> extract_suffix_name('NORWOOD II', suffixes=['II'])
       ('NORWOOD', 'II')
       >>> extract_suffix_name('SAUL K JR', suffixes=['JR'])
       ('SAUL K', 'JR')
       >>> extract_suffix_name('JONES V', suffixes=['V'])
       ('JONES', 'V')
       >>> extract_suffix_name('MICHAEL V', suffixes=['JR', 'SR'])
       ('MICHAEL V', '')
    '''
    # Split input name (x) by spaces and look for suffix in name pieces
    suffix = [w for w in x.split(" ") if w in suffixes]
    # If any suffix found, return it, else return empty string
    assert len(suffix) < 2, 'Too many suffixes found {}'.format(suffix)
    # If suffix is not empty and the name does not start with a 1 letter suffix
    if (suffix and
        (len(suffix) != 1 or
         suffix != x.split(" ")[0])):
        # Remove suffix from name
        x = re.sub('(^{0}\s)|(\s{0}$)|(\s{0}\s)'.format(suffix[0]),
                   ' ', x)
        x = basic_strip_name(x)
        # Remove white space from suffix
        suffix = suffix[0].replace(' ', '')
    else:
        # Fill suffix with empty string
        suffix = ''

    # If name is empty after suffix removed
    # EX: first name = JR
    if not x:
        x = suffix    # Re-assign suffix as name
        suffix = ''   # Replace suffix with empty string
    return basic_strip_name(x), suffix


def extract_middle_initial(x, mi_pattern='(\s[A-Z]\s)|(\s[A-Z]$)',
                           not_pattern='^((DE LA)\s[A-Z]($|\s))',
                           avoid_suffixes=[]):
    '''returns the middle initial in a given string
       >>> extract_middle_initial('BURKE SR J')
       ('BURKE SR', 'J')
       >>> extract_middle_initial('DE LA O JR')
       ('DE LA O JR', '')
       >>> extract_middle_initial('SAUL V')
       ('SAUL', 'V')
       >>> extract_middle_initial('BUSH V', avoid_suffixes=['V', 'I'])
       ('BUSH V', '')
       >>> extract_middle_initial('ERIN E M JR')
       ('ERIN M JR', 'E')
       >>> extract_middle_initial('A RICHARD V')
       ('A RICHARD', 'V')
    '''
    # Search input name for middle initial based on regex pattern
    mi = re.search(mi_pattern, x)
    # Assign identified middle initial if it is found
    # And if the name is too short (Ex: A J, J R)
    # Otherwise mi is assigned as an empty string
    mi = mi.group() if mi and len(x) > 3 else ''
    # If middle initial is not empty
    # And name does not have the not patterns (DE LA O)
    # And the identified middle initial is not in the suffixes
    if (mi and
        not ((not_pattern and
              re.search(not_pattern, x)) or
             mi[-1] in avoid_suffixes)):
        # Remove the middle initial from the name
        x = re.sub(mi_pattern, ' ', x)
        # Remove spaces in middle initial
        mi = mi.replace(' ', '')
    else:
        # Assign middle initial as empty string
        mi = ''
    return basic_strip_name(x), mi


def split_name(x, suffixes,
               not_pattern='^((DE LA)\s[A-Z]($|\s))',
               mi_pattern='(\s[A-Z]\s)|(\s[A-Z]$)'):
    '''returns list of name components
       the first element is no-spaced name,
       while the last element may contain spaces
       >>> split_name('DE LA O JR', ['II', 'III', 'IV', 'JR', 'SR', 'V', 'I'])
       ['DELAO', '', '', 'JR', 'DE LA O']
       >>> split_name('A RICHARD A', ['II', 'III', 'IV', 'JR', 'SR'], '')
       ['ARICHARD', 'A', '', '', 'A RICHARD']
       >>> split_name('J EDGAR V E JR', ['II', 'III', 'IV', 'JR', 'SR'])
       ['JEDGAR', 'V', 'E', 'JR', 'J EDGAR']
       >>> split_name('GEORGE H W', ['II', 'III', 'IV', 'JR', 'SR'])
       ['GEORGE', 'H', 'W', '', 'GEORGE']
    '''
    # Extract middle initial
    x, mi = extract_middle_initial(x, mi_pattern, not_pattern, suffixes)
    # Extract second middle initial
    x, mi2 = extract_middle_initial(x, mi_pattern, not_pattern, suffixes)
    # Extract suffix name
    x, suff = extract_suffix_name(x, suffixes)
    # If first initial is empty then fill it with first letter of x
    # Return list of fully stripped name (no spaces), middle initial,
    # middle initial 2, suffix, and basic stripped name
    out_list = [full_strip_name(x).replace(' ', ''),
                mi, mi2, suff,
                x]
    return out_list


def clean_name_col(col):
    '''returns pandas dataframe of name columns
       see test_clean_functions.py for tests/details.
    '''
    # Store name of input column
    col_name = col.name
    # Remove redundant spaces and periods
    col = col.map(basic_strip_name)
    # Ensure string is in uppercase
    col = col.map(str.upper)

    # If col is first or last name
    if col_name in ['first_name', 'last_name']:
        # Assign middle_initial pattern to be:
        # single letters between spaces or at end of string
        mi_pattern = '(\s[A-Z]\s)|(\s[A-Z]$)'
        # Choose regex pattern to avoid in middle initial selection:
        # If first name: no not_pattern
        # If last name: avoid DE LA _ pattern (or others added as needed)
        not_pattern = ('' if col_name == 'first_name'
                       else '^((DE LA)\s[A-Z]($|\s))')
        # Chooses suffixes to look for depending on column:
        # First Name columns probably do no contain V as a suffix
        # V may be mistaken as a middle initial
        # But Last Name columns might contain V or I as a suffix
        suffixes = ['II', 'III', 'IV', 'JR', 'SR']
        if col_name == 'last_name':
            suffixes.extend(['V', 'I'])
        # Return pandas dataframe created from list of lists
        # Comprised of first/last name (with no separation - NS),
        # middle initial, middle inital 2,
        # suffix name, and first/last name (with spaces or dashes).
        # Middle initial and suffix name elements are abbrivated
        # and which column they came from is idenified by F/L
        return pd.DataFrame([split_name(x, suffixes,
                                        not_pattern,
                                        mi_pattern)
                             for x in col],
                            columns=[col_name + '_NS',
                                     col_name[0] + '_MI',
                                     col_name[0] + '_MI2',
                                     col_name[0] + '_SN',
                                     col_name])

    # Else if column is a middle name column
    elif col_name == 'middle_name':
        # Return pandas dataframe of middle name
        # And first letter of middle name (middle initial)
        # If there is not middle name, return empty strings
        return pd.DataFrame([['', '']
                             if not (isinstance(x, str) and x)
                             else [x, x[0]]
                             for x in col],
                            columns=[col_name, 'middle_initial'])

    # If column is middle initial
    elif col_name == 'middle_initial':
        # Return middle initial column
        return col

    # If column not recognized, raise error
    else:
        raise Exception('Uh.. what sort of names?')


def compare_columns(df, colnames):
    '''returns dataframe of columns designated by colnames
       filled with non-empty values in original columns.
       see test_clean_functions.py for tests/details.
    '''
    output_list = []
    ncols = len(colnames)
    for i, row in df.iterrows():
        x = list(filter(bool, row))
        assert len(x) <= ncols, 'too many {}'.format(x)
        output_list.append(x)
    return pd.DataFrame(output_list, columns=colnames)


def clean_names(df):
    '''returns pandas dataframe of cleaned name columns
       see test_clean_functions.py for tests/details.
    '''
    df_cols = list(df.columns)  # Store column names
    df_rows = df.shape[0]   # Store full df shape
    # Insert df index as column in case of shuffling
    df.insert(0, 'Index', df.index)
    # Create name_df as unique rows in input df
    name_df = (df[df_cols].drop_duplicates()
               .copy()
               .reset_index(drop=True))
    # Give names an ID by index
    name_df.insert(0, 'ID', name_df.index)
    # Give ID to full data
    df = df.merge(name_df, on=df_cols, how='inner')
    # Ensure no rows were dropped
    assert df.shape[0] == df_rows,\
        'Lost rows when giving ID: {0} vs. {1}'.format(df_rows,
                                                       df.shape[0])
    # Then drop all columns except for ID
    df.drop(df_cols, axis=1, inplace=True)

    # If names are full names
    if 'full_name' in df_cols:
        # Split full names into last and first names
        name_df = split_full_names(name_df['full_name'])
    # Otherwise (generally first and last names)
    else:
        name_df = name_df.fillna("")  # Fill NAs with empty strings

    # Split first name column into first name, middle initial, suffix
    fn_df = clean_name_col(name_df['first_name'])
    # Split last name column into last name, middle initial, suffix
    ln_df = clean_name_col(name_df['last_name'])
    # Join name dataframes
    cleaned_df = fn_df.join(ln_df)

    # If suffix not in columns already (almost always)
    if 'suffix_name' not in df_cols:
        # Insert suffix name column of empty strings
        cleaned_df.insert(0, 'suffix_name', '')

    # If middle name is already in columns
    if 'middle_name' in df_cols:
        # Join cleaned middle name and initial columns to cleaned_df
        cleaned_df = cleaned_df.join(clean_name_col(name_df['middle_name']))
    # If middle initial is already in columns
    elif 'middle_initial' in df_cols:
        # Join cleaned middle initial column to cleaned_df
        cleaned_df = cleaned_df.join(clean_name_col(name_df['middle_initial']))
    # If no middle inital columns in original dataframe
    else:
        # Insert middle initial column of empty strings
        cleaned_df.insert(0, 'middle_initial', '')
    # Create middle initial dataframe for comparing
    mi_df = cleaned_df[['middle_initial', 'f_MI', 'f_MI2', 'l_MI', 'l_MI2']]
    mi_df = compare_columns(mi_df,
                            colnames=['middle_initial', 'middle_initial2'])

    # Create suffix name dataframe (suffix_name)
    sn_df = cleaned_df[['suffix_name', 'f_SN', 'l_SN']]
    sn_df = compare_columns(sn_df,
                            colnames=['suffix_name'])

    # Identify name columns which exist in cleaned_df
    name_cols = intersect(cleaned_df.columns,
                          ['first_name', 'last_name',
                           'first_name_NS', 'last_name_NS',
                           'middle_name'])
    # Remove middle initial and suffix columns from cleaned dataframe
    # Then joined cleaned dataframe to middle initial and suffix name dfs
    cleaned_df = cleaned_df[name_cols].join(mi_df).join(sn_df)
    cleaned_df.fillna('', inplace=True)

    # Expand cleaned_df to size of original df
    # by giving ID column (index)
    # then merging to df on ID and delete ID
    cleaned_df.insert(0, 'ID', cleaned_df.index)
    cleaned_df = cleaned_df.merge(df, on='ID', how='inner')
    del cleaned_df['ID']

    # Ensure row numbers are correct
    assert df.shape[0] == df_rows,\
        ('Lost rows when remerging: {0} vs. {1}'
         '').format(cleaned_df.shape[0],
                    df.shape[0])
    # Sort dataframe by original index
    # and delete index column
    cleaned_df = (cleaned_df
                  .sort_values('Index')
                  .reset_index(drop=True)
                  .drop('Index', axis=1))
    # Return cleaned names dataframe
    return cleaned_df


def clean_data(df, skip_cols=[]):
    '''returns pandas dataframe with all relevant columns
       cleaned in a standard format way
    '''
    # Initialize empty list for name columns
    name_cols = []

    # Load column reference yaml file as dictionary
    with open('hand/column_types.yaml', 'r') as file:
        col_dict = yaml.load(file)

    # Loop over col_dict column names and types
    for col_name, col_type in col_dict.items():
        # If column is designated to be skipped or is not in df columns
        # then do nothing
        if col_name in skip_cols or col_name not in df.columns:
            pass

        # If col_type is race or gender then format it using a race/gender_type
        # yaml reference file in the hand/ directory
        # and replace any values not in the dictionary with an empty string
        elif col_type in ['race', 'gender']:
            with open('hand/{}_types.yaml'.format(col_type), 'r') as file:
                type_dict = yaml.load(file)
            df[col_name] = df[col_name].str.upper().replace(type_dict)
            df.loc[~df[col_name].isin(type_dict.values()), col_name] = ''

        # If the col_type is a general int, then map clean_int onto it
        elif col_type == 'int':
            df[col_name] = df[col_name].map(clean_int)

        # If col_type is age, then map a special case of clean_int onto it
        # using a specified upper and lower bound
        elif col_type == 'age':
            def clean_age(age):
                return (
                    clean_int(age, upper=110, lower=1,
                              inclusive=True)
                       )
            df[col_name] = df[col_name].map(clean_age)

        # If col_type is name then append this column to the name_cols list
        elif col_type == 'name':
            name_cols.append(col_name)

        # If col_type is date or datetime
        elif col_type in ['date', 'datetime']:
            cleaned_date_df = clean_date_df(df[[col_name]])
            del df[col_name]
            df = df.join(cleaned_date_df)

    # Store input dataframe columns
    df_cols = df.columns.values

    # If there are any name columns
    if name_cols:
        # Create cleaned name dataframe from
        # input dataframe's selected name columns
        # and middle initial and suffix name conflicts
        cn_df = clean_names(df[name_cols])
        # Fill empty strings with NaN
        cn_df[cn_df == ''] = np.nan
        # Join input dataframe (minus name columns)
        # and the cleaned name dataframe
        df = df[list_diff(df.columns,
                          name_cols)].join(cn_df)

    df_cols = df.columns    # Store column names
    # Drop columns that are completely missing values
    df.dropna(axis=1, how='all', inplace=True)
    # Store columns dropped due to all missing values
    dropped_cols = list(set(df_cols) - set(df.columns))
    # Print out columns dropped due to missing values
    print(('Columns dropped due to '
           'all missing values:\n {}').format(dropped_cols))

    # Return cleaned dataframe
    return df


if __name__ == "__main__":
    import doctest
    doctest.testmod()
    doctest.run_docstring_examples(clean_int, globals())
    doctest.run_docstring_examples(full_strip_name, globals())
    doctest.run_docstring_examples(basic_strip_name, globals())
    doctest.run_docstring_examples(extract_suffix_name, globals())
    doctest.run_docstring_examples(extract_middle_initial, globals())
    doctest.run_docstring_examples(split_name, globals())
