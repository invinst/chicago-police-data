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
       >>> clean_int(-267.12571)
       -267
       >>> clean_int(-9126, 100, 0)
       nan
       >>> clean_int("26.06", 100, 0)
       26
    '''
    # Either get integer to int form or return na_value
    if isinstance(integer, str):
        # Check to see if it the string may be a float
        if re.search('^[0-9,.]*$', integer):
            integer = int(float(integer))
        # If not, return na_value
        else:
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
    '''
    x = re.sub(r'[^\w\s]', '', x)
    return ' '.join(x.split())


def basic_strip_name(x):
    '''returns string after
       removing any redundant whitespace or period from string
    '''
    x = re.sub('\s\s+', ' ',
               re.sub(r'^\s|\.|\,|\s$', '', x))
    return x


def split_full_names(names, ln='Last.Name', fn='First.Name'):
    '''returns pandas dataframe of last and first name columns
       made from splitting input pandas series of full names
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
    '''returns the suffix name in a given string'''
    # Split input name (x) by spaces and look for suffix in name pieces
    suffix = [w for w in x.split(" ") if w in suffixes]
    # If any suffix found, return it, else return empty string
    return suffix[0] if suffix else ""


def extract_middle_initial(x, mi_pattern, suffixes):
    '''returns the middle initial in a given string'''
    # Search input name for middle initial based on regex pattern
    mi = re.search(mi_pattern, x)
    # Assign identified middle initial if it is found
    # And if the name is too short (Ex: A J, J R)
    # Otherwise mi is assigned as an empty string
    mi = mi.group() if mi and len(x) > 3 else ''

    # If middle initial is not empty
    # And name is not DE LA _ (middle initial is not valid)
    # And the identified middle initial is not in the suffixes
    if (mi and
        not (re.search('^DE LA\s[A-Z]$', x) or
             mi[1] in suffixes)):
        # Remove the middle initial from the name
        x = x.replace(mi, ' ')
        # Remove spaces in middle initial
        mi = mi.replace(' ', '')
    else:
        # Assign middle initial as empty string
        mi = ''
    return x, mi


def split_name(x, mi_pattern, suffixes):
    # Extract middle initial
    x, mi = extract_middle_initial(x, mi_pattern, suffixes)
    # Extract second middle initial
    x, mi2 = extract_middle_initial(x, mi_pattern, suffixes)
    # Extract suffix name
    suff = extract_suffix_name(x, suffixes)
    # If suffix is not empty
    if suff:
        # Remove suffix from name
        x = re.sub('(^{0}\s)|(\s{0}$)'.format(suff),
                   '', x)
        # Remove white space from suffix
        suff = suff.replace(' ', '')
    # If name is empty after suffix removed
    # EX: first name = JR
    if not x:
        x = suff    # Re-assign suffix as name
        suff = ''   # Replace suffix with empty string

    # Return list of fully stripped name (no spaces), middle initial,
    # middle initial 2, suffix, and basic stripped name
    return [full_strip_name(x).replace(' ', ''), mi,
            mi2, suff, basic_strip_name(x)]


def clean_name_col(col):
    # Store name of input column
    col_name = col.name
    # Remove redundant spaces and periods
    col = col.map(basic_strip_name)
    # Ensure string is in uppercase
    col = col.map(str.upper)

    # If col is First or Last Name
    if col_name in ['First.Name', 'Last.Name']:
        # Choose regex pattern for middle initial depending on column:
        # If First Name: single letters starting, ending, or middle of string
        # If Last Name: single letters ending or middle of string
        # EX: This avoids O Brien or D Angelo mistakes
        mi_pattern = ('(^[A-Z]\s)|(\s[A-Z])\s|\s[A-Z]$'
                      if col_name == 'First.Name'
                      else '(\s[A-Z])\s|\s[A-Z]$')
        # Chooses suffixes to look for depending on column:
        # First Name columns probably do no contain V as a suffix
        # V may be mistaken as a middle initial
        # But Last Name columns might contain V or I as a suffix
        suffixes = ['II', 'III', 'IV', 'JR', 'SR']
        if col_name == 'Last.Name':
            suffixes.extend(['V', 'I'])
        # Return pandas dataframe created from list of lists
        # Comprised of first/last name (with no separation - NS),
        # middle initial, middle inital 2,
        # suffix name, and first/last name (with spaces or dashes).
        # Middle initial and suffix name elements are abbrivated
        # and which column they came from is idenified by F/L
        return pd.DataFrame([split_name(x, mi_pattern, suffixes)
                             for x in col],
                            columns=[col_name + '_NS',
                                     col_name[0] + '_MI',
                                     col_name[0] + '_MI2',
                                     col_name[0] + '_SN',
                                     col_name])

    # Else if column is a middle name column
    elif col_name == 'Middle.Name':
        # Return pandas dataframe of middle name
        # And first letter of middle name (middle initial)
        # If there is not middle name, return empty strings
        return pd.DataFrame([['', '']
                             if not (isinstance(x, str) and x)
                             else [x, x[0]]
                             for x in col],
                            columns=[col_name, 'Middle.Initial'])

    # If column is middle initial
    elif col_name == 'Middle.Initial':
        # Return middle initial column
        return col

    # If column not recognized, raise error
    else:
        raise Exception('Uh.. what sort of names?')


# Is this necessary? and if it is, is there a better way of doing it?
def compare_middle_initials(mi_df):
    '''returns pandas dataframe and list of conflicts'''
    output_list = []    # Initialize output list
    conflict_indexes = []   # Initialize conflict index list
    # Iterate over the the middle initial columns
    for i, v, w, x, y, z in mi_df[['Middle.Initial',
                                   'F_MI', 'F_MI2',
                                   'L_MI', 'L_MI2']].itertuples():
        # Initialize (unique) set of middle initials
        mis = set((v, w, x, y, z))
        # Remove all empty strings
        mis.discard('')
        # If the set is empty (all middle initials were empty)
        if not mis:
            # Add tuple of two  empty strings to output
            output_list.append(('', ''))
        # If there is only 1 real middle initial in columns
        elif len(mis) == 1:
            # Add first (only) middle initial and empty string to output
            output_list.append((mis.pop(), ''))
        # If there are two real middle initials
        elif len(mis) == 2:
            # Append both (since the values are in order of importance)
            output_list.append((mis))
        # Else (if there are 3 unique values)
        else:
            # Add this index to the conflict list
            conflict_indexes.append(i)
            # Add first and second middle initials to output
            output_list.append(tuple(mis)[:2])

    # Return dataframe of first (primary) middle initials
    # and secondary middle initials
    # and list of conflict indexes
    return (pd.DataFrame(output_list,
                         columns=['Middle.Initial',
                                  'Middle.Initial2']),
            conflict_indexes)


def compare_suffix_names(sn_df):
    '''returns pandas dataframe and list of conflicts'''
    output_list = []    # Initialize output list
    conflict_indexes = []   # Initialize conflict index list
    # Iterate over the the suffix name columns
    # In order of importance any given suffix name column takes priority
    # (or it is totally empty), then come suffix names found in last names
    # And lastly suffix names found in first names (rare)
    for i, x, y, z in sn_df[['Suffix.Name', 'L_SN', 'F_SN']].itertuples():
        # Initialize (unique) set of suffix names
        sns = set((x, y, z))
        # Remove all empty strings
        sns.discard('')
        # If the set is empty (all suffix names were empty)
        if not sns:
            # Add empty string to output
            output_list.append('')
        # If there is only 1 real suffix name in columns
        elif len(sns) == 1:
            output_list.append(sns.pop())
        # If there are more than 1 unique suffixes
        else:
            # Add index to conflicts list
            conflict_indexes.append(i)
            # Append first element to output
            output_list.append(sns.pop())
    # Return pandas dataframe of 1 Suffix Name column
    # and list of conflict indexes
    return (pd.DataFrame(output_list,
                         columns=['Suffix.Name']),
            conflict_indexes)


def clean_names(df):
    '''returns pandas dataframe of cleaned name columns'''
    df_cols = df.columns.values  # Store column names
    # If names are Full.Names
    if 'Full.Name' in df_cols:
        # Split full names into last and first names
        name_df = split_full_names(df['Full.Name'])
    # Otherwise (generally first and last names)
    else:
        name_df = df.fillna("")  # Fill NAs with empty strings

    # Split first name column into first name, middle initial, suffix
    ## natural language processing for name parsing english
    fn_df = clean_name_col(name_df['First.Name'])
    # Split last name column into last name, middle initial, suffix
    ln_df = clean_name_col(name_df['Last.Name'])
    # Join name dataframes
    cleaned_df = fn_df.join(ln_df)

    # If suffix not in columns already (almost always)
    if 'Suffix.Name' not in df_cols:
        # Insert suffix name column of empty strings
        cleaned_df.insert(0, 'Suffix.Name', '')

    # If middle name is already in columns
    if 'Middle.Name' in df_cols:
        # Join cleaned middle name and initial columns to cleaned_df
        cleaned_df = cleaned_df.join(clean_name_col(name_df['Middle.Name']))
    # If middle initial is already in columns
    elif 'Middle.Initial' in df_cols:
        # Join cleaned middle initial column to cleaned_df
        cleaned_df = cleaned_df.join(clean_name_col(name_df['Middle.Initial']))
    # If no middle inital columns in original dataframe
    else:
        # Insert middle initial column of empty strings
        cleaned_df.insert(0, 'Middle.Initial', '')
    # Create middle initial dataframe (Middle.Initial, Middle.Initial2)
    # And collect indexes of conflicting middle initials
    mi_cols = ['Middle.Initial', 'F_MI', 'F_MI2', 'L_MI', 'L_MI2']
    mi_df, mi_conflicts = compare_middle_initials(cleaned_df[mi_cols])
    # Create suffix name dataframe (Suffix.Name)
    # And collect indexes of conflicting suffix names
    sn_cols = ['Suffix.Name', 'F_SN', 'L_SN']
    sn_df, sn_conflicts = compare_suffix_names(cleaned_df[sn_cols])

    # Identify name columns which exist in cleaned_df
    name_cols = intersect(cleaned_df.columns,
                          ['First.Name', 'Last.Name',
                           'Last.Name_NS', 'First.Name_NS',
                           'Middle.Name'])
    # Remove middle initial and suffix columns from cleaned dataframe
    # Then joined cleaned dataframe to middle initial and suffix name dfs
    cleaned_df = cleaned_df[name_cols].join(mi_df).join(sn_df)

    # Return tuple of cleaned names dataframe and conflict indexes
    return (cleaned_df, mi_conflicts + sn_conflicts)


def clean_data(df, skip_cols=[]):
    '''returns pandas dataframe with all relevant columns
       cleaned in a standard format way
       returns tuple if name columns are present
       second item contains conflicting name dataframe
    '''
    # Initialize empty list for name columns
    name_cols = []

    # Load column reference yaml file as dictionary
    with open('hand/column_types.yaml', 'r') as file:
        col_dict = yaml.load(file)

    # Loop over col_dict column names and types
    for col_name, col_type in col_dict.items():
        # If column is designated to be skipped, do nothing
        if col_name in skip_cols:
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
            print(cleaned_date_df.head())
            del df[col_name]
            df = df.join(cleaned_date_df)
            print(df.head())
    # Store input dataframe columns
    df_cols = df.columns.values

    raise Exception
    # If there are any name columns
    if name_cols:
        # Create cleaned name dataframe from
        # input dataframe's selected name columns
        # and middle initial and suffix name conflicts
        cn_df, conflicts = clean_names(df[name_cols])
        # Fill empty strings with NaN
        cn_df[cn_df == ''] = np.nan
        # Create conflicts dataframe for output
        conflicts_df = df[name_cols].ix[conflicts]
        # Join input dataframe (minus name columns)
        # and the cleaned name dataframe
        df = df[list_diff(df.columns,
                          name_cols)].join(cn_df)

        # Print out conflicts dataframe
        if conflicts_df.empty:
            print('No middle initial/suffix name conflicts.')
        else:
            print('Conflicting middle initial/suffix name conflicts:')
            print(conflicts_df)

    df_cols = df.columns    # Store column names
    # Drop columns that are completely missing values
    df.dropna(axis=1, how='all', inplace=True)
    # Store columns dropped due to all missing values
    dropped_cols = list(set(df_cols) - set(df.columns))
    # Print out columns dropped due to missing values
    print(('Columns dropped due to '
           'all missing values:\n {}').format(dropped_cols))

    # If names were formatted
    if name_cols:
        # Return cleaned dataframe and conflicts df tuple
        return df, conflicts_df
    # If names were not formatted
    else:
        # Return just cleaned dataframe
        return df


if __name__ == "__main__":
    import doctest
    doctest.testmod()
