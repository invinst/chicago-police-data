#! usr/bin/env python3
#
# Author:   Roman Rivera

'''functions used in the cleaning step'''

import re
import numpy as np
import pandas as pd

# Load gender and race reference dataframe
gender_df = pd.read_csv('hand/gender_dictionary.csv')
race_df = pd.read_csv('hand/race_dictionary.csv')

# Zip together reference dataframes into gender_ and race_ dictionaries
gender_dict = dict(zip(gender_df.Original, gender_df.Standard))
race_dict = dict(zip(race_df.Original, race_df.Standard))


def list_diff(list1, list2):
    '''returns list after taking set difference of two lists'''
    return list(set(list1) - set(list2))


def intersect(list1, list2):
    '''returns list after taking set intersection of two lists'''
    return list(set(list1) & set(list2))


def clean_int(integer, na_value=np.nan):
    '''returns an integer from an object if possible,
       else returns an na_value
    '''
    if isinstance(integer, str):
        # Check to see if it the string may be a float
        if re.search('^[0-9,.]*$', integer):
            return int(float(integer))
        # If not, return na_value
        else:
            return na_value
    elif np.isfinite(integer):
        return int(float(integer))
    else:
        return na_value


def standardize_gender(gender):
    '''returns a standardized gender string
       by passing input string into gender reference dictionary
    '''
    # Ensure input gender is string
    if isinstance(gender, str):
        gender = gender.upper()   # Change x to uppercase
        # Check if x is already standardized
        if gender in gender_dict.values():
            return gender
        # If not, pass x into gender dictionary for standardization
        else:
            return gender_dict[gender]
    # If not a string return 'NAN'
    else:
        return gender_dict['NAN']


def standardize_race(race):
    '''returns a standardized race string
       by passing input string into race reference dictionary
    '''
    # Ensure input race is string
    if isinstance(race, str):
        race = race.upper()   # Change race to uppercase
        # Check if race is already standardized
        if race in race_dict.values():
            return race
        # If not, pass race in race dictionary for standardization
        else:
            return race_dict[race]
    # If not a string return 'NAN'
    else:
        return race_dict['NAN']


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
        # 'Org.Hire.Datetime' -> ['Org', 'Hire']
        col_suffix = col.split('.')[:-1]
        # Try to convert column to datetime
        # And create column in dt_df ending with .Date
        try:
            dt_df['.'.join(col_suffix + ["Date"])] = \
                pd.to_datetime(df[col], errors='raise').dt.date
        # If there were errors, notify the user,
        # And repeat above but with coercing errors to NaT
        except:
            print('Some errors in {}. Returned as NaT.'.format(col))
            dt_df['.'.join(col_suffix + ["Date"])] = \
                pd.to_datetime(df[col], errors='coerce').dt.date

        # If time is in column, repeat above
        # Except convert to time not date
        if 'time' in col:
            try:
                dt_df['.'.join(col_suffix + ["Time"])] = \
                    pd.to_datetime(df[col]).dt.time
            except:
                print('Some errors in {}. Returned as NaT.'.format(col))
                dt_df['.'.join(col_suffix + ["Time"])] = \
                    pd.to_datetime(df[col], errors='coerce').dt.date

    # EX: df columns = ['Org.Hire.Datetime', 'Start.Date']
    #     dt_df columns = ['Org.Hire.Date', 'Org.Hire.Time', 'Start.Date']
    return dt_df


def strip_name(x):
    '''returns string after
       removing any redundant whitespace or punctuation from string
    '''
    x = re.sub(r'[^\w\s]', '', x)
    return ' '.join(x.split())


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


def extract_middle_initial(x, mi_pattern):
    '''returns the middle initial in a given string'''
    # Search input name for middle initial based on regex pattern
    mi = re.search(mi_pattern, x)
    # Assign identified middle initial if it is found
    # And if the name is too short (Ex: A J, J R)
    # Otherwise mi is assigned as an empty string
    mi = mi.group() if mi and len(x) > 3 else ''
    return mi


def split_name(x, mi_pattern, suffixes):
    # Extract middle initial
    mi = extract_middle_initial(x, mi_pattern)
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

    # Extract suffix name
    suff = extract_suffix_name(x, suffixes)
    # If suffix is not empty
    if suff:
        x = x.replace(suff, '')  # Remove suffix from name
        # Remove white space from suffix
        suff = suff.replace(' ', '')
    # If name is empty after suffix removed
    # EX: first name = JR
    if not x:
        x = suff    # Re-assign suffix as name
        suff = ''   # Replace suffix with empty string

    # Remove all empty space in name
    x = x.replace(' ', '')

    # Return list of cleaned name, middle initial, and suffix
    return [x, mi, suff]


def clean_name_col(col):
    # Store name of input column
    col_name = col.name
    # Remove redundant spaces and punctuation
    col = col.map(strip_name)
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
        # But Last Name columns might contain V as a suffix
        suffixes = ['II', 'III', 'IV', 'JR', 'SR']
        if col_name == 'Last.Name':
            suffixes.append('V')
        # Return pandas dataframe created from list of lists
        # Comprised of first/last name, middle initial, suffix name
        # middle initial and suffix name elements are abbrivated
        # And which column they came from is idenified by F/L
        return pd.DataFrame([split_name(x, mi_pattern, suffixes)
                             for x in col],
                            columns=[col_name,
                                     col_name[0] + '_MI',
                                     col_name[0] + '_SN'])

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


def compare_name_cols(df):
    '''compares two column pandas dataframe
       returns series of best values and list of conflicts
    '''
    conflicts = []  # Initialize conflicts list
    out_list = []   # Initialize output list

    # Loop over dataframe column
    # i = index, m = main column, s = sub column
    for i, m, s in df.itertuples():
        # If main and sub are both non empty strings
        if m and s:
            # Add this index to the list of conflicts
            conflicts.append(i)
            # Add main value to output list
            out_list.append(m)
        # If one or neither is empty
        else:
            # Add the max of the two to output list
            out_list.append(max(m, s))

    if conflicts:
        print(('{0} conflicts from {1} at indexes:'
               '\n{2}').format(df.columns[0],
                               df.columns[1],
                               str(conflicts)))

    # Return output list as series and conflict indexes
    return (pd.Series(out_list), conflicts)


def clean_names(df):
    '''returns pandas dataframe of cleaned name columns'''
    # Initialize all potential name columns
    name_cols = ['First.Name', 'Last.Name',
                 'Middle.Initial', 'Middle.Name',
                 'Suffix.Name']

    df_cols = df.columns.values  # Store column names
    # If names are Full.Names
    if 'Full.Name' in df_cols:
        # Split full names into last and first names
        name_df = split_full_names(df['Full.Name'])
    # Otherwise (generally first and last names)
    else:
        name_df = df.fillna("")  # Fill NAs with empty strings

    # Split first name column into first name, middle initial, suffix
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

    # Compare Middle Initial/Suffix Name column (either given or empty strings)
    # To the middle initials/suffixes found in first(last) name columns
    # Then compare those results to those found in last(first) name columns
    # Continually storing the conflicts
    conflicts = []
    for mc, sc in zip(['Middle.Initial'] * 2 + ['Suffix.Name'] * 2,
                      ['F_MI', 'L_MI', 'L_SN', 'F_SN']):
        cleaned_df[mc], new_conflicts = compare_name_cols(cleaned_df[[mc, sc]])
        conflicts.extend(new_conflicts)

    # Only keep columns specified in name_cols
    cleaned_df = cleaned_df[intersect(cleaned_df.columns,
                                      name_cols)]

    # Fill any blank spaces with empty strings
    cleaned_df[cleaned_df == ' '] = ''

    # Return cleaned names dataframe
    return cleaned_df, conflicts


def clean_data(df, skip_cols=[]):
    '''returns pandas dataframe with all relevant columns
       cleaned in a standard format way
    '''
    # Load column reference file as dataframe
    col_df = pd.read_csv('hand/column_dictionary.csv')
    # Collect all potential Name columns in list
    name_cols = col_df.loc[col_df["Type"] == 'Name', 'Standard'].tolist()
    # Collect all potential Integer columns in list
    int_cols = col_df.loc[col_df["Type"] == 'Int', 'Standard'].tolist()
    # Store input dataframe columns
    df_cols = df.columns.values

    # If input dataframe has a gender column
    # and it is not designated to be skipped
    if 'Gender' in df_cols and 'Gender' not in skip_cols:
        # Standardize the values in the gender column
        df['Gender'] = df['Gender'].map(standardize_gender)
    # If input dataframe has a race column
    # and it is not designated to be skipped
    if 'Race' in df_cols and 'Race' not in skip_cols:
        # Standardize the values in the race column
        df['Race'] = df['Race'].map(standardize_race)

    # Loop over columns in input dataframe
    for col in [ic for ic in df_cols
                # If they are in the integer column list
                # and not designated to be skipped
                if ic in int_cols and ic not in skip_cols]:
        # Clean the integer column values
        df[col] = df[col].map(clean_int)

    # Collect date(time) columns
    datetime_cols = [col for col in df_cols
                     # If Date in the column name
                     # and column is not designated to be skipped
                     if 'Date' in col and col not in skip_cols]
    # If there are any date(time) columns
    if datetime_cols:
        # Create cleaned date dataframe from
        # input dataframe's selected datetime columns
        cd_df = clean_date_df(df[datetime_cols])
        # Join input dataframe (minus datetime columns)
        # and the cleaned date dataframe
        df = df[list_diff(df.columns,
                          datetime_cols)].join(cd_df)

    # Collect name columns
    name_cols = [col for col in df_cols
                 # If column in name columns
                 # and column is not designated to be skipped
                 if col in name_cols and col not in skip_cols]
    # If there are any name columns
    if name_cols:
        # Create cleaned name dataframe from
        # input dataframe's selected name columns
        # and middle initial and suffix name conflicts
        cn_df, conflicts = clean_names(df[name_cols])
        # Create conflicts dataframe for output
        conflicts_df = df[name_cols].ix[conflicts]
        # Join input dataframe (minus name columns)
        # and the cleaned name dataframe
        df = df[list_diff(df.columns,
                          name_cols)].join(cn_df)

    # Return cleaned dataframe
    return df, conflicts_df
