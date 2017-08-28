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


def list_diff(l1, l2):
    '''returns list after taking set difference of two lists'''
    return list(set(l1) - set(l2))


def clean_int(x, na_value=np.nan):
    '''returns an integer from an object if possible,
       else returns an na_value
    '''
    if isinstance(x, str):
        # Check to see if it the string may be a float
        if re.search('^[0-9,.]*$', x):
            return int(float(x))
        # If not, return na_value
        else:
            return na_value
    elif np.isfinite(x):
        return int(float(x))
    else:
        return na_value


def standardize_gender(x):
    '''returns a standardized gender string
       by passing input string into gender reference dictionary
    '''
    # Ensure input gender (x) is string
    if isinstance(x, str):
        x = x.upper()   # Change x to uppercase
        # Check if x is already standardized
        if x in gender_dict.values():
            return x
        # If not, pass x into gender dictionary for standardization
        else:
            return gender_dict[x]
    # If not a string return 'NAN'
    else:
        return gender_dict['NAN']


def standardize_race(x):
    '''returns a standardized race string
       by passing input string into race reference dictionary
    '''
    # Ensure input race (x) is string
    if isinstance(x, str):
        x = x.upper()   # Change x to uppercase
        # Check if x is already standardized
        if x in race_dict.values():
            return x
        # If not, pass x in race dictionary for standardization
        else:
            return race_dict[x]
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
                pd.to_datetime(df[col]).dt.date
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


def extract_suffix_names(x):
    '''returns the suffix name in a given string'''
    suffixes = ('II', 'III', 'IV', 'JR', 'SR')
    # Split input name (x) by spaces and look for suffix in name pieces
    suffix = [w for w in x.split(" ") if w in suffixes]
    # If any suffix found, return it, else return empty string
    return suffix[0] if suffix else ""


def extract_middle_initial(x):
    '''returns the middle initial in a given string'''
    # Split input name (x) by spaces
    xs = x.split(' ')
    if (len(xs) > 1 and  # If there are multiple spaced parts,
            len(xs[0]) == 1 and  # the first piece is 1 character,
            len(xs[1]) > 1):  # and the length of second piece is not 1
                mi = xs[0]    # Then the first element is a middle initial
    else:
        mi = ''
    # EX: A J -> '', Billy Bob -> '', Billy -> '', B Jones -> B
    return mi


def strip_names(x):
    '''returns string after
       removing any redundant whitespace or punctuation from string
    '''
    x = re.sub(r'[^\w\s]', '', x)
    return ' '.join(x.split())


def clean_last_names(x):
    '''returns list of two strings made from input string
       after stripping string and separating suffix name
    '''
    x = strip_names(x)  # Remove redundant spaces and all punctuation
    suffix = extract_suffix_names(x)    # Find suffix
    x = x.replace(suffix, "")   # Replace suffix with empty string
    # Return list of x without suffix (and trailing space) and suffix
    return [''.join(x.split()), suffix]


def clean_first_names(x):
    '''returns list of two strings made from input string
       after stripping string and separating middle initial
    '''
    x = strip_names(x)  # Remove redundant spaces and all punctuation
    mi = extract_middle_initial(x)  # Find middle initial
    # Return list of x without middle initial (and trailing space)
    # and middle initial
    return [''.join(x.replace(mi + " ", "").split()), mi]


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


def split_names(names, main_name, sub_name, clean_func):
    ''' returns pandas dataframe of two columns
        from a pandas series of names split into main and sub parts
        the split depends on the input clean_func (clean_ first/last _names)
    '''
    names = names.map(str.upper)    # Ensure all names are uppercase
    # Map cleaing function into names series
    # Returns series of two item lists, main and sub name parts
    names = names.map(clean_func)
    return pd.DataFrame(names.values.tolist(),
                        columns=[main_name, sub_name])


def clean_names(df):
    '''returns pandas dataframe of cleaned name columns'''
    df_cols = df.columns.values  # Store column names
    # If names are Full.Names, split them into last and first names
    # Then remove suffix and middle initial respectively
    if 'Full.Name' in df_cols:
        name_df = split_full_names(df['Full.Name'])
        last = split_names(name_df['Last.Name'],
                           'Last.Name', 'Suffix.Name',
                           clean_last_names)
        first = split_names(name_df['First.Name'],
                            'First.Name', 'Middle.Initial',
                            clean_first_names)
    else:
        df = df.fillna("")  # Fill NAs with empty strings
        # Split last names into last and suffix columns
        last = split_names(df['Last.Name'],
                           'Last.Name', 'Suffix.Name',
                           clean_last_names)
        # Split first name series into first and middle initial columns
        first = split_names(df['First.Name'],
                            'First.Name', 'Middle.Initial',
                            clean_first_names)
    # Join last (and suffix) and first (and middle initial) dataframes
    name_df = last.join(first)

    # If middle initial column already existed in dataframe
    if 'Middle.Initial' in df_cols:
        # Print any conflicts between original middle initial and generated
        print('Middle Initial Conflicts:',
              name_df.ix[(df['Middle.Initial'] != '') &
                         (name_df['Middle.Initial'] != '')])
        # Replace generated middle initials with original middle initials
        # at locations with non-empty middle initials in origin dataframe
        name_df.ix[df['Middle.Initial'] != '', 'Middle.Initial'] = \
            df.ix[df['Middle.Initial'] != '', 'Middle.Initial']

    # If suffix column already existed in dataframe
    if 'Suffix.Name' in df_cols:
        # Print any conflicts between original suffix and generated
        print('Suffix Name Conflicts:',
              name_df.ix[(df['Suffix.Name'] != '') &
                         (name_df['Suffix.Name'] != '')])
        # Replace generated suffixes with original suffixes
        # at locations with non-empty suffixes in origin dataframe
        name_df.ix[df['Suffix.Name'] != '', 'Suffix.Name'] = \
            df.ix[df['Suffix.Name'] != '', 'Suffix.Name']
    # Replace any black spaces with empty string
    name_df[name_df == ' '] = ''
    return name_df


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
        cn_df = clean_names(df[name_cols])
        # Join input dataframe (minus name columns)
        # and the cleaned name dataframe
        df = df[list_diff(df.columns,
                          name_cols)].join(cn_df)

    # Return cleaned dataframe
    return df
