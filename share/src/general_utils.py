#!usr/bin/env python3
#
# Author(s):  Roman Rivera (Invisible Institute)

'''script containing utility functions used for general purposes'''

import re
import itertools
import pandas as pd
import numpy as np


def string_strip(string, no_sep=False):
    """Remove unnecessary characters from string

    Parameters
    ----------
    string : str
    no_sep : bool
        If True, remove all non alpha-numeric characters
        If False, remove periods, commas, and redundant whitespace

    Returns
    -------
        stripped_string : str

    Examples
    --------
    >>> string_strip("Mary-Ellen.", False)
    'Mary-Ellen'
    >>> string_strip("     SADOWSKY,  J.R", False)
    'SADOWSKY JR'
    >>> string_strip("KIM-TOY", True)
    'KIMTOY'
    >>> string_strip("LUQUE-.ROSALES", True)
    'LUQUEROSALES'
    """
    if no_sep:
        stripped_string = re.sub(r'[^\w\s]', '', string).replace(" ", "")
    else:
        stripped_string = re.sub(r'\s\s+', ' ',
                                 re.sub(r'^[ \s]*|\.|\,|\s$', '', string))
    return ' '.join(stripped_string.split())


def collapse_data(full_df, temp_id='TempID'):
    """Collapses dataframe to unique values
       returning collapsed data and a dataframe with original index
       and corresponding collapsed index values stored for later expansion
    Parameters
    ----------
    full_df : pandas DataFrame
    temp_id : str
        Temporary ID for collapsed data index in stored df
    Returns
    -------
    collapsed_df : pandas DataFrame
    stored_df : pandas DataFrame
        Stores indexes from full_df
        and temp_id col corresponding indexes in collapsed_df
    """
    cols = full_df.columns.tolist()
    full_df.insert(0, 'Index', full_df.index)
    collapsed_df = (full_df[cols]
                    .drop_duplicates()
                    .copy()
                    .reset_index(drop=True))
    collapsed_df.insert(0, temp_id, collapsed_df.index)
    stored_df = full_df.merge(collapsed_df, on=cols, how='inner')
    stored_df.drop(cols, axis=1, inplace=True)
    del full_df['Index']
    del collapsed_df[temp_id]
    return collapsed_df, stored_df


def expand_data(collapsed_df, stored_df, temp_id='TempID'):
    """Expandas collapsed dataframe based on index and stored_df
       returning full dataframe with same indicies as pre-collapsed
    Parameters
    ----------
    collapsed_df : pandas DataFrame
        Should be output [0] from collapse_data() after some function applied
    stored_df : pandas DataFrame
        Should be output [1] from collapse_data()
    temp_id : str
        Should be same as temp_id in collapsed_data

    Returns
    -------
    full_df : pandas DataFrame
        Indexes will be identical to full_df input in collapse_data()
    """
    collapsed_df.insert(0, temp_id, collapsed_df.index)
    stored_df = stored_df.merge(collapsed_df, on=temp_id, how='inner')
    del collapsed_df[temp_id]
    full_df = stored_df\
        .sort_values('Index')\
        .set_index('Index')\
        .drop(temp_id, axis=1)
    del full_df.index.name
    return full_df


def remove_duplicates(dup_df, cols=[], unique=False):
    """Removes rows that are non-unique based on values in specified columns.
       Exact opposite of keep_duplicates().
    Parameters
    ----------
    dup_df : pandas DataFrame
    cols : list
        Column names in dup_df to determine unique-ness of row
        If no columns specified, assumes all columns in dup_df
    unique : bool
    Returns
    -------
    rd_df : pandas DataFrame
        Dataframe of rows that were unique (based on input cols) in dup_df
        Sorted by values of the input cols
    """
    if not cols:
        cols = dup_df.columns.tolist()
    if unique:
        dup_df = dup_df.drop_duplicates()
    rd_df = dup_df[~dup_df.duplicated(subset=cols, keep=False)]\
        .sort_values(cols)
    return rd_df


def keep_duplicates(dup_df, cols=[]):
    """Keeps rows that are non-unique based on values in specified columns.
       Exact opposite of remove_duplicates().
    Parameters
    ----------
    dup_df : pandas DataFrame
    cols : list
        Column names in dup_df to determine unique-ness of row
        If no columns specified, assumes all columns in dup_df

    Returns
    -------
    kd_df : pandas DataFrame
        Dataframe of rows that were not-unique (based on input cols) in dup_df
        Sorted by values of the input cols
    """
    if not cols:
        cols = dup_df.columns
    kd_df = dup_df[dup_df.duplicated(subset=cols, keep=False)]\
        .sort_values(cols)
    return kd_df


def keep_conflicts(dup_df, cols=[], all_dups=True):
    """Keeps rows that are duplicates in any identified column

    Parameters
    ----------
    dup_df : pandas DataFrame
    cols : list
        Column names in dup_df to determine unique-ness of row
        If no columns specified, assumes all columns in dup_df
    all_dups : bool
        If True (default), keep all rows identified by keep_duplicates
        If False, keep only rows that have duplicates in each identified column
    Returns
    -------
    conflicts_df : pandas DataFrame
    """
    if not cols:
        cols = dup_df.columns.tolist()

    kdf_list = [keep_duplicates(dup_df, col) for col in cols]
    conflicts_df = pd.concat(kdf_list)

    if not all_dups:
        ind_list = [kdf.index.tolist() for kdf in kdf_list]
        keep_list = list(set(ind_list[0]).intersection(*ind_list))
        conflicts_df = conflicts_df.loc[keep_list]

    conflicts_df = conflicts_df.drop_duplicates()\
        .sort_index()

    return conflicts_df

def union_group(df, gid, cols, sep = '__', starting_gid=1):
    """Adds group id (gid) numbers to data based on multiple columns
    By taking the union of rows which contain an overlapping
    set of values in each columns as collecting duplicates of specified
    columns does not include columns that may be missing values.
    This is similar to creating a network where edges connect values between
    columns in the same row and identifying rows by connected component.
    (ex. looking at dates:
     (A) 7/6/1993,  (B) 11/19/1930, and  (C) 7/21/1930
     would be in the same group as (A) shares 7 as the month with (C)
     and  (B) shares 1930 as the year with (C),
     while (D) 6/20/1931 has a separate group since it has no element
     in common with any of (A), (B), or (C), despite (A) having a 6 in it,
     this is for the day field while (D) has 6 in the month field)

    Parameters
    ----------
    df : pandas DataFrame
    gid : str
    cols : list
        Column names to be used for identifying groups
    sep : str
        Separator for temporary values
    starting_gid : int

    Returns
    -------
    out_df : pandas DataFrame
    """
    import networkx as nx

    all_vals = set()
    temp_cols = []
    # Create temporary columns to ensure no overlapping of values between columns
    for col in cols:
        df.loc[df[col].notnull(),'temp_'+col] =\
            df.loc[df[col].notnull(), col].map(lambda x: col + sep + str(x))

        assert not all_vals & set(df['temp_'+col].dropna())
        all_vals.update(set(df['temp_'+col].dropna()))
        temp_cols.append('temp_'+col)
    # Generate edge list of connections between column values by row
    el = []
    for i,r in df[temp_cols].drop_duplicates().iterrows():
        vals = r.dropna().tolist()
        if len(vals) > 1:
            els = list(itertools.combinations(vals,2))
            el.extend(els)
        else:
            el.append((vals[0], vals[0]))
    # Collect edgelist into list of connected components
    cc = nx.connected_components(
            nx.from_pandas_dataframe(
                pd.DataFrame(el, columns=['H', 'T']),
                'H','T'))
    # Turn column values into list of 'nodes' with group ids
    ccl = []
    for group in cc:
        ccl.extend(list(zip([starting_gid]*len(group), group)))
        starting_gid+=1
    node_df = pd.DataFrame(ccl, columns=[gid, 'node'])
    out_df = pd.DataFrame()
    df.insert(0, 'ROWID', df.index)
    # Iterate over temporary columns and merge back group ids using 'nodes'
    for col in temp_cols:
        mdf = df[['ROWID', col]].merge(node_df, left_on=col,
                                        right_on='node', how='inner')
        out_df = out_df.append(mdf[['ROWID', gid]])
        df.drop(col, axis=1, inplace=True)
    out_df = df.merge(out_df.drop_duplicates(), on='ROWID', how='left')\
        .set_index('ROWID')
    del out_df.index.name
    df.drop('ROWID', axis=1, inplace=True)

    return out_df


def reshape_data(df, reshape_col, id_col):
    """Reshapes dataframe from wide to long for a single columns
       preservers observations with only NaN values in reshape columns

    Parameters
    ----------
    df : pandas DataFrame
    reshape_col : str
        Name of column to be reshaped by ('star' for star1, star2, ...)
    id_col : str
        Name of column containing unique ids

    Returns
    ----------
    long_df : pandas DataFrame
    """
    long_df = pd.wide_to_long(
        df, [reshape_col], j=reshape_col+'_num', i=id_col)\
        .dropna(subset=[reshape_col])\
        .reset_index()\
        .drop(reshape_col+'_num', axis=1)
    dropped_ids = list(set(df[id_col]) - set(long_df[id_col]))
    if dropped_ids:
        long_df = long_df.append(
            df.loc[df[id_col].isin(dropped_ids),
                   list_diff(long_df.columns, [reshape_col])])

    long_df = long_df.drop_duplicates().reset_index(drop=True)

    return long_df


def fill_data(df, id_col):
    """Creates dataframe of products of all non-nan value in fill_columns
    Parameters
    ----------
    df : pandas DataFrame

    Returns
    ----------
    filled_df : pandas DataFrame
    """
    cols = df.columns
    df = df[cols].drop_duplicates()
    rd_df = remove_duplicates(df, id_col)
    kd_list = []

    def df_product(df):
        """Iterates over columns and creates list of list of non-nan values"""
        non_nan_list = []
        for col in cols:
            non_nan_vals = df[col].dropna().unique().tolist()
            if non_nan_vals:
                non_nan_list.append(non_nan_vals)
            else:
                non_nan_list.append([np.nan])
        return list(itertools.product(*non_nan_list))

    for ind, grp in keep_duplicates(df, id_col).groupby(id_col):
        kd_list.extend(df_product(grp))

    filled_df = pd.DataFrame(kd_list, columns=cols)\
        .append(rd_df)\
        .sort_values(id_col)\
        .reset_index(drop=True)
    return filled_df


def list_unique(dup_list):
    """Returns list of first unique values in a list
    Parameters
    ----------
    dup_list : list

    Returns
    -------
    unique_list : list

    Examples
    --------
    >>> list_unique([3,2,1,3,2,1,1,2,1,1])
    [3, 2, 1]
    >>> list_unique([])
    []
    """
    unique_list = []
    for i in dup_list:
        if i not in unique_list:
            unique_list.append(i)
    return unique_list


def list_intersect(list1, list2, unique=True):
    """Returns list of (unique) elements in list1 and list2 in order of list1
    Parameters
    ----------
    list1 : list
    list2 : list
    unique : bool
        If True (default) unique elements intersected elements are returned
        If False uniqueness is not enforced

    Returns
    -------
    intersected_list : list

    Examples
    --------
    >>> list_intersect(['A', 3, 3, 4, 'D'], ['D', 'B', 99, 3, 'A', 'A'], True)
    ['A', 3, 'D']
    >>> list_intersect([1,2,3], [4,5,6])
    []
    >>> list_intersect([1,2,3,1], [4,5,6,1], False)
    [1, 1]
    """
    if unique:
        list1 = list_unique(list1)
    intersected_list = [i for i in list1 if i in list2]
    return intersected_list


def list_diff(list1, list2, unique=True):
    """Returns list of (unique) elements in list1
       but not in list2 in order of list1
    Parameters
    ----------
    list1 : list
    list2 : list
    unique : bool
        If True (default) unique elements are returned
        If False uniqueness is not enforced

    Returns
    -------
    setdiff_list : list

    Examples
    --------
    >>> list_diff([1, 2, 2, 3, 1, 2, 3], [3, 2, 14, 5, 6])
    [1]
    >>> list_diff([1,1,2,3,4,2], [4,2,3], False)
    [1, 1]
    >>> list_diff([], [1,2,3])
    []
    """
    if unique:
        list1 = list_unique(list1)
    diff_list = [i for i in list1 if i not in list2]
    return diff_list


def list_union(list1, list2, unique=True):
    """Returns (unique) union of elements in list1 and list2
    Parameters
    ----------
    list1 : list
    list2 : list
    unique : bool
        If True (default) unique elements are returned
        If False uniqueness is not enforced

    Returns
    -------
    union_list : list

    Examples
    --------
    >>> list_union([1, 2, 2, 3, 4, 3], [6, 2, 3, 1, 9])
    [1, 2, 3, 4, 6, 9]
    >>> list_union([1, 2, 2, 3, 4, 3], [6, 2, 3, 1, 9], False)
    [1, 2, 2, 3, 4, 3, 6, 2, 3, 1, 9]
    """
    union_list = list(list1) + list(list2)
    if unique:
        union_list = list_unique(union_list)
    return union_list


if __name__ == "__main__":
    import doctest
    doctest.testmod()
    doctest.run_docstring_examples(list_unique, globals())
    doctest.run_docstring_examples(list_intersect, globals())
    doctest.run_docstring_examples(list_diff, globals())
    doctest.run_docstring_examples(list_union, globals())

#
# end
