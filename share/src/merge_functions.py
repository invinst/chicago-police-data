#!usr/bin/env python3
#
# Author: Roman Rivera

'''functions used to merge datasets'''

import itertools
import pandas as pd
from assign_unique_ids_functions import remove_duplicates, aggregate_data


def intersect(list1, list2):
    '''returns list of unique intersection between two lists'''
    return list(set(list1) & set(list2))


def listdiff(list1, list2):
    '''returns list of unique items in first list but not in second list'''
    return list(set(list1) - set(list2))


def union(list1, list2):
    '''returns list of unique items in both lists'''
    return list(set(list1) | set(list2))


def unique(duplist):
    '''returns list of unique items in list'''
    return list(set(duplist))


def take_first_four(in_str):
    '''returns first 4 characters in string'''
    return in_str[:4]


def add_columns(df,
                add_cols=["F4FN", "F4LN", "Current.Age", "Min_Max_Star",
                          "BY_to_CA", "Stars", "L4FN", "L4LN"],
                current_age_from=2017):
    '''returns pandas dataframe with columns added on
       depending on the specified add_cols and the columns
       in the input dataframe.
    '''
    # Add F(irst) 4 of F(irst)/L(ast) N(ame) columns
    # if F4FN/F4LN and First./Last.Name_NS in df columns
    if "F4FN" in add_cols and "First.Name_NS" in df.columns:
        df['F4FN'] = df['First.Name_NS'].map(lambda x: x[:4])
    if "F4LN" in add_cols and 'Last.Name_NS' in df.columns:
        df['F4LN'] = df['Last.Name_NS'].map(lambda x: x[:4])

    # Add F(irst) 2 of F(irst)/L(ast) N(ame) columns
    # if F2FN/F2LN and First./Last.Name_NS in df columns
    if "F2FN" in add_cols and "First.Name_NS" in df.columns:
        df['F2FN'] = df['First.Name_NS'].map(lambda x: x[:2])
    if "F2LN" in add_cols and 'Last.Name_NS' in df.columns:
        df['F2LN'] = df['Last.Name_NS'].map(lambda x: x[:2])

    # Add L(ast) 4 of F(irst)/L(ast) N(ame) columns
    # if L4FN/L4LN and First./Last.Name_NS in df columns
    if "L4FN" in add_cols and "First.Name_NS" in df.columns:
        df['L4FN'] = df['First.Name_NS'].map(lambda x: x[-4:])
    if "L4LN" in add_cols and 'Last.Name_NS' in df.columns:
        df['L4LN'] = df['Last.Name_NS'].map(lambda x: x[-4:])

    # Since current age cannot be always matched based on birth year
    # If Current.Age will be used for matching,
    # Current.Age.p(lus)1 and Current.Age.m(inus)1 must be added
    # If Current.Age is in the dataframe then both are equal to it
    if "Current.Age" in add_cols and "Current.Age" in df.columns:
        df['Current.Age.p1'] = df['Current.Age']
        df['Current.Age.m1'] = df['Current.Age']
    # If BY_to_CA is specified and Birth.Year is a column
    # Generate Current.Age.p/m1 by subtracting current_age_from from birth year
    # and subtract 1 for the .m1 column
    if "BY_to_CA" in add_cols and "Birth.Year" in df.columns:
        df['Current.Age.p1'] = \
            df['Birth.Year'].map(lambda x: current_age_from - x)
        df['Current.Age.m1'] = \
            df['Birth.Year'].map(lambda x: current_age_from - x - 1)

    # If Min_Max_Star specified in add_cols and Star1-10 in columns
    # Then create min/max star columns
    if ('Min_Max_Star' in add_cols and
            'Star1' in df.columns and
            'Star10' in df.columns):
        star_cols = ['Star' + str(i)
                     for i in range(1, 11)]
        df['Min.Star'] = df[star_cols].min(axis=1, skipna=True)
        df['Max.Star'] = df[star_cols].max(axis=1, skipna=True)

    # If Stars specified in add_cols and Current.Star in columns
    # and Star1 (thus Star2-9) not in columns
    # Then create Star1-10 columns all equal to Current.Star
    if ('Stars' in add_cols and
            'Current.Star' in df.columns and
            'Star1' not in df.columns):
        for i in range(1, 11):
            df['Star{}'.format(i)] = df['Current.Star']

    # Return dataframe with relevant columns added
    return df


def generate_on_lists(data_cols, base_lists, drop_cols):
    '''returns list of lists composed of every possible combination
       of each value in the sub lists of base_lists, so long as those
       values are included in the data_cols, and not in drop_cols
       EX: generate_on_lists(['A1', 'A2', 'B1','B2','C2'],
                             [['A1','A2', ''], ['B1', 'B2'], ['C1'])
           -> [[],[], [], []]
    '''
    # Initialize empty merge lists
    merge_lists = []
    # Add empty string to data_cols as placeholder for non-necessary columns
    data_cols.append('')
    # Loop over lists in base_lists
    for col_list in base_lists:
        # Initialize merge list as the intersection between
        # col list (from base lists) and the data_cols
        # and list difference from drop columns
        merge_list = listdiff(intersect(col_list, data_cols),
                              drop_cols)
        # If merge_list is not empty
        if merge_list:
            # Append merge_list to merge_lists
            merge_lists.append(sorted(merge_list, reverse=True))
    # Initialize on_lists by generating lists from all combinations
    # of one element in each list in merge_lists
    on_lists = list(itertools.product(*reversed(merge_lists)))
    # '' was used as a placeholder for when a column is not always necessary
    # Remove '' elements from lists in on_lists
    on_lists = [[i for i in ol if i != ''] for ol in on_lists]

    # Return on_lists used for loop_merge
    return on_lists


def generate_merge_report(total_merged,
                          total_df1,
                          total_df2,
                          decimals=2):
    '''returns formatted string listing details about
       output of a merge between two datasets
    '''
    unmerged_df1 = total_df1 - total_merged
    unmerged_df2 = total_df2 - total_merged
    prcnt_m_df1 = round(100 * total_merged / total_df1,
                        decimals)
    prcnt_m_df2 = round(100 * total_merged / total_df2,
                        decimals)
    prcnt_um_df1 = round(100 * unmerged_df1 / total_df1,
                         decimals)
    prcnt_um_df2 = round(100 * unmerged_df2 / total_df2,
                         decimals)
    return(('{0} Total Merged. '
            '{1}% of DF1 and {2}% of DF2 Merged.\n'
            '{3} Unmerged in DF1. '
            '{4}% Unmerged.\n'
            '{5} Unmerged in DF2. '
            '{6}% Unmerged.').format(total_merged,
                                     prcnt_m_df1,
                                     prcnt_m_df2,
                                     unmerged_df1,
                                     prcnt_um_df1,
                                     unmerged_df2,
                                     prcnt_um_df2))


def loop_merge(df1, df2, on_lists, keep_columns, print_merging=False,
               return_unmatched=True, return_merge_report=True):
    '''returns a dictionary containing, at minimum, a pandas dataframe
       resulting from iterative merging of two dataframes
    '''
    # Initialize match dataframe, used to store successful merges
    dfm = pd.DataFrame(columns=keep_columns + ['Match'])
    # Store number of rows for input dataframes 1 and 2
    df1_rows = df1.shape[0]
    df2_rows = df2.shape[0]
    # Iterate over lists of column names in on_lists
    for merge_cols in on_lists:
        # Create temporary dataframes of df1 and df2
        # Including only the relevant keep_column in each
        # and the columns used for merging in this loop
        # Remove all rows with duplicate merge_cols (see remove_duplicates)
        # This ensures no double-merging on identical rows
        df1t = remove_duplicates(df1[keep_columns[:1] + merge_cols],
                                 merge_cols)
        df2t = remove_duplicates(df2[keep_columns[1:] + merge_cols],
                                 merge_cols)
        # Create temporary match dataframe by merging df1t and df2t
        # on merge_cols such that only unique matches are saved
        dfmt = df1t.merge(df2t, on=merge_cols, how='inner')
        # If there are temporary matches
        if dfmt.shape[0] > 0:
            # Print this round of merging if print_merging==True
            if print_merging:
                print(('{0} Matches on \n'
                       '{1} columns').format(dfmt.shape[0],
                                             merge_cols))
            # Store the merge columns as string
            # in Match column of temporary match dataframe
            dfmt['Match'] = '-'.join(merge_cols)
            # Update match dataframe by appending temporary match dataframe
            dfm = dfm.append(dfmt[keep_columns +
                                  ['Match']].reset_index(drop=True))
            # Update df1 and df2 by removing successfully merged rows
            df1 = df1.loc[~df1[keep_columns[0]].isin(dfm[keep_columns[0]])]
            df2 = df2.loc[~df2[keep_columns[1]].isin(dfm[keep_columns[1]])]

    # Generate summary of merging process
    merge_report = generate_merge_report(dfm.shape[0],
                                         df1_rows,
                                         df2_rows)
    print(merge_report)

    # Initialize dictionary of return values with 'merged' data
    return_dict = {'merged': dfm.reset_index(drop=True)}
    # If return_unmatched is True then return unmatched rows in
    # df1 and df2 as UM1 and UM2, respectively.
    if return_unmatched:
        return_dict['UM1'] = df1
        return_dict['UM2'] = df2
    # If return_merge_report is True then return merge report as MR
    if return_merge_report:
        return_dict['MR'] = merge_report

    return return_dict


def merge_datasets(df1, df2, keep_columns, custom_matches=[],
                   no_match_cols=[], min_match_length=4,
                   extend_base_lists=[], drop_base_cols=[],
                   current_age_from=2017,
                   expand_stars=False, F2=False, L4=False,
                   return_unmatched=True, return_merge_report=True,
                   print_merging=False):
    '''returns dictionary from loop_merge
       automates adding columns and merging list creation
    '''
    # Remove columns from df1 and df2 that are all NaN
    df1 = df1.dropna(axis=1, how='all')
    df2 = df2.dropna(axis=1, how='all')

    add_cols = []   # Initialize add_cols
    # Intersect df1 and df2 columns
    df12_cols = intersect(df1.columns, df2.columns)
    # Add specified columns to add_cols given set of conditions
    if 'First.Name_NS' in df12_cols:
        add_cols.append('F4FN')
    if 'Last.Name_NS' in df12_cols:
        add_cols.append('F4LN')
    if "Birth.Year" not in df12_cols:
        add_cols.extend(["BY_to_CA", "Current.Age"])
    if 'Star1' not in df12_cols and expand_stars:
        add_cols.append('Stars')
    if 'Star1' in df12_cols and 'Star10' in df12_cols:
        add_cols.append('Min_Max_Star')
    if 'First.Name_NS' in df12_cols and F2:
        add_cols.append('F2FN')
    if 'Last.Name_NS' in df12_cols and F2:
        add_cols.append('F2LN')
    if 'First.Name_NS' in df12_cols and L4:
        add_cols.append('L4FN')
    if 'Last.Name_NS' in df12_cols and L4:
        add_cols.append('L4LN')
    # Add specified add_cols to both dataframes
    df1 = add_columns(df1, add_cols, current_age_from)
    df2 = add_columns(df2, add_cols, current_age_from)

    # Collect columns in both df1 and df2
    cols = intersect(df1.columns, df2.columns)

    # Keep columns in df1 and df2 either in keep_columns or in both dataframes
    df1 = df1[keep_columns[:1] + cols]
    df2 = df2[keep_columns[1:] + cols]

    # Create list of lists:
    # Each list contains a single 'type' of column which
    # are listed in order from most to least important
    # EX: Birth.Year is more reliable than Current.Age
    # And the lists themselves are in order from most to least useful
    # empty strings used as placeholder for non-crucial column 'types'
    base_lists = [
        ['Current.Star', 'Min.Star', 'Max.Star',
         'Star1', 'Star2', 'Star3', 'Star4', 'Star5',
         'Star6', 'Star7', 'Star8', 'Star9', 'Star10'],
        ['First.Name_NS', 'F4FN', 'F2FN'],
        ['Last.Name_NS', 'F4LN', 'F2LN'],
        ['Appointed.Date'],
        ['Birth.Year', 'Current.Age', 'Current.Age.p1', 'Current.Age.m1', ''],
        ['Middle.Initial', ''],
        ['Middle.Initial2', ''],
        ['Gender', ''],
        ['Race', ''],
        ['Suffix.Name', ''],
        ['Current.Unit', '']
    ]
    if extend_base_lists:
        base_lists.extend(extend_base_lists)
    # Generate on_lists from cols in both datasets and base_lists
    on_lists = generate_on_lists(cols, base_lists, drop_base_cols)

    # Iterate over no_match_cols
    # A no_match_col identifies a list in base_lists
    # that should be dropped, an a new on_lists generated,
    # which will be appended onto the original on_lists
    for nmc in no_match_cols:
        # Generate on_lists with the base list that contains
        # the specified nmc removed
        nmc_lists = generate_on_lists(cols,
                                      [ml for ml in base_lists
                                       if nmc not in ml],
                                      drop_base_cols)
        # Ensure all generated no_match_col on_lists contain
        # at least min_match_length items
        nmc_lists = [nmcl for nmcl in nmc_lists
                     if len(nmcl) >= min_match_length]
        # Add the new on_lists to the original on_lists
        on_lists.extend(nmc_lists)

    # If custom_matches are specified
    if custom_matches:
        # Add custom_matches (list of lists) to end of on_lists
        on_lists.extend(custom_matches)

    # Run loop_merge on prepared df1 and df1,
    # merging on on_lists, etc.
    merged_data = loop_merge(df1, df2,
                             on_lists=on_lists,
                             keep_columns=keep_columns,
                             return_unmatched=return_unmatched,
                             return_merge_report=return_merge_report,
                             print_merging=print_merging)

    # Return results of loop_merge
    return merged_data


def append_to_reference(sub_df, profile_df, ref_df,
                        custom_matches=[], return_unmatched=False,
                        min_match_length=4, no_match_cols=[],
                        extend_base_lists=[], drop_base_cols=[],
                        return_merge_report=True, print_merging=False,
                        return_merge_list=True, expand_stars=False,
                        F2=False, L4=False, current_age_from=2017):
    '''returns dictionary including at least a pandas dataframe
       appends merged and unmerged results from merge_datasets
       on to the input ref_df
    '''
    # Initialize return_dict with None objects instead of values
    return_dict = {'ref': None,
                   'UM1': None,
                   'UM2': None,
                   'MR': None,
                   'ML': None}

    # Reset indexes
    profile_df.reset_index(drop=True, inplace=True)
    sub_df.reset_index(drop=True, inplace=True)

    # Check if profile_df or ref_df is empty
    if profile_df.empty or ref_df.empty:
        # If it is, then sub_df becomes profile_df and ref_df by default
        # UID column is initialized equal to sub_df.index + 1
        sub_df.insert(0, 'UID', sub_df.index + 1)
        # return sub_df as ref(erence) in return_dict
        return_dict['ref'] = sub_df
    # If profile_df and ref_df are not empty actually do merging
    else:
        # Get id_col from sub_df (which will always end with _ID)
        id_col = [col for col in sub_df.columns if col.endswith('_ID')][0]
        # Initialize keep_columns with UID first and id_col second
        keep_columns = ['UID', id_col]

        # Run merge_datasets on profiles_df and sub_df
        md_dict = merge_datasets(profile_df, sub_df,
                                 keep_columns=keep_columns,
                                 custom_matches=custom_matches,
                                 no_match_cols=no_match_cols,
                                 min_match_length=min_match_length,
                                 extend_base_lists=extend_base_lists,
                                 drop_base_cols=drop_base_cols,
                                 return_merge_report=return_merge_report,
                                 expand_stars=expand_stars,
                                 F2=F2,
                                 L4=L4,
                                 current_age_from=current_age_from,
                                 print_merging=print_merging)

        # Create a link_df of all merge and unmerged rows
        link_df = pd.concat([md_dict['merged'][keep_columns],
                             md_dict['UM1'][[keep_columns[0]]],
                             md_dict['UM2'][[keep_columns[1]]]])[
              keep_columns].reset_index(drop=True)
        # Sort link_df by uid, with NaN uids (unmatched sub_df rows) last
        link_df = link_df.sort_values('UID', na_position='last')
        link_df.reset_index(drop=True, inplace=True)    # Reset index
        # Reinitalize UID column equal to the index + 1
        # All pre-existing UIDs will be preserved, new ones will be added
        link_df['UID'] = link_df.index + 1
        # Give sub_df UID column by merging link_df to it
        sub_df = sub_df.merge(link_df[unique([keep_columns[1], 'UID'])],
                              on=keep_columns[1], how='left')
        # Add the data in sub_df at the end of the ref_df
        # and reset index
        ref_df = ref_df.append(sub_df).reset_index(drop=True)

        # Start filling return_dict
        # Regardless of arguments, append_to_reference always returns ref_df
        return_dict['ref'] = ref_df
        if return_unmatched:
            return_dict['UM1'] = md_dict['UM1']
            return_dict['UM2'] = md_dict['UM2']
        if return_merge_report:
            return_dict['MR'] = md_dict['MR']
        if return_merge_list:
            return_dict['ML'] = md_dict['merged']['Match']

    return return_dict


def generate_profiles(ref, uid,
                      column_order=[
                            'First.Name_NS', 'Last.Name_NS',
                            'Middle.Initial', 'Suffix.Name', 'Middle.Initial2',
                            'Race', 'Gender', 'Birth.Year', 'Appointed.Date',
                            'Resignation.Date', 'Current.Rank',
                            'Current.Age', 'Current.Unit', 'Current.Star',
                            'Star1', 'Star2', 'Star3', 'Star4', 'Star5',
                            'Star6', 'Star7', 'Star8', 'Star9', 'Star10'],
                      mode_cols=[],
                      max_cols=[]):
    '''returns pandas dataframe
       after aggregating data from the input reference dataframe
       sorts columns by column order
       and counts number of occurance of each officer in reference
    '''
    # Initialize profiles data by aggregating input ref data
    profiles = aggregate_data(ref, uid,
                              mode_cols=mode_cols,
                              max_cols=max_cols)
    # Initialize count_df, counting number of occurances by uid
    count_df = pd.DataFrame(ref[uid].value_counts())
    # Rename column in count_df to profile_count
    count_df.columns = ['profile_count']
    # Create uid column equal to the index of count_df
    count_df[uid] = count_df.index
    # Merge count_df to the profiles dataframe on uid
    profiles = profiles.merge(count_df, on=uid)
    # Ensure no uids were excluded
    assert profiles.shape[0] == len(ref[uid].unique()),\
        'Missing some UIDs'
    # Collect _ID cols from profiles
    ID_cols = [col for col in profiles.columns
               if col.endswith('_ID')]
    # Reorder sort columns in profiles by column_order
    cols = [col for col in column_order
            if col in profiles.columns]
    # Reorder profile columns with uid first, then core columns,
    # then ID columns and ending with profile_count
    profiles = profiles[[uid] + cols + ID_cols + ['profile_count']]

    # Return profiles
    return profiles


def remerge(df, link_df, uid_col, id_col):
    '''returns pandas dataframe after merging new unique ids
       taken from a link_df
    '''
    rows = df.shape[0]
    df = df.merge(link_df[[uid_col, id_col]],
                  on=id_col, how='left')
    assert(df.shape[0] == rows), print('Missing rows!')
    return df
