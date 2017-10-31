#!usr/bin/env python3
#
# Author(s): Roman Rivera

'''functions used to merge datasets'''

import itertools
import pandas as pd
from assign_unique_ids_functions import remove_duplicates, aggregate_data


def unique(duplist):
    '''returns list of unique items in list while maintaining order

    >>> unique([3,2,1,3,2,1,1,2,1,1])
    [3, 2, 1]
    >>> unique([])
    []
    '''
    uniques = []
    for i in duplist:
        if i not in uniques:
            uniques.append(i)
    return uniques


def intersect(list1, list2):
    '''returns list of unique intersection between two lists

    >>> intersect(['A', 3, 3, 4, 'D'], ['D', 'B', 99, 3, 'A', 'A'])
    ['A', 3, 'D']
    >>> intersect([1,2,3], [4,5,6])
    []
    '''
    return [i for i in unique(list1) if i in list2]


def listdiff(list1, list2):
    '''returns list of unique items in first list but not in second list

    >>> listdiff([1, 2, 2, 3, 1, 2, 3], [3, 2, 14, 5, 6])
    [1]
    >>> listdiff([], [1,2,3])
    []
    '''
    return [i for i in unique(list1) if i not in list2]


def union(list1, list2):
    '''returns list of unique items in both lists

    >>> union([1, 2, 2, 3, 4, 3], [6, 2, 3, 1, 9])
    [1, 2, 3, 4, 6, 9]
    '''

    return unique(list1 + list2)


def take_first_four(in_str):
    '''returns first 4 characters in string

    >>> take_first_four('abcde')
    'abcd'
    >>> take_first_four('a')
    'a'
    '''
    return in_str[:4]


def add_columns(df,
                add_cols=["F4FN", "F4LN", "current_age", "min_max_star",
                          "BY_to_CA", "stars", "L4FN", "L4LN"],
                current_age_from=2017,
                end_star=10):
    '''returns pandas dataframe with columns added on
       depending on the specified add_cols and the columns
       in the input dataframe.

       see test_merge_functions.py for tests and details
    '''
    # Add F(irst) 4 of F(irst)/L(ast) N(ame) columns
    # if F4FN/F4LN and First./last_name_NS in df columns
    if "F4FN" in add_cols and "first_name_NS" in df.columns:
        df['F4FN'] = df['first_name_NS'].map(lambda x: x[:4])
    if "F4LN" in add_cols and 'last_name_NS' in df.columns:
        df['F4LN'] = df['last_name_NS'].map(lambda x: x[:4])

    # Add F(irst) 2 of F(irst)/L(ast) N(ame) columns
    # if F2FN/F2LN and First./last_name_NS in df columns
    if "F2FN" in add_cols and "first_name_NS" in df.columns:
        df['F2FN'] = df['first_name_NS'].map(lambda x: x[:2])
    if "F2LN" in add_cols and 'last_name_NS' in df.columns:
        df['F2LN'] = df['last_name_NS'].map(lambda x: x[:2])

    # Add L(ast) 4 of F(irst)/L(ast) N(ame) columns
    # if L4FN/L4LN and First./last_name_NS in df columns
    if "L4FN" in add_cols and "first_name_NS" in df.columns:
        df['L4FN'] = df['first_name_NS'].map(lambda x: x[-4:])
    if "L4LN" in add_cols and 'last_name_NS' in df.columns:
        df['L4LN'] = df['last_name_NS'].map(lambda x: x[-4:])

    # Since current age cannot be always matched based on birth year
    # If Current.Age will be used for matching,
    # Current.Age.p(lus)1 and Current.Age.m(inus)1 must be added
    # If Current.Age is in the dataframe then both are equal to it
    if "current_age" in add_cols and "current_age" in df.columns:
        df['current_age_p1'] = df['current_age']
        df['current_age_m1'] = df['current_age']
    # If BY_to_CA is specified and Birth.Year is a column
    # Generate Current.Age.p/m1 by subtracting current_age_from from birth year
    # and subtract 1 for the .m1 column
    if "BY_to_CA" in add_cols and "birth_year" in df.columns:
        df['current_age_p1'] = \
            df['birth_year'].map(lambda x: current_age_from - x)
        df['current_age_m1'] = \
            df['birth_year'].map(lambda x: current_age_from - x - 1)

    # If Min_Max_Star specified in add_cols and Star1-10 in columns
    # Then create min/max star columns
    if ('min_max_star' in add_cols and
            'star1' in df.columns and
            'star10' in df.columns):
        star_cols = ['star' + str(i)
                     for i in range(1, 11)]
        df['min_star'] = df[star_cols].min(axis=1, skipna=True)
        df['max_star'] = df[star_cols].max(axis=1, skipna=True)

    # If Stars specified in add_cols and Current.Star in columns
    # and Star1 (thus Star2-9) not in columns
    # Then create Star1-10 columns all equal to Current.Star
    if ('stars' in add_cols and
            'current_star' in df.columns and
            'star1' not in df.columns):
        for i in range(1, end_star+1):
            df['star{}'.format(i)] = df['current_star']

    # Return dataframe with relevant columns added
    return df


def generate_on_lists(data_cols, base_lists, drop_cols):
    '''returns list of lists composed of every possible combination
       of each value in the sub lists of base_lists, so long as those
       values are included in the data_cols, and not in drop_cols

       see test_merge_functions.py for tests and details
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
               return_unmatched=True, merge_report=True):
    '''returns a dictionary containing, at minimum, a pandas dataframe
       resulting from iterative merging of two dataframes

       see test_merge_functions.py for tests and details
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

    # Initialize dictionary of return values with 'merged' data
    return_dict = {'merged': dfm.reset_index(drop=True)}
    # If return_unmatched is True then return unmatched rows in
    # df1 and df2 as UM1 and UM2, respectively.
    if return_unmatched:
        return_dict['UM1'] = df1
        return_dict['UM2'] = df2
    # If merge_report is True then return merge report as MR
    if merge_report:
        return_dict['MR'] = merge_report

    return return_dict


def merge_datasets(df1, df2, keep_columns, custom_matches=[],
                   no_match_cols=[], min_match_length=4,
                   extend_base_lists=[], drop_base_cols=[],
                   current_age_from=2017,
                   expand_stars=False, F2=False, L4=False,
                   return_unmatched=True, merge_report=True,
                   print_merging=False):
    '''returns dictionary from loop_merge
       automates adding columns and merging list creation

       see test_merge_functions.py for tests and details
    '''
    # Remove columns from df1 and df2 that are all NaN
    df1 = df1.dropna(axis=1, how='all')
    df2 = df2.dropna(axis=1, how='all')

    add_cols = []   # Initialize add_cols
    # Intersect df1 and df2 columns
    df12_cols = intersect(df1.columns, df2.columns)
    # Add specified columns to add_cols given set of conditions
    if 'first_name_NS' in df12_cols:
        add_cols.append('F4FN')
    if 'last_name_NS' in df12_cols:
        add_cols.append('F4LN')
    if "birth_year" not in df12_cols:
        add_cols.extend(["BY_to_CA", "current_age"])
    if 'star1' not in df12_cols and expand_stars:
        add_cols.append('stars')
    if 'star1' in df12_cols and 'star10' in df12_cols:
        add_cols.append('min_max_star')
    if 'first_name_NS' in df12_cols and F2:
        add_cols.append('F2FN')
    if 'last_name_NS' in df12_cols and F2:
        add_cols.append('F2LN')
    if 'first_name_NS' in df12_cols and L4:
        add_cols.append('L4FN')
    if 'last_name_NS' in df12_cols and L4:
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
        ['current_star', 'min_star', 'max_star',
         'star1', 'star2', 'star3', 'star4', 'star5',
         'star6', 'star7', 'star8', 'star9', 'star10'],
        ['first_name_NS', 'F4FN', 'F2FN'],
        ['last_name_NS', 'F4LN', 'F2LN'],
        ['appointed_date'],
        ['birth_year', 'current_age', 'current_age_p1', 'current_age_m1', ''],
        ['middle_initial', ''],
        ['middle_initial2', ''],
        ['gender', ''],
        ['race', ''],
        ['suffix_name', ''],
        ['current_unit', '']
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
                             merge_report=merge_report,
                             print_merging=print_merging)

    # Return results of loop_merge
    return merged_data


def append_to_reference(sub_df, profile_df, ref_df,
                        custom_matches=[], return_unmatched=False,
                        min_match_length=4, no_match_cols=[],
                        extend_base_lists=[], drop_base_cols=[],
                        merge_report=True, print_merging=False,
                        return_merge_list=True, expand_stars=False,
                        F2=False, L4=False, current_age_from=2017):
    '''returns dictionary including at least a pandas dataframe
       appends merged and unmerged results from merge_datasets
       on to the input ref_df

       see test_merge_functions.py for tests and details
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
                                 merge_report=merge_report,
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
        if merge_report:
            return_dict['MR'] = md_dict['MR']
        if return_merge_list:
            return_dict['ML'] = md_dict['merged']['Match']

    return return_dict


def generate_profiles(ref, uid,
                      column_order=[
                            'first_name_NS', 'last_name_NS',
                            'middle_initial', 'suffix_name', 'middle_initial2',
                            'race', 'gender', 'birth_year', 'appointed_date',
                            'resignation_date', 'current_rank', 'current_age',
                            'current_unit', 'current_unit_description', 'current_star',
                            'star1', 'star2', 'star3', 'star4', 'star5',
                            'star6', 'star7', 'star8', 'star9', 'star10'],
                      mode_cols=[],
                      max_cols=[],
                      current_cols=[],
                      time_col='',
                      merge_cols=[],
                      merge_on_cols=[],
                      include_IDs=True):
    '''returns pandas dataframe
       after aggregating data from the input reference dataframe
       sorts columns by column order
       and counts number of occurance of each officer in reference

       see test_merge_functions.py for tests and details
    '''
    # Initialize profiles data by aggregating input ref data
    profiles = aggregate_data(ref, uid,
                              mode_cols=mode_cols,
                              max_cols=max_cols,
                              current_cols=current_cols,
                              time_col=time_col,
                              merge_cols=merge_cols,
                              merge_on_cols=merge_on_cols)
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
    # If include_IDs is specified as True
    if include_IDs:
        # Collect _ID cols from profiles
        ID_cols = [col for col in profiles.columns
                   if col.endswith('_ID')]
    # If include_IDs is false
    else:
        ID_cols = []
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

       see test_merge_functions.py for tests and details
    '''
    rows = df.shape[0]
    df = df.merge(link_df[[uid_col, id_col]],
                  on=id_col, how='left')
    assert(df.shape[0] == rows), print('Missing rows!')
    return df


def merge_process(input_demo_file, input_full_file,
                  ref_df, profile_df,
                  args_dict, log,
                  intrafile_id, uid='UID'):
    '''returns tuple of reference df, profile df, and full df with uids
    '''
    log.info(('Processing file: {} as demographics file.'
              '').format(input_demo_file))
    sub_df = pd.read_csv(input_demo_file)
    atr_dict = append_to_reference(sub_df=sub_df,
                                   profile_df=profile_df,
                                   ref_df=ref_df,
                                   **args_dict)
    log.info('Append to reference complete.')
    ref_df = atr_dict['ref']
    log.info(('Officers Added: {}'
              '').format(len(unique(ref_df[uid])) - profile_df.shape[0]))

    if not profile_df.empty:
        log.info('Merge Report: {}'.format(atr_dict['MR']))
        log.info('Merge List: {}'.format(atr_dict['ML'].value_counts()))

    log.info('Starting to generate_profiles.')
    profile_df = generate_profiles(ref_df, uid,
                                   mode_cols=listdiff(ref_df.columns,
                                                      [uid]))
    log.info('Finised generate_profiles.')

    if input_full_file:
        log.info('Starting remerge to {} data'.format(input_full_file))

        full_df = pd.read_csv(input_full_file)
        assert intrafile_id in full_df.columns,\
            'No {} in input_full_file'.format(intrafile_id)

        full_df = remerge(full_df, profile_df,
                          uid, intrafile_id)
        log.info('Finished remerge.')

    else:
        log.warning('Missing input_full_file. No remerging step.\n'
                    'Will return an empty pandas dataframe instead of full_df')
        full_df = pd.DataFrame()

    return ref_df, profile_df, full_df


if __name__ == "__main__":
    import doctest
    doctest.testmod()
    doctest.run_docstring_examples(unique, globals())
    doctest.run_docstring_examples(intersect, globals())
    doctest.run_docstring_examples(listdiff, globals())
    doctest.run_docstring_examples(union, globals())
    doctest.run_docstring_examples(take_first_four, globals())
