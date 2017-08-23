#!usr/bin/env python3
#
# Author: Roman Rivera

'''functions used to merge datasets'''

import itertools
import pandas as pd


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


def remove_duplicates(df, cols=[]):
    '''returns pandas dataframe keeping only rows
       that do not have duplicated values in specified cols
    '''
    if not cols:
        cols = df.columns.tolist()
    return df[~df.duplicated(subset=cols, keep=False)].sort_values(cols)


def keep_duplicates(df, cols):
    '''returns pandas dataframe keeping only rows
       that have duplicated values in specified cols
    '''
    return df[df.duplicated(subset=cols, keep=False)].sort_values(cols)


def add_columns(df,
                add_cols=["F4FN", "F4LN", "Current.Age", "BY_to_CA", "Stars"],
                current_year=2016):
    '''returns pandas dataframe with columns added on
       depending on the specified add_cols and the columns
       in the input dataframe.
    '''
    if "F4FN" in add_cols and "First.Name" in df.columns:
        df['F4FN'] = df['First.Name'].map(take_first_four)
    if "F4LN" in add_cols and 'Last.Name' in df.columns:
        df['F4LN'] = df['Last.Name'].map(take_first_four)

    if "Current.Age" in add_cols and "Current.Age" in df.columns:
        df['Current.Age.p1'] = df['Current.Age']
        df['Current.Age.m1'] = df['Current.Age']

    if "BY_to_CA" in add_cols and "Birth.Year" in df.columns:
        df['Current.Age.p1'] = \
            df['Birth.Year'].map(lambda x: current_year - x)
        df['Current.Age.m1'] = \
            df['Birth.Year'].map(lambda x: current_year - x - 1)

    if ('Stars' in add_cols and
            'Current.Star' in df.columns and
            'Star1' not in df.columns):
        for i in range(1, 11):
            df['Star{}'.format(i)] = df['Current.Star']
    return df


def generate_on_lists(data_cols, base_lists):
    '''returns list of lists composed of every possible combination
       of each value in the sub lists of base_lists, so long as those
       values are included in the data_cols
    '''
    merge_lists = []
    for col_list in base_lists:
        if intersect(col_list, data_cols):
            merge_list = intersect(col_list, data_cols)
            if '' in col_list:
                merge_list.append('')
            merge_lists.append(sorted(merge_list, reverse=True))

    merge_lists = list(itertools.product(*reversed(merge_lists)))
    merge_lists = [[i for i in ml if i != ''] for ml in merge_lists]

    return merge_list


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
    dfm = pd.DataFrame(columns=keep_columns + ['Match'])
    df1_rows = df1.shape[0]
    df2_rows = df2.shape[0]
    for merge_cols in on_lists:
        df1t = remove_duplicates(df1[keep_columns[:1] + merge_cols],
                                 merge_cols)
        df2t = remove_duplicates(df2[keep_columns[1:] + merge_cols],
                                 merge_cols)
        dfmt = df1t.merge(df2t, on=merge_cols, how='inner')
        if dfmt.shape[0] > 0:
            if print_merging:
                print(('{0} Matches on \n'
                       '{1} columns').format(dfmt.shape[0],
                                             merge_cols))
            dfmt['Match'] = '-'.join(merge_cols)
            dfm = dfm.append(dfmt[keep_columns +
                                  ['Match']].reset_index(drop=True))
            df1 = df1.loc[~df1[keep_columns[0]].isin(dfm[keep_columns[0]])]
            df2 = df2.loc[~df2[keep_columns[1]].isin(dfm[keep_columns[1]])]

    merge_report = generate_merge_report(dfm.shape[0],
                                         df1_rows,
                                         df2_rows)
    print(merge_report)

    return_dict = {'merged': dfm.reset_index(drop=True)}
    if return_unmatched:
        return_dict['UM1'] = df1
        return_dict['UM2'] = df2
    if return_merge_report:
        return_dict['MR'] = merge_report

    return return_dict


def merge_datasets(df1, df2, keep_columns, custom_matches=[],
                   no_match_cols=[], min_match_length=4,
                   expand_stars=False, return_unmatched=True,
                   return_merge_report=True, print_merging=False):
    '''returns dictionary from loop_merge
       automates adding columns and merging list creation
    '''
    df1 = df1.dropna(axis=1, how='all')
    df2 = df2.dropna(axis=1, how='all')
    add_cols = ['F4FN', 'F4LN']

    if "Birth.Year" not in intersect(df1.columns, df2.columns):
        add_cols.extend(["BY_to_CA", "Current.Age"])
    if 'Star1' not in intersect(df1.columns, df2.columns) and expand_stars:
        add_cols.append('Stars')

    df1 = add_columns(df1, add_cols)
    df2 = add_columns(df2, add_cols)

    cols = intersect(df1.columns, df2.columns)

    df1 = df1[[col for col in df1.columns
               if col in cols or col == keep_columns[0]]]
    df2 = df2[[col for col in df2.columns
               if col in cols or col == keep_columns[1]]]

    base_lists = [
        ['Current.Star', 'Star1', 'Star2', 'Star3', 'Star4',
         'Star5', 'Star6', 'Star7', 'Star8', 'Star9', 'Star10'],
        ['First.Name', 'F4FN'],
        ['Last.Name', 'F4LN'],
        ['Appointed.Date'],
        ['Birth.Year', 'Current.Age', 'Current.Age.p1', 'Current.Age.m1', ''],
        ['Middle.Initial', ''],
        ['Gender', ''],
        ['Race', ''],
        ['Suffix.Name', ''],
        ['Current.Unit', '']
    ]

    on_lists = generate_on_lists(cols, base_lists)

    for nmc in no_match_cols:
        nmc_lists = generate_on_lists(cols,
                                      [ml for ml in base_lists
                                       if nmc not in ml])
        nmc_lists = [nmcl for nmcl in nmc_lists
                     if len(nmcl) >= min_match_length]
        on_lists.extend(nmc_lists)

    if custom_matches:
        on_lists.extend(custom_matches)

    merged_data = loop_merge(df1, df2,
                             on_lists=on_lists,
                             keep_columns=keep_columns,
                             return_unmatched=return_unmatched,
                             return_merge_report=return_merge_report,
                             print_merging=print_merging)

    return(merged_data)


def append_to_reference(sub_df, profile_df, ref_df,
                        custom_matches=[], return_unmatched=False,
                        min_match_length=4, no_match_cols=[],
                        return_merge_report=True, print_merging=False,
                        return_merge_list=True, expand_stars=False):
    '''returns dictionary including at least a pandas dataframe
       appends merged and unmerged results from merge_datasets
       on to the input ref_df
    '''
    return_dict = {'ref': None,
                   'UM1': None,
                   'UM2': None,
                   'MR': None,
                   'ML': None}

    if profile_df.empty:
        sub_df.insert(0, 'UID', sub_df.index + 1)
        return_dict['ref'] = sub_df
    else:
        id_col = [col for col in sub_df.columns if col.endswith('_ID')][0]
        keep_columns = ['UID', id_col]

        md_dict = merge_datasets(profile_df, sub_df,
                                 keep_columns=keep_columns,
                                 custom_matches=custom_matches,
                                 no_match_cols=no_match_cols,
                                 min_match_length=min_match_length,
                                 return_merge_report=return_merge_report,
                                 expand_stars=expand_stars,
                                 print_merging=print_merging)

        ref = pd.concat([md_dict['merged'][keep_columns],
                        md_dict['UM1'][[keep_columns[0]]],
                        md_dict['UM2'][[keep_columns[1]]]])[
              keep_columns].reset_index(drop=True)

        ref = ref.sort_values('UID', na_position='last').reset_index(drop=True)
        ref['UID'] = ref.index + 1
        sub_df = sub_df.merge(ref[unique([keep_columns[1], 'UID'])],
                              on=keep_columns[1], how='left')
        ref_df = pd.concat([ref_df, sub_df]).reset_index(drop=True)

        return_dict['ref'] = ref_df
        if return_unmatched:
            return_dict['UM1'] = md_dict['UM1']
            return_dict['UM2'] = md_dict['UM2']
        if return_merge_report:
            return_dict['MR'] = md_dict['MR']
        if return_merge_list:
            return_dict['ML'] = md_dict['merged']['Match']

    return return_dict


def remerge(df, link_df, uid_col, id_col):
    '''returns pandas dataframe after merging new unique ids
       taken from a link_df
    '''
    rows = df.shape[0]
    df = df.merge(link_df[[uid_col, id_col]],
                  on=id_col, how='left')
    assert(df.shape[0] == rows), print('Missing rows!')
    return df
