# default merges: require first name, last name, appointed date
# if appointed date is null, require first name, last name, birth year
# if both appointed date and birth year are null, require first name, last name, 

from merge_data import Merge, MergePostProcessor
from foia_data import FoiaData
import pandas as pd
from typing import List, Callable
from functools import partial
import numpy as np
from fuzzywuzzy import fuzz
from general_utils import list_intersect
from filters import name_matching_ensemble

## filters ##
# note that these filters should all be MergePostProcessor type
# meaning they should take the same arguments as that type (see merge_data.py)
# a lot of these filters are converted to specific MergePostProcessors with partials specifying optional parameters
def multirow_merge_process(ref_unmerged: pd.DataFrame,
                           sup_unmerged: pd.DataFrame,
                           ref_id: int,
                           sup_id: int,
                           merged_df: pd.DataFrame,
                           required_cols: List[str]) -> pd.DataFrame:
    all_matched_columns = merged_df.assign(matched_on=merged_df.matched_on.str.split("-")) \
        .explode('matched_on') \
        .drop_duplicates() \
        .groupby([ref_id, sup_id]) \
        .agg(matched_on=('matched_on', '-'.join)) 
    
    required_cols = [col for col in required_cols if (col in sup_unmerged) and (col in ref_unmerged)]

    def get_matched_mask(df, required_cols):
        mask = pd.Series([True] * len(df), index=df.index)
        for col in required_cols:
            mask = mask & df.matched_on.str.contains(col)
        return mask
    
    multirow_match_mask = get_matched_mask(all_matched_columns, required_cols)

    return all_matched_columns[multirow_match_mask].reset_index()
    
def link_ref_and_sup(ref_unmerged: pd.DataFrame, 
                    sup_unmerged: pd.DataFrame,
                    ref_id: int,
                    sup_id: int,
                    merged_df: pd.DataFrame,
                    keep_cols: List[str] = None) -> pd.DataFrame:
    """Link reference and supplemental dataframes to merged dataframe
    Helper function used by some postprocess filters"""
    if not keep_cols:
        keep_cols = list_intersect(ref_unmerged.columns, sup_unmerged.columns)
        
    return merged_df.merge(ref_unmerged[keep_cols + [ref_id]], on=ref_id)\
        .merge(sup_unmerged[keep_cols + [sup_id]], on=sup_id, suffixes=['_ref', '_sup'])
    
def apply_score_func(ref_unmerged: pd.DataFrame, 
                    sup_unmerged: pd.DataFrame, 
                    ref_id: int, 
                    sup_id: int, 
                    merged_df: pd.DataFrame, 
                    col: str, 
                    score_func: Callable[[str, str], int]) -> pd.DataFrame:
    """Returns a function that scores a dataframe based on a string match of a column between dataframes, 
    linked with merged_df

    Parameters
    ----------
    col : str
        column to fuzzy match on
    score_func : Callable[[str, str], int]
        function that takes two strings and return's a score based on their similarity 
    threshold : int, optional
        threshold for string match, by default 0 to just see all scores

    Returns 
    -------
    DataFrameFilter
        function that filters a dataframe based on a fuzzy match of a column
        function adds a "score_col" column to the dataframe, returns only rows with score >= threshold
    """
    merged_with_columns = link_ref_and_sup(ref_unmerged, 
                                           sup_unmerged, 
                                           ref_id, 
                                           sup_id, 
                                           merged_df, 
                                           [col])
    
    return score_func(merged_with_columns) 

def filter_substring_names(ref_unmerged: pd.DataFrame, 
                           sup_unmerged: pd.DataFrame, 
                           ref_id: int,
                           sup_id: int,
                           merged_df: pd.DataFrame,
                           col: str) -> pd.DataFrame:
    """Returns a function that filters a dataframe based on a substring match of a column between dataframes, 
    linked with merged_df

    Parameters
    ----------
    col : str
        column to substring match on

    Returns 
    -------
    DataFrameFilter
        function that filters a dataframe based on a substring match of a column
    """
    merged_with_columns = link_ref_and_sup(ref_unmerged, sup_unmerged, ref_id, sup_id, merged_df, [col])
    ref_col = col + "_ref"
    sup_col = col + "_sup"

    substring_mask = merged_with_columns.apply(lambda row: (row[ref_col] in row[sup_col]) or 
                                               (row[sup_col] in row[ref_col]), axis=1)
    
    filtered_df = merged_with_columns[substring_mask][[ref_id, sup_id]].drop_duplicates()

    return merged_df.merge(filtered_df[[ref_id, sup_id]], on=[ref_id, sup_id]) 

def remove_sup_duplicate_merges_filter(ref_unmerged: pd.DataFrame,
                           sup_unmerged: pd.DataFrame,
                           ref_id: int,
                           sup_id: int,
                           merged_df: pd.DataFrame):
    """Remove duplicate matches from merged_df, i.e., where sup_id matches to multiple ref_ids"""
    duplicate_mask = merged_df.groupby(sup_id)[ref_id].transform('nunique') > 1

    return merged_df[~duplicate_mask]

def remove_ref_duplicate_merges_filter(ref_unmerged: pd.DataFrame,
                           sup_unmerged: pd.DataFrame,
                           ref_id: int,
                           sup_id: int,
                           merged_df: pd.DataFrame):
    """Remove duplicate matches from merged_df, i.e., where ref_id matches to multiple sup_ids"""
    duplicate_mask = merged_df.groupby(ref_id)[sup_id].transform('nunique') > 1

    return merged_df[~duplicate_mask]

def remove_duplicate_merges_filter(ref_unmerged: pd.DataFrame,
                           sup_unmerged: pd.DataFrame,
                           ref_id: int,
                           sup_id: int,
                           merged_df: pd.DataFrame):
    return remove_ref_duplicate_merges_filter(ref_unmerged, 
                                              sup_unmerged,
                                              ref_id,
                                              sup_id,
                                              remove_sup_duplicate_merges_filter(ref_unmerged, sup_unmerged, ref_id, sup_id, merged_df))

default_multirow_merge_process: MergePostProcessor = partial(multirow_merge_process, required_cols=['first_name_NS', 'last_name_NS', 'appointed_date'])

first_name_substring_filter: MergePostProcessor = partial(filter_substring_names, col='first_name_NS')
last_name_substring_filter: MergePostProcessor = partial(filter_substring_names, col='last_name_NS')

first_name_phonetic_filter: MergePostProcessor = partial(filter_substring_names)

## default merges ##
base_merge_dict = {'star': ['star', ''],
                    'first_name': ['first_name_NS', 'F4FN'],
                    'last_name': ['last_name_NS', 'F4LN'],
                    'appointed_date': ['appointed_date'],
                    'birth_year': ['birth_year', 'current_age', 'current_age_m1', 'current_age_p1', ''],
                    'middle_initial': ['middle_initial', ''],
                    'gender': ['gender', ''],
                    'race': ['race', ''],
                    'suffix_name': ['suffix_name', ''],
                    'current_unit': ['current_unit', '']}

base_merge = Merge(name="base", merge_dict=base_merge_dict)

multirow_merge = Merge(name="multirow merges",
                       merge_dict={**base_merge_dict,
                                   'last_name': ['last_name_NS', 'F4LN', ''],
                                   'appointed_date': ['appointed_date', '']},
                       filter_reference_merges_flag=False,
                       filter_supplemental_merges_flag=False,
                       merge_postprocess=default_multirow_merge_process)

null_appointed_date_reference_merge = Merge(name='null appointed date (reference) merge',
                                            merge_dict={**base_merge_dict, 
                                             'appointed_date': ['appointed_date', ''],
                                             'birth_year': ['birth_year', 'current_age']},
                                             reference_preprocess='appointed_date_null')

null_appointed_date_supplemental_merge = Merge(name='null appointed date (supplemental) merge',
                                               merge_dict={**base_merge_dict,
                                                           'appointed_date': ['appointed_date', ''],
                                                           'birth_year': ['birth_year', 'current_age']},
                                                supplemental_preprocess='appointed_date_null')

null_appointed_date_and_birth_reference_merge = Merge(name='null appointed date and birth year reference merge',
                                            merge_dict={**base_merge_dict,
                                                        'appointed_date': ['appointed_date', ''],
                                                        'birth_year': ['birth_year', 'current_age', '']},
                                            reference_preprocess='appointed_date_null and birth_year_null',)

null_appointed_date_and_birth_supplemental_merge = Merge(name='null appointed date and birth year supplemental merge',
                                                         merge_dict={**base_merge_dict,
                                                              'appointed_date': ['appointed_date', ''],
                                                              'birth_year': ['birth_year', 'current_age', '']},
                                                    supplemental_preprocess='appointed_date_null and birth_year_null')

married_merge = Merge(name='married - last name change merge',
                        merge_dict={**base_merge_dict, 
                                  'last_name': ['last_name_NS', 'F4LN', ''],
                                  'star': ['star']},
                        query="gender == 'FEMALE'")

last_name_substring_merge = Merge(name='last name substring merge',
                                  merge_dict={**base_merge_dict,
                                              'last_name': ['last_name_NS', 'L4LN', '']},
                                    merge_postprocess=last_name_substring_filter)

default_merges = [base_merge, multirow_merge, null_appointed_date_reference_merge, null_appointed_date_supplemental_merge, 
                  null_appointed_date_and_birth_reference_merge, null_appointed_date_and_birth_supplemental_merge,
                  married_merge, last_name_substring_merge]

first_name_substring_merge = Merge(name='first name substring merge',
                                   merge_dict={**base_merge_dict,
                                               'first_name': ['first_name_NS', 'F4FN', '']},
                                    merge_postprocess=first_name_substring_filter)

# first_name_fuzzy_match_merge = Merge(name='first name fuzzy match merge',
#                                      merge_dict={**base_merge_dict,
#                                                 'first_name': ['first_name_NS', 'F4FN', '']},
#                                     merge_postprocess=first_name_fuzzy_match_filter)

# last_name_fuzzy_match_merge = Merge(name='last name fuzzy match merge',
#                                      merge_dict={**base_merge_dict,
#                                                 'last_name': ['last_name_NS', 'F4LN', '']},
#                                     merge_postprocess=last_name_fuzzy_match_filter)

def view_possible_matches(ref_unmerged: pd.DataFrame, 
                           sup_unmerged: pd.DataFrame, 
                           ref_id: int,
                           sup_id: int,
                           merged_df: pd.DataFrame) -> pd.DataFrame:
    """Formats a dataframe for viewing possible matches,
    including all columns in ref_unmerged and sup_unmerged, ordered in a sensible way

    Most likely matches should be near the top, based on merge_dict and generate_on_list
    """

    cols = list_intersect(ref_unmerged.columns, sup_unmerged.columns)

    relaxed_merges = link_ref_and_sup(ref_unmerged, sup_unmerged, ref_id, sup_id, merged_df, cols)

    # prefer this order of columns, but include all columns after
    ordered_cols = ['first_name_NS', 'last_name_NS', 'middle_initial', 'appointed_date', 
                    'birth_year', 'current_age', 'gender', 'race', 'current_unit', 'current_rank']

    cols = [col for col in ordered_cols if col in cols] + [col for col in cols if col not in ordered_cols]

    suffixed_cols = ['matched_on', ref_id, sup_id] + [f"{col}_{suffix}" for col in cols for suffix in ['ref', 'sup']]
    other_cols = [col for col in relaxed_merges.columns if col not in suffixed_cols]

    return relaxed_merges[suffixed_cols + other_cols]
    

# very lax merge, just for manual testing, does not check for duplicates, do not actually use
relaxed_test_merge = Merge(name='relaxed test merge', 
                        merge_dict={**base_merge_dict,
                                    'first_name': ['first_name_NS', 'F4FN', ''],
                                    'last_name': ['last_name_NS', 'L4LN'],
                                    'appointed_date': ['appointed_date', '']},
                        merge_postprocess=view_possible_matches,
                        check_duplicates=False)

# whitelist postprocess
def whitelist_postprocess(ref_unmerged: pd.DataFrame,
                        sup_unmerged: pd.DataFrame, 
                        ref_id: int,
                        sup_id: int,
                        merged_df: pd.DataFrame, 
                        whitelist_df: pd.DataFrame) -> pd.DataFrame:
    """Postprocess function for custom merges"""
    return merged_df.merge(whitelist_df, on=['matched_on', ref_id, sup_id])
    

# use in conjunction with relaxed_test_merge to view possible matches and manually add them here
def whitelist_merge(whitelist: pd.DataFrame | dict, ref_id: int, sup_id: int) -> Merge:
    """Build a custom merge from a list of possible merges and a set of whitelisted indexes
    """
    if isinstance(whitelist, dict):
        whitelist_df = pd.DataFrame(whitelist)
    else:
        whitelist_df = whitelist

    custom_merges = [matched_on.split("-") for matched_on in whitelist_df.matched_on.unique()]
    custom_whitelist_postprocess: MergePostProcessor = partial(whitelist_postprocess, whitelist_df=whitelist_df)
    return Merge(name='manual/custom merges',
                 merge_dict={},
                 custom_merges=custom_merges,
                 merge_postprocess=custom_whitelist_postprocess)


def date_offset_postprocess(ref_unmerged: pd.DataFrame,
                            sup_unmerged: pd.DataFrame, 
                            ref_id: int,
                            sup_id: int,
                            merged_df: pd.DataFrame, 
                            date_col: str,
                            date_offset: pd.Timedelta) -> pd.DataFrame:
    """Checks if date column within a specified offset for valid merge
    I.e., check if appointed date in ref is within a day of appointed date in sup
    """
    merged_with_columns = link_ref_and_sup(ref_unmerged, sup_unmerged, ref_id, sup_id, merged_df, [date_col])
    ref_col = date_col + "_ref"
    sup_col = date_col + "_sup"

    date_offset_mask = (merged_with_columns[ref_col] - merged_with_columns[sup_col]).abs() <= date_offset

    return merged_df.merge(merged_with_columns.loc[date_offset_mask, [ref_id, sup_id]].drop_duplicates(), 
                           on=[ref_id, sup_id]) 

appointed_date_day_offset: MergePostProcessor = partial(date_offset_postprocess, date_col='appointed_date', date_offset=pd.Timedelta(days=1))

def crosswalk_postprocess(ref_unmerged: pd.DataFrame,
                          sup_unmerged: pd.DataFrame, 
                          ref_id: int,
                          sup_id: int,
                          merged_df: pd.DataFrame) -> pd.DataFrame:
    """Postprocess function for crosswalk merges"""
    return merged_df.merge(sup_unmerged[[sup_id, 'matched_on', 'foia_name']].drop_duplicates(), on=[sup_id, 'matched_on', 'foia_name'])


def crosswalk_merge(sup_unmerged: pd.DataFrame) -> Merge:
    """For crosswalk between releases: splits out matched on from new release to create a custom merge going to the old release"""
    custom_merges = [[col for col in matched_on.split("-") if col in sup_unmerged.columns] 
                     for matched_on in sup_unmerged.query('matched_on != ""').matched_on.unique()]

    return Merge(name='crosswalk merge',
                 merge_dict={},
                 custom_merges=custom_merges,
                 filter_reference_merges_flag=False,
                 filter_supplemental_merges_flag=False,
                 merge_postprocess=crosswalk_postprocess,
                 check_duplicates=False)