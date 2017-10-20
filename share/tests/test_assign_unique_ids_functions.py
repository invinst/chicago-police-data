#! usr/bin/env python3
#
# Author:   Roman Rivera

'''pytest functions in clean_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import assign_unique_ids_functions


def test_remove_duplicates():
    '''test remove_duplicates'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1],
         'B': [2, 2, 3, 4],
         'C': [1, 3, 1, 3]})
    output_df = pd.DataFrame(
        {'A': [1, 1],
         'B': [3, 4],
         'C': [1, 3]},
        index=[2, 3])

    results = assign_unique_ids_functions.remove_duplicates(input_df,
                                                            ['A', 'B'])
    assert results.equals(output_df)


def test_keep_duplicates():
    '''test keep_duplicates'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1],
         'B': [2, 2, 3, 4],
         'C': [1, 3, 1, 3]})
    output_df = pd.DataFrame(
        {'A': [1, 1],
         'B': [2, 2],
         'C': [1, 3]},
        index=[0, 1])

    results = assign_unique_ids_functions.keep_duplicates(input_df,
                                                          ['A', 'B'])
    assert results.equals(output_df)


def test_resolve_conflicts():
    '''test resolve_conflicts'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 2, 2],
         'B': [2, np.nan, 3, 4],
         'C': [np.nan, 5, np.nan, np.nan]})
    input_args = {'id_cols': ['A'],
                  'conflict_cols': ['B', 'C'],
                  'uid': 'ID',
                  'starting_uid': 10}

    output_df = pd.DataFrame(
        {'ID': [11, 11, 12, 13],
         'A': [1, 1, 2, 2],
         'B': [2, np.nan, 3, 4],
         'C': [np.nan, 5, np.nan, np.nan]},
        columns=['ID', 'A', 'B', 'C'],
        index=[0, 1, 0, 1])

    results = assign_unique_ids_functions.resolve_conflicts(input_df,
                                                            **input_args)
    assert results.equals(output_df)


def test_assign_unique_ids():
    '''test assign_unique_ids
       does not test report generation
    '''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 2, 3, 3, 4, 4, 5, 5],
         'B': [2, np.nan, 2, 3, 4, 4, 2, np.nan, 3, 4],
         'C': [2, np.nan, np.nan, 3, 1, 1, np.nan, 5, np.nan, np.nan]})
    input_args = {'uid': 'ID', 'id_cols': ['A'], 'conflict_cols': ['B', 'C']}

    output_df = pd.DataFrame(
        {'ID': [3, 3, 3, 1, 2, 2, 4, 4, 5, 6],
         'A': [1, 1, 1, 2, 3, 3, 4, 4, 5, 5],
         'B': [2, np.nan, 2, 3, 4, 4, 2, np.nan, 3, 4],
         'C': [2, np.nan, np.nan, 3, 1, 1, np.nan, 5, np.nan, np.nan]})

    results = assign_unique_ids_functions.assign_unique_ids(input_df,
                                                            **input_args)
    results = results[0]
    assert results.equals(output_df)


def test_order_aggregate():
    '''test order_aggregate'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 2, 2],
         'B': [2, 5, 3, 3, 2],
         'C': [6, 1, 3, 4, 1],
         'D': [3, 7, 1, np.nan, 2]})
    input_args = {'id_cols': ['A'],
                  'agg_cols': ['B', 'C'],
                  'order_cols': ['D'],
                  'minimum':False}

    output_df = pd.DataFrame(
        {'A': [1, 2],
         'B': [5, 2],
         'C': [1, 1]})

    results = assign_unique_ids_functions.order_aggregate(input_df,
                                                          **input_args)
    assert results.equals(output_df)
