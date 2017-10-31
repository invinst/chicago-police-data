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
                  'minimum': False}

    output_df = pd.DataFrame(
        {'A': [1, 2],
         'B': [5, 2],
         'C': [1, 1]})

    results = assign_unique_ids_functions.order_aggregate(input_df,
                                                          **input_args)
    assert results.equals(output_df)


def test_max_aggregate():
    '''test max_aggregate'''

    input_df = pd.DataFrame(
        {'A': [1, 1, 2, 2, 3],
         'B': [1, np.nan, 2, 5, np.nan]})
    input_args = {'uid': 'A',
                  'col': 'B'}
    output_df = pd.DataFrame(
        {'A': [1, 2],
         'B': [1.0, 5.0]})
    results = assign_unique_ids_functions.max_aggregate(input_df,
                                                        **input_args)
    assert results.equals(output_df)


def test_mode_aggregate():
    '''test mode_aggregate'''

    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3],
         'B': [5, np.nan, np.nan, np.nan, 5, 1, 'E', 'F', 'F', 'E', np.nan]})
    input_args = {'uid': 'A',
                  'col': 'B'}
    output_df = pd.DataFrame(
        {'A': [1, 2],
         'B': [5, 'E']})
    results = assign_unique_ids_functions.mode_aggregate(input_df,
                                                         **input_args)
    assert results.equals(output_df)


def test_aggregate_data():
    ''' test aggregate_data'''
    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'C', 'C'],
         'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'max': [1, np.nan, 10, 1, 3, 9, 2, 2, -2, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})

    input_args = {'uid': 'uid',
                  'id_cols': ['ID'],
                  'mode_cols': ['mode'],
                  'max_cols': ['max'],
                  'current_cols': ['age'],
                  'time_col': 'date_of_age_obs',
                  'merge_cols': ['max_names'],
                  'merge_on_cols': ['max']}
 
    output_df = pd.DataFrame(
        {'uid': [1, 4, 99],
         'ID': ['A', 'B', 'C'],
         'mode': [2.0, 1.0, 5.0],
         'max': [10, 2, np.nan],
         'current_age': [23, 57, np.nan],
         'max_names': ['Ten', 'Two', np.nan]},
        columns=['uid', 'ID', 'mode', 'max', 'current_age', 'max_names'])

    results = assign_unique_ids_functions.aggregate_data(input_df,
                                                         **input_args)
    assert results.equals(output_df)
