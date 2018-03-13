#! usr/bin/env python3
#
# Author:   Roman Rivera (Invisible Institute)

'''pytest functions in assign_unique_ids_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import assign_unique_ids_functions
import copy
pd.options.mode.chained_assignment = None


def test_resolve_conflicts():
    '''test resolve_conflicts'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 2, 2,2],
         'B': [2, np.nan, 3, 4,4],
         'C': [np.nan, 5, np.nan, np.nan, 1]})
    orig_input_df = copy.deepcopy(input_df)
    input_args = {'id_cols': ['A'],
                  'conflict_cols': ['B', 'C'],
                  'uid': 'ID',
                  'start_uid': 10}
    output_df = pd.DataFrame(
        {'ID': [10, 10, 11, 12, 12],
         'A': [1, 1, 2, 2, 2],
         'B': [2, np.nan, 3, 4, 4],
         'C': [np.nan, 5, np.nan, np.nan, 1]},
        columns=['A', 'B', 'C', 'ID'])

    results = assign_unique_ids_functions.resolve_conflicts(input_df,**input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


def test_resolve_conflicts_unresolved():
    '''test resolve_conflicts with unresolved conflicts'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1,1],
         'B': [2, np.nan, 3, 4,np.nan],
         'C': [4, 4, 5, 5, 5]})
    orig_input_df = copy.deepcopy(input_df)

    input_args = {'id_cols': ['A'],
                  'conflict_cols': ['B', 'C'],
                  'uid': 'ID',
                  'start_uid': 1}
    output_df = pd.DataFrame(
        {'A': [1, 1, 1, 1,1],
         'B': [2, np.nan, 3, 4,np.nan],
         'C': [4, 4, 5, 5, 5],
         'ID': [1, 1, np.nan, np.nan, np.nan]},
        columns=['A', 'B', 'C', 'ID'])

    results = assign_unique_ids_functions.resolve_conflicts(input_df,**input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)

def test_assign_unique_ids():
    '''test assign_unique_ids
       does not test report generation
    '''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 2, 3, 3, 4, 4, 5, 5, 5],
         'B': [2, np.nan, 2, 3, 4, 4, 2, np.nan, 3, 4, 4],
         'C': [2, np.nan, np.nan, 3, 1, 1, np.nan, 5, np.nan, np.nan, np.nan]})
    orig_input_df = copy.deepcopy(input_df)
    input_args = {'uid': 'ID', 'id_cols': ['A'],
                  'conflict_cols': ['B', 'C'], 'log' : False}

    output_df = pd.DataFrame(
        {'ID': [3, 3, 3, 1, 2, 2, 4, 4, 5, 6, 6],
         'A': [1, 1, 1, 2, 3, 3, 4, 4, 5, 5, 5],
         'B': [2, np.nan, 2, 3, 4, 4, 2, np.nan, 3, 4, 4],
         'C': [2, np.nan, np.nan, 3, 1, 1, np.nan, 5, np.nan, np.nan, np.nan]})

    results = assign_unique_ids_functions.assign_unique_ids(input_df, **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


def test_assign_unique_ids_unresolved_distinct():
    '''test assign_unique_ids with unresolved_policy = 'distinct'
       does not test report generation
    '''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 2, 2, 2, 2],
         'B': [2, np.nan, 2, 3, 4, 4, 4],
         'C': [2, np.nan, np.nan, 1, 1, np.nan, 2]})
    orig_input_df = copy.deepcopy(input_df)
    input_args = {'uid': 'ID', 'id_cols': ['A'],
                  'conflict_cols': ['B', 'C'], 'log' : False,
                  'unresolved_policy' : 'distinct'}

    output_df = pd.DataFrame(
        {'A': [1, 1, 1, 2, 2, 2, 2],
         'B': [2, np.nan, 2, 3, 4, 4, 4],
         'C': [2, np.nan, np.nan, 1, 1, np.nan, 2],
         'ID': [1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0]})

    results = assign_unique_ids_functions.assign_unique_ids(input_df, **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


def test_assign_unique_ids_unresolved_same():
    '''test assign_unique_ids with unresolved_policy = 'same'
       does not test report generation
    '''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 2, 2, 2, 2, 3],
         'B': [2, np.nan, 3, 2, 3, 4, 4, 4, 3],
         'C': [2, np.nan, np.nan, np.nan, 1, 1, np.nan, 2, 3]})
    orig_input_df = copy.deepcopy(input_df)
    input_args = {'uid': 'ID', 'id_cols': ['A'],
                  'conflict_cols': ['B', 'C'], 'log' : False,
                  'unresolved_policy' : 'same'}

    output_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 2, 2, 2, 2, 3],
         'B': [2, np.nan, 3, 2, 3, 4, 4, 4, 3],
         'C': [2, np.nan, np.nan, np.nan, 1, 1, np.nan, 2, 3],
         'ID': [3.0, 3.0, 3.0, 3.0, 2.0, 4.0, 4.0, 4.0, 1.0]})

    results = assign_unique_ids_functions.assign_unique_ids(input_df, **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


# def test_assign_unique_ids_unresolved_manual():
#     '''test assign_unique_ids with unresolved_policy = 'manual'
#        does not test report generation
#     '''
#
#     input_df = pd.DataFrame(
#         {'A': [1, 1, 1, 1, 2, 2, 2, 2, 3],
#          'B': [2, np.nan, 3, 2, 4, 4, 4, 4, 3],
#          'C': [2, np.nan, 2, 1, 1, 1, np.nan, 2, 3]})
#     input_args = {'uid': 'ID', 'id_cols': ['A'],
#                   'conflict_cols': ['B', 'C'], 'log' : False,
#                   'unresolved_policy' : 'manual'}
#
#     output_df = pd.DataFrame(
#         {'A': [1, 1, 1, 1, 2, 2, 2, 2, 3],
#          'B': [2, np.nan, 3, 2, 4, 4, 4, 4, 3],
#          'C': [2, np.nan, 2, 1, 1, 1, np.nan, 2, 3.0],
#          'ID': [2.0, 2.0, 3.0, 2.0, 4.0, 4.0, 5.0, 6.0, 1.0]})
#     results = assign_unique_ids_functions.assign_unique_ids(input_df, **input_args)
#     assert results.equals(output_df)
# test_assign_unique_ids_unresolved_manual()

def test_order_aggregate():
    '''test order_aggregate'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 2, 2],
         'B': [2, 5, 3, 3, 2],
         'C': [6, 1, 3, 4, 1],
         'D': [3, 7, 1, np.nan, 2]})
    orig_input_df = copy.deepcopy(input_df)
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
    assert orig_input_df.equals(input_df)


def test_max_aggregate():
    '''test max_aggregate'''

    input_df = pd.DataFrame(
        {'A': [1, 1, 2, 2, 3],
         'B': [1, np.nan, 2, 5, np.nan]})
    orig_input_df = copy.deepcopy(input_df)
    input_args = {'uid': 'A',
                  'col': 'B'}
    output_df = pd.DataFrame(
        {'A': [1, 2],
         'B': [1.0, 5.0]})
    results = assign_unique_ids_functions.max_aggregate(input_df,
                                                        **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


def test_mode_aggregate():
    '''test mode_aggregate'''

    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3],
         'B': [5, np.nan, np.nan, np.nan, 5, 1, 'F', 'E', 'F', 'E', np.nan]})
    orig_input_df = copy.deepcopy(input_df)
    input_args = {'uid': 'A',
                  'col': 'B'}
    output_df = pd.DataFrame(
        {'A': [1, 2],
         'B': [5, 'E']})
    results = assign_unique_ids_functions.mode_aggregate(input_df,
                                                         **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


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
    orig_input_df = copy.deepcopy(input_df)
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
    assert orig_input_df.equals(input_df)
