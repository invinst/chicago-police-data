#! usr/bin/env python3
#
# Author:   Roman Rivera

'''pytest functions in clean_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import merge_functions

def test_add_columns_F4CAMMS():
    '''test add_columns with add_cols options:
       F4FN, F4LN, current_age, min_max_stars
    '''
    input_df = pd.DataFrame(
        {'first_name_NS': ['BOB', 'JEDGAR'],
         'last_name_NS': ['JONES', 'COB'],
         'current_age': [57.0, 66.0],
         'star1': [1234.0, 5678.0],
         'star2': [10981.0, 198.0],
         'star3': [2919.0, 30040.0],
         'star4': [5600.0, np.nan],
         'star5': [np.nan, np.nan],
         'star6': [np.nan, np.nan],
         'star7': [np.nan, np.nan],
         'star8': [np.nan, np.nan],
         'star9': [np.nan, np.nan],
         'star10': [np.nan, np.nan]
         })
    input_args = {'add_cols': ['F4FN', 'F4LN', 'current_age',
                               'min_max_stars']}
    output_df = pd.DataFrame(
        {'first_name_NS': ['BOB', 'JEDGAR'],
         'last_name_NS': ['JONES', 'COB'],
         'current_age': [57.0, 66.0],
         'star1': [1234.0, 5678.0],
         'star2': [10981.0, 198.0],
         'star3': [2919.0, 30040.0],
         'star4': [5600.0, np.nan],
         'star5': [np.nan, np.nan],
         'star6': [np.nan, np.nan],
         'star7': [np.nan, np.nan],
         'star8': [np.nan, np.nan],
         'star9': [np.nan, np.nan],
         'star10': [np.nan, np.nan],
         'F4FN': ['BOB', 'JEDG'],
         'F4LN': ['JONE', 'COB'],
         'current_age_p1': [57.0, 66.0],
         'current_age_m1': [57.0, 66.0],
         'min_star': [1234.0, 198.0],
         'max_star': [10981.0, 30040.0]})

    results = merge_functions.add_columns(input_df,
                                          **input_args)

    assert len(set(results.columns) - set(output_df.columns)) == 0
    output_df = output_df[results.columns]
    assert results.equals(output_df)


def test_add_columns_L4BYS():
    '''test add_columns with add_cols options:
       L4FN, L4LN, BY_to_CA, stars
    '''
    input_df = pd.DataFrame(
        {'first_name_NS': ['BOB', 'JEDGAR'],
         'last_name_NS': ['JONES', 'COB'],
         'birth_year': [1960.0, 1950.0],
         'current_star': [2919.0, 30040.0]})
    input_args = {'add_cols': ['L4FN', 'L4LN', 'BY_to_CA',
                               'stars'],
                  'current_age_from': 2017,
                  'end_star': 3}
    output_df = pd.DataFrame(
        {'first_name_NS': ['BOB', 'JEDGAR'],
         'last_name_NS': ['JONES', 'COB'],
         'birth_year': [1960.0, 1950.0],
         'current_star': [2919.0, 30040.0],
         'L4FN': ['BOB', 'DGAR'],
         'L4LN': ['ONES', 'COB'],
         'current_age_p1': [57.0, 67.0],
         'current_age_m1': [56.0, 66.0],
         'star1': [2919.0, 30040.0],
         'star2': [2919.0, 30040.0],
         'star3': [2919.0, 30040.0]})

    results = merge_functions.add_columns(input_df,
                                          **input_args)

    assert len(set(results.columns) - set(output_df.columns)) == 0
    output_df = output_df[results.columns]
    assert results.equals(output_df)


def test_generate_on_lists():
    '''test generate_on_lists'''
    input_data_cols = ['A1', 'A2', 'B1', 'B2', 'C1']
    input_base_lists = [['A1', 'A2', ''], ['B1', 'B2'], ['D1', 'D2']]
    input_drop_cols = ['B1']
    output_on_list = [['B2', 'A2'], ['B2', 'A1'], ['B2']]

    results = merge_functions.generate_on_lists(input_data_cols,
                                                input_base_lists,
                                                input_drop_cols)
    assert results == output_on_list
