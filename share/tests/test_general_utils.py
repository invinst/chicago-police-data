#! usr/bin/env python3
#
# Author:   Roman Rivera (Invisible Institute)

'''pytest functions in assign_unique_ids_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import general_utils

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

    results = general_utils.remove_duplicates(input_df,
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

    results = general_utils.keep_duplicates(input_df,
                                            ['A', 'B'])
    assert results.equals(output_df)

def test_keep_conflicts_all():
    '''test keep_conflicts with all=True'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 2, 10, 11, 12],
         'B': [2, 2, 3, 4, 3, 10, 3, 12],
         'C': [1, 3, 1, 3, 3, 10, 11, 1]})
    input_args = {'cols' : ['A','B'], 'all_dups' : True}

    output_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 2, 11],
         'B': [2, 2, 3, 4, 3, 3],
         'C': [1, 3, 1, 3, 3, 11]},
        index=[0, 1, 2, 3, 4, 6],
        columns=['A','B','C'])

    results = general_utils.keep_conflicts(input_df, **input_args)

    assert results.equals(output_df)

def test_keep_conflicts_not_all():
    '''test keep_conflicts with all=False'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 2, 10, 11],
         'B': [2, 2, 3, 4, 3, 10, 3],
         'C': [1, 3, 1, 3, 3, 10, 11]})
    input_args = {'cols' : ['A', 'B'], 'all_dups' : False}

    output_df = pd.DataFrame(
        {'A': [1, 1, 1],
         'B': [2, 2, 3],
         'C': [1, 3, 1]},
        index=[0, 1, 2],
        columns=['A','B','C'])

    results = general_utils.keep_conflicts(input_df, **input_args)
    assert results.equals(output_df)


def test_reshape_data():
    '''test reshape_data'''
    input_df = pd.DataFrame(
        {'A': [1, 2, 3,4],
         'B1': [1,2, np.nan, 5,],
         'B2': [1,np.nan,np.nan, 6],
         'C' : [10,11,12, 13]})
    input_args = {'reshape_col' : 'B', 'id_col' : 'A'}

    output_df = pd.DataFrame(
        {'A': [1, 2, 4, 4, 3],
         'B': [1, 2, 5,6, np.nan],
         'C': [10, 11, 13, 13, 12]},
        columns=['A','B','C'])

    results = general_utils.reshape_data(input_df, **input_args)
    assert results.equals(output_df)


def test_fill_data():
    '''test fill_data'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', 'BOB', 'AN', 'ANNIE', 'ANNA'],
         'B': ['M', np.nan, np.nan, 'L', 'L'],
         'C': ['JR', np.nan, np.nan, np.nan, np.nan],
         'D': ['JONES', 'JONES' , 'SMITH', 'SMITH', 'ORIELY'],
         'E': [10, 50, 20, 2, np.nan]})
    input_args = {'id_col' : 'id'}
    output_df = pd.DataFrame(
        {'id' : [1,1, 2, 2, 2, 2, 3],
         'A': ['BOB','BOB', 'AN', 'AN', 'ANNA','ANNA', 'ANNIE'],
         'B': ['M','M', 'L', 'L', 'L', 'L', 'L'],
         'C': ['JR','JR', np.nan, np.nan, np.nan, np.nan, np.nan],
         'D': ['JONES','JONES', 'SMITH', 'ORIELY', 'SMITH', 'ORIELY', 'SMITH'],
         'E' : [10.0, 50.0, 20.0, 20.0, 20.0, 20.0, 2.0]})
    results = general_utils.fill_data(input_df, **input_args)
    assert results.equals(output_df)
