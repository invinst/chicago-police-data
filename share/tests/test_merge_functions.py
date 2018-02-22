#! usr/bin/env python3
#
# Author:   Roman Rivera (Invisible Institute)

'''pytest functions in merge_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import logging
from merge_functions import ReferenceData
from general_utils import *
import itertools

log = logging.getLogger('test')

def test_one_to_one_True():
    '''tests merging for one_to_one merging'''
    input_df = pd.DataFrame(
        {'data_id' : [10, 20, 30, 1, 109],
         'first_name_NS': ['BOB', 'BOB', 'KATHLEEN', 'KEVIN', 'ELLEN'],
         'middle_initial': ['M', np.nan, np.nan, 'J', 'L'],
         'suffix_name': ['SR', 'JR', np.nan, 'II', np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'SMITH', 'PARK', 'ORIELY'],
         'star1': [10, 50, 20, 2, np.nan],
         'star2': [20, 51, np.nan, 4, np.nan],
         'star3': [30, np.nan, np.nan, 10, np.nan],
         'merge': [1, 1, 1, 0, 1]})
    input_args = {'uid': 'UID', 'data_id': 'data_id',
                  'log': log, 'starting_uid' : 5}

    RD = ReferenceData(input_df, **input_args)


    def test_initialize_ReferenceData():
        '''test initializing ReferenceData'''
        output_ref_df = pd.DataFrame(
            {'data_id' : [10, 10, 10, 20, 20, 30, 109],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATHLEEN', 'ELLEN'],
             'middle_initial': ['M', 'M', 'M', np.nan, np.nan, np.nan, 'L'],
             'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR', np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY'],
             'star' : [10, 20, 30, 50, 51, 20, np.nan],
             'UID' : [5, 5, 5, 6, 6, 7, 8]})

        results = RD.ref_df
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_initialize_ReferenceData()

    input_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'birth_year' : [1970, 1990, 1985, 1965, 1986],
         'middle_initial': [np.nan, 'M', 'C', 'L', 'E'],
         'suffix_name': ['SR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'current_star': [np.nan, 51, 20, 100, 192]})

    RD = RD.add_sup_data(input_sup_df,
                         add_cols=[
                         "F4FN",
                        {'id': '',
                         'exec' : "df['L4LN'] = df['last_name_NS'].map(lambda x: x[-4:None])"}])

    def test_add_sup_data_sup_df():
        '''test added supplementary data'''
        output_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'birth_year' : [1970, 1990, 1985, 1965, 1986],
         'middle_initial': [np.nan, 'M', 'C', 'L', 'E'],
         'suffix_name': ['SR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'current_star': [np.nan, 51, 20, 100, 192],
         'star': [np.nan, 51, 20, 100, 192]})

        results = RD.sup_df
        output_sup_df = output_sup_df[results.columns]
        assert results.equals(output_sup_df)
    test_add_sup_data_sup_df()

    def test_add_sup_data_sup_um():
        '''test added unmerged supplementary data'''
        output_sup_um = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'F4FN' : ['BOB', 'BOB', 'KATH', 'ELLE', 'JENN'],
         'birth_year' : [1970, 1990, 1985, 1965, 1986],
         'middle_initial': [np.nan, 'M', 'C', 'L', 'E'],
         'suffix_name': ['SR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'L4LN': ['ONES', 'ONES' , 'RANT', 'IELY', 'ONES'],
         'current_star': [np.nan, 51, 20, 100, 192],
         'star': [np.nan, 51, 20, 100, 192]})

        results = RD.sup_um
        output_sup_um = output_sup_um[results.columns]
        assert results.equals(output_sup_um)
    test_add_sup_data_sup_um()

    def test_add_sup_data_ref_um():
        '''test unmerged reference data'''
        output_ref_um = pd.DataFrame(
            {'data_id' : [10, 10, 10, 20, 20, 30, 109],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATHLEEN', 'ELLEN'],
             'middle_initial': ['M', 'M', 'M', np.nan, np.nan, np.nan, 'L'],
             'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR', np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY'],
             'F4FN': ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATH', 'ELLE'],
             'L4LN': ['ONES', 'ONES', 'ONES', 'ONES', 'ONES', 'MITH', 'IELY'],
             'star' : [10, 20, 30, 50, 51, 20, np.nan],
             'UID' : [5, 5, 5, 6, 6, 7, 8]})

        results = RD.ref_um
        output_ref_um = output_ref_um[results.columns]
        assert results.equals(output_ref_um)
    test_add_sup_data_ref_um()

    RD = RD.loop_merge(custom_merges=[["first_name_NS", "L4LN", "middle_initial"],
                                     {'cols' : ["F4FN", "star"],
                                      'query' : 'F4FN=="KATH"'}])

    def test_loop_merge_on_lists():
        '''test on_lists for loop_merge'''
        output_on_lists = [['star', 'first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['star', 'first_name_NS', 'last_name_NS', 'middle_initial'],
                           ['star', 'first_name_NS', 'last_name_NS', 'suffix_name'],
                           ['star', 'first_name_NS', 'last_name_NS'],
                           ['star', 'F4FN', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['star', 'F4FN', 'last_name_NS', 'middle_initial'],
                           ['star', 'F4FN', 'last_name_NS', 'suffix_name'],
                           ['star', 'F4FN', 'last_name_NS'],
                           ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['first_name_NS', 'last_name_NS', 'middle_initial'],
                           ['first_name_NS', 'last_name_NS', 'suffix_name'],
                           ['first_name_NS', 'last_name_NS'],
                           ['F4FN', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['F4FN', 'last_name_NS', 'middle_initial'],
                           ['F4FN', 'last_name_NS', 'suffix_name'],
                           ['F4FN', 'last_name_NS'],
                           ['first_name_NS', 'L4LN', 'middle_initial'],
                           {'cols' : ["F4FN", "star"],
                            'query' : 'F4FN=="KATH"'}]

        results = RD.on_lists
        assert results == output_on_lists
    test_loop_merge_on_lists()

    def test_loop_merge_merged_df():
        '''test merged data from loop_merge'''
        output_merged_df = pd.DataFrame(
            {'UID' : [6, 5, 8, 7],
             'sub__2016_ID' : [2, 1, 4, 3],
             'matched_on' : ['star-first_name_NS-last_name_NS',
                             'first_name_NS-last_name_NS-suffix_name',
                             'first_name_NS-L4LN-middle_initial',
                             'F4FN-star']

            }).astype(str)
        results = RD.merged_df.astype(str)
        output_merged_df = output_merged_df[results.columns]
        assert results.equals(output_merged_df)
    test_loop_merge_merged_df()

    def test_loop_merge_ref_um():
        '''test unmerged reference data from loop_merge'''
        results = RD.ref_um
        assert results.empty
    test_loop_merge_ref_um()

    def test_loop_merge_sup_um():
        '''test unmerged supplementary data from loop_merge'''
        output_sup_um = pd.DataFrame(
            [{'sub__2016_ID' : 5,
             'first_name_NS' : 'JENNA',
             'last_name_NS' : 'JONES',
             'middle_initial' : 'E',
             'star' : 192.0,
             'suffix_name' : np.nan,
             'F4FN' : 'JENN',
             'L4LN' : 'ONES'}], index=[4])
        results = RD.sup_um.astype(str)
        output_sup_um = output_sup_um[results.columns].astype(str)
        assert results.equals(output_sup_um)
    test_loop_merge_sup_um()

    RD = RD.append_to_reference(drop_cols=['current_star'])


    def test_append_to_reference_ref_df_drop_cols():
        '''test reference data after append_to_reference'''
        output_ref_df = pd.DataFrame(
            {'data_id' : [10, 10, 10, 20, 20, 30, 109,
                          np.nan, np.nan, np.nan, np.nan, np.nan],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATHLEEN', 'ELLEN',
                               'BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
             'middle_initial': ['M', 'M', 'M', np.nan, np.nan, np.nan, 'L',
                                np.nan, 'M', 'C', 'L', 'E'],
             'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR', np.nan, np.nan,
                             'SR', np.nan, np.nan, np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY',
                              'JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
             'star' : [10, 20, 30, 50, 51, 20, np.nan,
                       np.nan, 51, 20, 100, 192],
             'birth_year': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
                            1970, 1990, 1985, 1965, 1986],
             'UID' : [5, 5, 5, 6, 6, 7, 8,
                      5, 6, 7, 8, 9],
             'sub__2016_ID' : [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
                               1, 2, 3, 4, 5]})
        results = RD.ref_df
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_append_to_reference_ref_df_drop_cols()

    def test_remerge_to_file():
        csv_opts = {'index': False}
        input_df = pd.DataFrame({
            'sub__2016_ID' :[1,2,3,4,5,6],
            'event' : [1, 0, 0, 0, 0, 1]
        }, columns=['sub__2016_ID', 'event'])
        input_path = 'test_remerge_to_file_input.csv'
        output_path = 'test_remerge_to_file_output.csv'
        test_output_path = 'test_remerge_to_file_test.csv'
        input_df.to_csv(input_path, **csv_opts)
        RD.remerge_to_file(input_path, output_path, csv_opts)

        output_df = pd.DataFrame({
            'sub__2016_ID' :[1,2,3,4,5,6],
            'event' : [1, 0, 0, 0, 0, 1],
            'UID' : [5,6,7,8,9,np.nan]
        }, columns=['sub__2016_ID', 'event', 'UID'])
        output_df.to_csv(test_output_path, **csv_opts)
        import filecmp
        assert filecmp.cmp(test_output_path,
                           output_path)
        import os
        os.system('rm %s %s %s'
                  % (input_path, output_path, test_output_path))
    test_remerge_to_file()


def test_one_to_one_False():
    '''tests merging for one_to_many merging'''
    input_df = pd.DataFrame(
        {'data_id' : [10, 30, 109],
         'first_name_NS': ['BOB', 'KATHLEEN', 'ELLEN'],
         'middle_initial': ['M', np.nan, 'L'],
         'suffix_name': ['JR', np.nan, np.nan],
         'last_name_NS': ['JONES', 'SMITH', 'ORIELY'],
         'star1': [10, 20, np.nan],
         'star2': [20, np.nan, np.nan],
         'star3': [30, np.nan, np.nan]})

    input_args = {'uid' : 'UID', 'data_id' : 'data_id', 'log' : log}
    RD = ReferenceData(input_df, **input_args)

    def test_initialize_ReferenceData():
        """test initializing ReferenceData
        """
        output_ref_df = pd.DataFrame(
            {'data_id' : [10, 10, 10, 30, 109],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'KATHLEEN', 'ELLEN'],
             'middle_initial': ['M', 'M', 'M', np.nan, 'L'],
             'suffix_name': ['JR', 'JR', 'JR', np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY'],
             'star' : [10, 20, 30, 20, np.nan],
             'UID' : [1, 1, 1, 2, 3]})

        results = RD.ref_df
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_initialize_ReferenceData()


    input_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'middle_initial': ['M', np.nan, np.nan, 'L', 'E'],
         'suffix_name': [np.nan, 'JR', np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'star': [10, 20, 20, 100, 192]})

    RD = RD.add_sup_data(input_sup_df, add_cols=["F4FN", "L4LN"],
                        base_OD = [
                            ('star',['star', '']),
                            ('first_name',['first_name_NS', 'F4FN']),
                            ('last_name', ['last_name_NS', 'L4LN']),
                            ('middle_initial', ['middle_initial', '']),
                            ('suffix_name',['suffix_name', ''])])

    def test_add_sup_data_sup_df():
        '''test added supplementary data'''
        output_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'middle_initial': ['M', np.nan, np.nan, 'L', 'E'],
         'suffix_name': [np.nan, 'JR', np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'current_star': [np.nan, 51, 20, 100, 192],
         'star': [10, 20, 20, 100, 192]})
        results = RD.sup_df
        output_sup_df = output_sup_df[results.columns]
        assert results.equals(output_sup_df)
    test_add_sup_data_sup_df()


    def test_add_sup_data_sup_um():
        '''test added unmerged supplementary data'''
        output_sup_um = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'F4FN' : ['BOB', 'BOB', 'KATH', 'ELLE', 'JENN'],
         'middle_initial': ['M', np.nan, np.nan, 'L', 'E'],
         'suffix_name': [np.nan, 'JR', np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'L4LN': ['ONES', 'ONES' , 'RANT', 'IELY', 'ONES'],
         'current_star': [np.nan, 51, 20, 100, 192],
         'star': [10, 20, 20, 100, 192]})

        results = RD.sup_um
        output_sup_um = output_sup_um[results.columns]
        assert results.equals(output_sup_um)
    test_add_sup_data_sup_um()


    def test_add_sup_data_ref_um():
        '''test unmerged reference data'''
        output_ref_um = pd.DataFrame(
            {'data_id' : [10, 10, 10, 30, 109],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'KATHLEEN', 'ELLEN'],
             'middle_initial': ['M', 'M', 'M', np.nan, 'L'],
             'suffix_name': ['JR', 'JR', 'JR', np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY'],
             'star' : [10, 20, 30, 20, np.nan],
             'F4FN': ['BOB', 'BOB', 'BOB', 'KATH', 'ELLE'],
             'L4LN': ['ONES', 'ONES', 'ONES', 'MITH', 'IELY'],
             'UID' : [1, 1, 1, 2, 3]})

        results = RD.ref_um
        output_ref_um = output_ref_um[results.columns]
        assert results.equals(output_ref_um)
    test_add_sup_data_ref_um()


    RD = RD.loop_merge(custom_merges=[['F4FN', 'star']],
                       one_to_one=False)

    def test_loop_merge_on_lists():
        '''test on_lists for loop_merge'''
        output_on_lists = [['star', 'first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['star', 'first_name_NS', 'last_name_NS', 'middle_initial'],
                           ['star', 'first_name_NS', 'last_name_NS', 'suffix_name'],
                           ['star', 'first_name_NS', 'last_name_NS'],
                           ['star', 'first_name_NS', 'L4LN', 'middle_initial', 'suffix_name'],
                           ['star', 'first_name_NS', 'L4LN', 'middle_initial'],
                           ['star', 'first_name_NS', 'L4LN', 'suffix_name'],
                           ['star', 'first_name_NS', 'L4LN'],
                           ['star', 'F4FN', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['star', 'F4FN', 'last_name_NS', 'middle_initial'],
                           ['star', 'F4FN', 'last_name_NS', 'suffix_name'],
                           ['star', 'F4FN', 'last_name_NS'],
                           ['star', 'F4FN', 'L4LN', 'middle_initial', 'suffix_name'],
                           ['star', 'F4FN', 'L4LN', 'middle_initial'],
                           ['star', 'F4FN', 'L4LN', 'suffix_name'],
                           ['star', 'F4FN', 'L4LN'],
                           ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['first_name_NS', 'last_name_NS', 'middle_initial'],
                           ['first_name_NS', 'last_name_NS', 'suffix_name'],
                           ['first_name_NS', 'last_name_NS'],
                           ['first_name_NS', 'L4LN', 'middle_initial', 'suffix_name'],
                           ['first_name_NS', 'L4LN', 'middle_initial'],
                           ['first_name_NS', 'L4LN', 'suffix_name'],
                           ['first_name_NS', 'L4LN'],
                           ['F4FN', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['F4FN', 'last_name_NS', 'middle_initial'],
                           ['F4FN', 'last_name_NS', 'suffix_name'],
                           ['F4FN', 'last_name_NS'],
                           ['F4FN', 'L4LN', 'middle_initial', 'suffix_name'],
                           ['F4FN', 'L4LN', 'middle_initial'],
                           ['F4FN', 'L4LN', 'suffix_name'],
                           ['F4FN', 'L4LN'],
                           ['F4FN', 'star']]
        results = RD.on_lists
        assert results == output_on_lists
    test_loop_merge_on_lists()

    def test_loop_merge_merged_df():
        '''test merged data from loop_merge'''
        output_merged_df = pd.DataFrame(
            {'UID' : [1, 1, 3, 2],
             'sub__2016_ID' : [1, 2, 4, 3],
             'matched_on' : ['star-first_name_NS-last_name_NS-middle_initial',
                             'star-first_name_NS-last_name_NS-suffix_name',
                             'first_name_NS-L4LN-middle_initial',
                             'F4FN-star']

            }).astype(str)
        results = RD.merged_df.astype(str)
        output_merged_df = output_merged_df[results.columns]
        assert results.equals(output_merged_df)
    test_loop_merge_merged_df()

    def test_loop_merge_ref_um():
        '''test unmerged reference data from loop_merge'''
        output_ref_um = pd.DataFrame({'data_id' : [10, 10, 10, 30, 109],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'KATHLEEN', 'ELLEN'],
             'middle_initial': ['M', 'M', 'M', np.nan, 'L'],
             'suffix_name': ['JR', 'JR', 'JR', np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY'],
             'star' : [10, 20, 30, 20, np.nan],
             'F4FN': ['BOB', 'BOB', 'BOB', 'KATH', 'ELLE'],
             'L4LN': ['ONES', 'ONES', 'ONES', 'MITH', 'IELY'],
             'UID' : [1, 1, 1, 2, 3]})

        results = RD.ref_um
        output_ref_um = output_ref_um[results.columns]
        assert results.equals(output_ref_um)
    test_loop_merge_ref_um()


    def test_loop_merge_sup_um():
        '''test unmerged supplementary data from loop_merge'''
        output_sup_um = pd.DataFrame(
            [{'sub__2016_ID' : 5,
             'first_name_NS' : 'JENNA',
             'last_name_NS' : 'JONES',
             'middle_initial' : 'E',
             'star' : 192,
             'suffix_name' : np.nan,
             'F4FN' : 'JENN',
             'L4LN' : 'ONES'}], index=[4])

        results = RD.sup_um.astype(str)
        output_sup_um = output_sup_um[results.columns].astype(str)
        assert results.equals(output_sup_um)
    test_loop_merge_sup_um()


    RD = RD.append_to_reference(keep_sup_um=False)


    def test_append_to_reference_ref_df_keep_sup_um():
        '''test reference data after append_to_reference'''
        output_ref_df = pd.DataFrame(
            {'data_id' : [10, 10, 10, 30, 109,
                          np.nan, np.nan, np.nan, np.nan],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'KATHLEEN', 'ELLEN',
                               'BOB', 'BOB', 'KATHY', 'ELLEN'],
             'middle_initial': ['M', 'M', 'M', np.nan, 'L',
                                'M', np.nan, np.nan, 'L'],
             'suffix_name': ['JR', 'JR', 'JR', np.nan, np.nan,
                             np.nan, 'JR', np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY',
                              'JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY'],
             'star' : [10, 20, 30, 20, np.nan,
                       10, 20, 20, 100],
             'UID' : [1, 1, 1, 2, 3,
                      1, 1, 2, 3],
             'sub__2016_ID' : [np.nan, np.nan, np.nan, np.nan, np.nan,
                               1, 2, 3, 4]})
        results = RD.ref_df
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_append_to_reference_ref_df_keep_sup_um()


def test_fill_data():
    '''tests merging for fill_data merging'''
    input_df = pd.DataFrame(
        {'data_id' : [1, 1, 2, 3, 2],
         'first_name_NS': ['BOB', 'BOB', 'AN', 'ANNIE', 'ANNA'],
         'middle_initial': ['M', np.nan, np.nan, 'L', 'L'],
         'suffix_name': ['JR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'SMITH', 'SMITH', 'ORIELY'],
         'star': [10, 50, 20, 2, np.nan]})
    input_args = {'uid' : 'UID', 'data_id' : 'data_id', 'log' : log}

    RD = ReferenceData(input_df, **input_args)


    def test_initialize_ReferenceData():
        '''test initializing ReferenceData'''
        output_ref_df = pd.DataFrame(
            {'data_id' : [1, 1, 2, 2,3],
             'first_name_NS': ['BOB', 'BOB', 'AN', 'ANNA', 'ANNIE'],
             'middle_initial': ['M', np.nan, np.nan, 'L', 'L'],
             'suffix_name': ['JR', np.nan, np.nan, np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES' , 'SMITH', 'ORIELY', 'SMITH'],
             'star': [10, 50, 20, np.nan,2],
             'UID' : [1,1,2,2,3]})

        results = RD.ref_df
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_initialize_ReferenceData()

    input_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3],
         'first_name_NS': ['BOB',  'AN', 'AN'],
         'middle_initial': [np.nan, 'L', 'L'],
         'suffix_name': [np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'ORIELY', 'SMITH'],
         'current_star': [50, 20, np.nan]})

    RD = RD.add_sup_data(input_sup_df, add_cols=["F2FN"],
                        fill_cols=['first_name_NS', 'middle_initial',
                                      'suffix_name', 'last_name_NS', 'star'])
    def test_add_sup_data_sup_df():
        '''test added supplementary data'''
        output_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3],
         'first_name_NS': ['BOB',  'AN', 'AN'],
         'middle_initial': [np.nan, 'L', 'L'],
         'suffix_name': [np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'ORIELY', 'SMITH'],
         'current_star': [50, 20, np.nan],
         'star': [50, 20, np.nan]})

        results = RD.sup_df
        output_sup_df = output_sup_df[results.columns]
        assert results.equals(output_sup_df)
    test_add_sup_data_sup_df()

    def test_add_sup_data_sup_um():
        '''test added unmerged supplementary data'''
        output_sup_um = pd.DataFrame(
            {'sub__2016_ID' : [1, 2, 3],
             'first_name_NS': ['BOB',  'AN', 'AN'],
             'F2FN': ['BO',  'AN', 'AN'],
             'middle_initial': [np.nan, 'L', 'L'],
             'suffix_name': [np.nan, np.nan, np.nan],
             'last_name_NS': ['JONES', 'ORIELY', 'SMITH'],
             'current_star': [50, 20, np.nan],
             'star': [50, 20, np.nan]})

        results = RD.sup_um
        output_sup_um = output_sup_um[results.columns]
        assert results.equals(output_sup_um)
    test_add_sup_data_sup_um()

    def test_add_sup_data_ref_um():
        '''test unmerged reference data'''
        output_ref_um = pd.DataFrame(
            {'UID' : [1,1, 2, 2, 2, 2, 3],
             'first_name_NS': ['BOB','BOB', 'AN', 'AN', 'ANNA','ANNA', 'ANNIE'],
             'middle_initial': ['M','M', 'L', 'L', 'L', 'L', 'L'],
             'suffix_name': ['JR','JR', np.nan, np.nan, np.nan, np.nan, np.nan],
             'last_name_NS': ['JONES','JONES', 'SMITH', 'ORIELY', 'SMITH', 'ORIELY', 'SMITH'],
             'F2FN': ['BO','BO', 'AN', 'AN', 'AN','AN', 'AN'],
             'star' : [10.0, 50.0, 20.0, 20.0, 20.0, 20.0, 2.0]})

        results = RD.ref_um
        output_ref_um = output_ref_um[results.columns]

        assert results.equals(output_ref_um)
    test_add_sup_data_ref_um()

    RD = RD.loop_merge(custom_merges=[['F2FN', 'last_name_NS', 'middle_initial']])

    def test_loop_merge_on_lists():
        '''test on_lists for loop_merge'''
        output_on_lists = [['star', 'first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['star', 'first_name_NS', 'last_name_NS', 'middle_initial'],
                           ['star', 'first_name_NS', 'last_name_NS', 'suffix_name'],
                           ['star', 'first_name_NS', 'last_name_NS'],
                           ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name'],
                           ['first_name_NS', 'last_name_NS', 'middle_initial'],
                           ['first_name_NS', 'last_name_NS', 'suffix_name'],
                           ['first_name_NS', 'last_name_NS'],
                           ['F2FN', 'last_name_NS', 'middle_initial']]
        results = RD.on_lists
        assert results == output_on_lists
    test_loop_merge_on_lists()

    def test_loop_merge_merged_df():
        '''test merged data from loop_merge'''
        output_merged_df = pd.DataFrame(
            {'UID' : [2, 1,3],
             'sub__2016_ID' : [2, 1,3],
             'matched_on' : ['star-first_name_NS-last_name_NS-middle_initial',
                             'star-first_name_NS-last_name_NS',
                             'F2FN-last_name_NS-middle_initial']

            }).astype(str)
        results = RD.merged_df.astype(str)
        output_merged_df = output_merged_df[results.columns]
        assert results.equals(output_merged_df)
    test_loop_merge_merged_df()

    def test_loop_merge_ref_um():
        '''test unmerged reference data from loop_merge'''
        results = RD.ref_um
        assert results.empty
    test_loop_merge_ref_um()

    def test_loop_merge_sup_um():
        '''test unmerged supplementary data from loop_merge'''
        results = RD.sup_um
        assert results.empty
    test_loop_merge_sup_um()

    RD = RD.append_to_reference()

    def test_append_to_reference_ref_df():
        '''test reference data after append_to_reference'''
        output_ref_df = pd.DataFrame(
            {'data_id' : [1, 1, 2, 2, 3, np.nan, np.nan, np.nan],
             'first_name_NS': ['BOB', 'BOB', 'AN', 'ANNA', 'ANNIE', 'BOB', 'AN', 'AN'],
             'middle_initial': ['M', np.nan, np.nan, 'L', 'L',
                                np.nan, 'L', 'L'],
             'suffix_name': ['JR', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'SMITH', 'ORIELY', 'SMITH', 'JONES', 'ORIELY', 'SMITH'],
             'star' : [10, 50, 20, np.nan, 2, 50, 20, np.nan],
             'current_star': [np.nan, np.nan, np.nan, np.nan, np.nan, 50, 20, np.nan],
             'UID' : [1, 1, 2,2, 3, 1, 2, 3],
             'sub__2016_ID' : [np.nan, np.nan, np.nan, np.nan, np.nan, 1, 2,3]})
        results = RD.ref_df
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_append_to_reference_ref_df()


def test_final_profiles():
    '''test final profiles creation'''
    input_df = pd.DataFrame({
        'ID' : [1, 1, 1, 2, 2, 3, 3],
        'name' : ['MIKE', 'MICHAEL', 'MICHAEL', 'JANE', 'JAN', 'BOB', 'BOB'],
        'age' : [25, 24, 24, 30, 31, 40, 42],
        'rank': ['SGT', 'PO', 'PO', 'DET', 'PO', 'PO', 'PA'],
        'fid1__2016-09_ID': [np.nan, np.nan, 10, np.nan, 34, 40, np.nan],
        'fid2__2017-01_ID':[51, np.nan, np.nan, 13, np.nan, np.nan, np.nan],
        'fid3__2015-01_ID': [np.nan, 2, np.nan, np.nan, np.nan, np.nan, 10111]
    })
    input_args = {
    'aggregate_data_args' : {
        'current_cols': ['rank'],
        'time_col' : 'foia_date',
        'mode_cols': ['name'],
        'max_cols': ['age']},
    'column_order' : ['current_rank', 'age', 'name']}
    output_df = pd.DataFrame({
        'ID' : [1,2,3],
        'current_rank' : ['SGT', 'DET', 'PO'],
        'age' : [25, 31, 42],
        'name' : ['MICHAEL', 'JAN', 'BOB'],
        'profile_count' : [3, 2, 2]},
        columns=['ID', 'current_rank', 'age', 'name', 'profile_count'])

    results = ReferenceData(input_df, uid='ID', log=log)\
        .final_profiles(**input_args)\
        .profiles
    assert results.equals(output_df)
