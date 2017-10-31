#! usr/bin/env python3
#
# Author:   Roman Rivera

'''pytest functions in clean_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import unit_history_functions

def test_combine_histories():
    '''test combine_histories'''
    input_uh_list = [
        pd.DataFrame({
            'UID': [1, 1, 1,
                    2, 2, 2],
            'unit': [10, 15, 10, 6, 7, 8],
            'unit_start_date': ['2010-01-01', '2011-01-01', np.nan,
                                '2015-01-01', '2016-01-01', '2017-01-01'],
            'unit_end_date': ['2010-12-31', np.nan, '2017-02-01',
                              '2015-12-31', '2016-12-31', np.nan]}),
        pd.DataFrame({
            'UID': [1, 1, 1,
                    2, 2, 2,
                    3, 3],
            'unit': [9, 15, 10, 6, 7, 8, 20, 19],
            'unit_start_date': ['2008-01-01', '2011-01-01', np.nan,
                                '2015-01-01', '2016-01-01', '2017-01-01',
                                '2017-03-01', '2016-01-01'],
            'unit_end_date': ['2009-12-31', np.nan, np.nan,
                              '2015-12-31', '2016-12-31', np.nan,
                              np.nan, '2015-01-01']})
    ]

    input_resignation_df = pd.DataFrame(
            {'UID': [1, 2, 3],
             'resignation_date': ['2017-01-01', '2017-02-01', np.nan]})
    input_args = {'uid_col': 'UID',
                  'unit_col': 'unit',
                  'start_col': 'unit_start_date',
                  'end_col': 'unit_end_date',
                  'resignation_col': 'resignation_date'}
    output_df = pd.DataFrame(
        {'UID': [1, 1, 1, 2, 2, 2, 3, 3],
         'unit': [9, 10, 15, 6, 7, 8, 19, 20],
         'unit_start_date': ['2008-01-01', '2010-01-01', '2011-01-01',
                             '2015-01-01', '2016-01-01', '2017-01-01',
                             '2016-01-01', '2017-03-01'],
         'unit_end_date': ['2009-12-31', '2010-12-31', '2017-01-01',
                           '2015-12-31', '2016-12-31', '2017-02-01',
                           np.nan, np.nan]})

    results = unit_history_functions.combine_histories(input_uh_list,
                                                       input_resignation_df,
                                                       **input_args)
    assert len(set(results.columns) - set(output_df.columns)) == 0
    output_df = output_df[results.columns]
    assert results.equals(output_df)


def test_to_first_dates_year():
    '''test to_first_dates for years'''
    input_dates = pd.to_datetime(pd.Series(['2000-12-31',
                                            '2014-02-20',
                                            '1999-12-31']))
    input_args = {'delta_type': 'AS'}
    output_array = np.array(['2000', '2014', '1999'],
                            dtype='datetime64[Y]')

    results = unit_history_functions.to_first_dates(input_dates,
                                                    **input_args)
    assert np.array_equal(results, output_array)


def test_to_first_dates_month():
    '''test to_first_dates for months'''
    input_dates = pd.to_datetime(pd.Series(['2000-12-31',
                                            '2014-02-20',
                                            '1999-12-31']))
    input_args = {'delta_type': 'MS'}
    output_array = np.array(['2000-12', '2014-02', '1999-12'],
                            dtype='datetime64[M]')

    results = unit_history_functions.to_first_dates(input_dates,
                                                    **input_args)
    assert np.array_equal(results, output_array)


def test_to_first_dates_day():
    '''test to_first_dates for days'''
    input_dates = pd.to_datetime(pd.Series(['2000-12-31',
                                            '2014-02-20',
                                            '1999-12-31']))
    input_args = {'delta_type': 'D'}
    output_array = np.array(['2000-12-31', '2014-02-20', '1999-12-31'],
                            dtype='datetime64[D]')

    results = unit_history_functions.to_first_dates(input_dates,
                                                    **input_args)
    assert np.array_equal(results, output_array)


def test_expand_history_day():
    '''test expand_history with freq = day'''
    input_args = {
        'sd': '1999-12-30',
        'ed': '2000-01-02',
        'unit': 10,
        'uid': 1,
        'freq': 'D'}
    output_df = pd.DataFrame(
        {'date': pd.to_datetime(['1999-12-30', '1999-12-31',
                                 '2000-01-01', '2000-01-02']),
         'event': [1, 0, 0, 2],
         'unit': [10, 10, 10, 10],
         'UID': [1, 1, 1, 1]})

    results = unit_history_functions.expand_history(**input_args)
    assert results.equals(output_df)


def test_expand_history_month():
    '''test expand_history with freq = month'''
    input_args = {
        'sd': '1999-11-01',
        'ed': '2000-02-01',
        'unit': 10,
        'uid': 1,
        'freq': 'MS'}
    output_df = pd.DataFrame(
        {'date': pd.to_datetime(['1999-11-01', '1999-12-01',
                                 '2000-01-01', '2000-02-01']),
         'event': [1, 0, 0, 2],
         'unit': [10, 10, 10, 10],
         'UID': [1, 1, 1, 1]})

    results = unit_history_functions.expand_history(**input_args)
    assert results.equals(output_df)


def test_expand_history_year():
    '''test expand_history with freq = year'''
    input_args = {
        'sd': '1998-01-01',
        'ed': '2001-01-01',
        'unit': 10,
        'uid': 1,
        'freq': 'AS'}
    output_df = pd.DataFrame(
        {'date': pd.to_datetime(['1998-01-01', '1999-01-01',
                                 '2000-01-01', '2001-01-01']),
         'event': [1, 0, 0, 2],
         'unit': [10, 10, 10, 10],
         'UID': [1, 1, 1, 1]})

    results = unit_history_functions.expand_history(**input_args)
    assert results.equals(output_df)


def test_history_to_panel_day():
    '''test history_to_panel for frequency = day'''
    input_df = pd.DataFrame(
        {'UID': [1, 1, 2, 2],
         'unit': [10, 15, 7, 8],
         'unit_start_date': ['1999-12-31', '2000-01-03',
                             '1999-01-30', '1999-12-31'],
         'unit_end_date': ['2000-01-02', np.nan,
                           '1999-12-30', '2000-01-02']})
    input_args = {'frequency': 'day',
                  'max_date': '2000-01-04',
                  'min_date': '1999-12-31',
                  'start_col': 'unit_start_date',
                  'end_col': 'unit_end_date',
                  'unit_col': 'unit',
                  'uid_col': 'UID'}
    output_df = pd.DataFrame({
        'UID': [1, 1, 1, 1, 1, 2, 2, 2],
        'event': [1, 0, 2, 1, 2, 1, 0, 2],
        'day': pd.to_datetime(['1999-12-31', '2000-01-01', '2000-01-02',
                               '2000-01-03', '2000-01-04',
                               '1999-12-31', '2000-01-01', '2000-01-02']),
        'unit': [10, 10, 10, 15, 15, 8, 8, 8]})

    results = unit_history_functions.history_to_panel(input_df,
                                                      **input_args)
    assert results.equals(output_df)


def test_history_to_panel_month():
    '''test history_to_panel for frequency = month'''
    input_df = pd.DataFrame(
        {'UID': [1, 1, 2, 2],
         'unit': [10, 15, 7, 8],
         'unit_start_date': ['1999-11-01', '2000-03-01',
                             '1999-01-01', '1999-12-01'],
         'unit_end_date': ['2000-02-01', np.nan,
                           '1999-11-01', '2000-01-01']})
    input_args = {'frequency': 'month',
                  'max_date': '2000-03-01',
                  'min_date': '1999-12-01',
                  'start_col': 'unit_start_date',
                  'end_col': 'unit_end_date',
                  'unit_col': 'unit',
                  'uid_col': 'UID'}
    output_df = pd.DataFrame({
        'UID': [1, 1, 1, 2, 2, 1],
        'event': [0, 0, 2, 1, 2, 3],
        'month': pd.to_datetime(['1999-12-01', '2000-01-01', '2000-02-01',
                                 '1999-12-01', '2000-01-01',
                                 '2000-03-01']),
        'unit': [10, 10, 10, 8, 8, 15]})

    results = unit_history_functions.history_to_panel(input_df,
                                                      **input_args)
    assert results.equals(output_df)


def test_history_to_panel_year():
    '''test history_to_panel for frequency = year'''
    input_df = pd.DataFrame(
        {'UID': [1, 1, 2, 2],
         'unit': [10, 15, 7, 8],
         'unit_start_date': ['1998-01-01', '2001-01-01',
                             '1990-01-01', '1999-01-01'],
         'unit_end_date': ['2000-01-01', np.nan,
                           '1998-01-01', '1999-01-01']})
    input_args = {'frequency': 'year',
                  'max_date': '2003-01-01',
                  'min_date': '1999-01-01',
                  'start_col': 'unit_start_date',
                  'end_col': 'unit_end_date',
                  'unit_col': 'unit',
                  'uid_col': 'UID'}
    output_df = pd.DataFrame({
        'UID': [1, 1, 1, 1, 1, 2],
        'event': [0, 2, 1, 0, 2, 3],
        'year': pd.to_datetime(['1999-01-01', '2000-01-01',
                                '2001-01-01', '2002-01-01', '2003-01-01',
                                '1999-01-01']),
        'unit': [10, 10, 15, 15, 15, 8]})

    results = unit_history_functions.history_to_panel(input_df,
                                                      **input_args)
    assert results.equals(output_df)
