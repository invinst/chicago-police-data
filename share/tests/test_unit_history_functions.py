#! usr/bin/env python3
#
# Author:   Roman Rivera (Invisible Institute)

'''pytest functions in unit_history_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import unit_history_functions
import logging
from datetime import datetime
TODAY = datetime.today().date()
log = logging.getLogger('test')

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
    input_args = {'uid': 'UID',
                  'unit': 'unit',
                  'start': 'unit_start_date',
                  'end': 'unit_end_date',
                  'resignation_col': 'resignation_date',
                  'log' : log}
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
                  'start': 'unit_start_date',
                  'end': 'unit_end_date',
                  'unit': 'unit',
                  'uid': 'UID',
                  'log' : log}
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
                  'start': 'unit_start_date',
                  'end': 'unit_end_date',
                  'unit': 'unit',
                  'uid': 'UID',
                  'log' : log}
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
                  'start': 'unit_start_date',
                  'end': 'unit_end_date',
                  'unit': 'unit',
                  'uid': 'UID',
                  'log' : log}
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


def test_check_overlaps_True():
    ''' test check_overlaps True'''
    input_df = pd.DataFrame({
        'unit_start_date' : pd.to_datetime(['2001-01-02', '2001-01-01']),
        'unit_end_date' : pd.to_datetime(['2001-01-04', '2004-01-01'])
    })
    assert unit_history_functions.check_overlaps(input_df) == True

def test_check_overlaps_False():
    ''' test check_overlaps False'''
    input_df = pd.DataFrame({
        'unit_start_date' : pd.to_datetime(['2001-01-02', '2000-01-01', '2007-01-02']),
        'unit_end_date' : pd.to_datetime(['2001-01-04', '2001-01-01', pd.NaT])
    })
    assert unit_history_functions.check_overlaps(input_df) == False


def test_check_overlaps_NaT_True():
    ''' test check_overlaps with NaT end but False'''
    input_df = pd.DataFrame({
        'unit_start_date' : pd.to_datetime(['2000-01-01', '2001-01-01']),
        'unit_end_date' : pd.to_datetime([pd.NaT, '2001-01-02'])
    })
    assert unit_history_functions.check_overlaps(input_df) == False

def test_check_overlaps_NaT_False():
    ''' test check_overlaps with NaT end but False'''
    input_df = pd.DataFrame({
        'unit_start_date' : pd.to_datetime(['2001-01-02', '2000-01-01']),
        'unit_end_date' : pd.to_datetime([pd.NaT, '2001-01-01'])
    })
    assert unit_history_functions.check_overlaps(input_df) == False



def test_resolve_units_case1():
    ''' tests resolve_units case 1'''
    input_df = pd.read_csv("unit_history_files/case1.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [9.0, 3.0, 8.0],
            'unit_start_date' : pd.to_datetime(['2006-05-25', '2016-02-07', '2016-02-28']),
            'unit_end_date' : pd.to_datetime(['2016-02-06', '2016-02-27', TODAY]),
            'CODE' : [1, 1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case2():
    ''' tests resolve_units case 2'''
    input_df = pd.read_csv("unit_history_files/case2.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [123.0],
            'unit_start_date' : pd.to_datetime(['1983-02-03']),
            'unit_end_date' : pd.to_datetime([TODAY]),
            'CODE' : [0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case3():
    ''' tests resolve_units case 3'''
    input_df = pd.read_csv("unit_history_files/case3.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [44.0, 6.0, 620.0, 2.0],
            'unit_start_date' : pd.to_datetime(['1990-08-27', '1992-02-06', '2000-09-01', '2016-06-05']),
            'unit_end_date' : pd.to_datetime(['1992-02-05', '2000-08-31', '2016-06-04', TODAY]),
            'CODE' : [1, 1, 1, 0]},
            columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case4():
    ''' tests resolve_units case 4'''
    input_df = pd.read_csv("unit_history_files/case4.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [15.0, 1.0, 153.0, 23.0, 45.0, 189.0, 191.0],
            'unit_start_date' : pd.to_datetime(['2000-08-14', '2001-07-19', '2007-03-01', '2007-11-08', '2009-08-13', '2011-03-31', '2013-06-23']),
            'unit_end_date' : pd.to_datetime(['2001-07-18', '2007-02-28', '2007-11-07', '2009-08-12', '2011-03-30', '2013-06-22', TODAY]),
            'CODE' : [1, 1, 1, 1, 1, 1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case5():
    ''' tests resolve_units case 5'''
    input_df = pd.read_csv("unit_history_files/case5.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [6.0, 5.0],
            'unit_start_date' : pd.to_datetime(['1986-11-13', '1995-08-17']),
            'unit_end_date' : pd.to_datetime(['1995-08-16', TODAY]),
            'CODE' : [1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case6():
    ''' tests resolve_units case 6'''
    input_df = pd.read_csv("unit_history_files/case6.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [22.0, 6.0, 189.0, 92.0, 7.0, 15.0, 4.0, 15.0, 2.0],
            'unit_start_date' : pd.to_datetime(['1981-01-19', '1982-02-04', '1991-04-25', '1991-05-23', '1992-09-17', '1998-12-10', '1999-06-16', '2000-01-06', '2007-03-29']),
            'unit_end_date' : pd.to_datetime(['1982-02-03', '1991-04-24', '1991-05-22', '1992-09-16', '1998-12-09', '1999-06-15', '2000-01-05', '2007-03-28', TODAY]),
            'CODE' : [1, 1, 1, 1, 1, 1, 1, 1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case7():
    ''' tests resolve_units case 7'''
    input_df = pd.read_csv("unit_history_files/case7.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [44.0, 19.0, 85.0, 189.0, 17.0, 84.0, 543.0, 11.0],
            'unit_start_date' : pd.to_datetime(['1966-08-15', '1966-11-20', '1967-02-02', '1970-07-23', '1971-08-16', '1979-10-16', '1984-05-24', '1985-01-31']),
            'unit_end_date' : pd.to_datetime(['1966-11-19', '1967-02-01', '1970-07-22', '1971-08-15', '1979-10-15', '1984-05-23', '1985-01-30', TODAY]),
            'CODE' : [1, 1, 1, 1, -4, 1, 1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case8():
    ''' tests resolve_units case 8'''
    input_df = pd.read_csv("unit_history_files/case8.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [22.0, 11.0, 15.0, 152.0, 144.0, 50.0, 19.0, 23.0],
            'unit_start_date' : pd.to_datetime(['1963-12-02', '1964-03-01', '1966-10-16', '1970-03-05', '1971-01-01', '1971-04-16', '1974-12-02', '1977-08-23']),
            'unit_end_date' : pd.to_datetime(['1964-02-29', '1966-10-15', '1970-03-04', '1970-12-31', '1971-04-15', '1974-12-01', '1977-08-22', TODAY]),
            'CODE' : [1, 1, 1, 1, 1, 1, 1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case9():
    ''' tests resolve_units case 9'''
    input_df = pd.read_csv("unit_history_files/case9.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [128.0, 23.0, 10.0, 2.0, 2.0, 24.0, 9.0, 4.0, 19.0, 18.0, 12.0],
            'unit_start_date' : pd.to_datetime(['1932-09-01', '1961-01-01', '1961-12-16', '1962-07-01', '1964-01-01', '1965-11-15', '1965-11-19', '1967-12-05', '1970-04-24', '1972-01-01', '1977-12-08']),
            'unit_end_date' : pd.to_datetime(['1960-12-31', '1961-12-15', '1962-06-30', '1963-12-31', '1965-11-14', '1965-11-18', '1967-12-04', '1970-04-23', '1971-12-31', '1977-12-07', TODAY]),
            'CODE' : [1, 1, 1, 1, -4, 0, 1, 1, 1, 1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case10():
    ''' tests resolve_units case 10'''
    input_df = pd.read_csv("unit_history_files/case10.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [620.0],
            'unit_start_date' : pd.to_datetime(['1996-12-15']),
            'unit_end_date' : pd.to_datetime([TODAY]),
            'CODE' : [0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case11():
    ''' tests resolve_units case 11'''
    input_df = pd.read_csv("unit_history_files/case11.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [54.0, 50.0],
            'unit_start_date' : pd.to_datetime(['1954-10-01', '1982-03-04']),
            'unit_end_date' : pd.to_datetime(['1981-08-09', TODAY]),
            'CODE' : [1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)

def test_resolve_units_case12():
    ''' tests resolve_units case 12'''
    input_df = pd.read_csv("unit_history_files/case12.csv.gz")
    output_df = pd.DataFrame({
            'unit' : [44.0, 10.0, 193.0, 10.0],
            'unit_start_date' : pd.to_datetime(['2006-06-26', '2007-12-06', '2011-01-06', '2011-09-15']),
            'unit_end_date' : pd.to_datetime(['2007-12-05', '2011-01-05', '2011-09-14', TODAY]),
            'CODE' : [1, 1, 1, 0]},
        columns = ['unit', 'unit_start_date', 'unit_end_date', 'CODE'])

    results = unit_history_functions.resolve_units(input_df)
    assert results.equals(output_df)
