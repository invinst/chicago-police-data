#! usr/bin/env python3
#
# Author:   Roman Rivera (Invisible Institute)

'''pytest functions in clean_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
from clean_utils import DateTimeCleaners
from clean_utils import GeneralCleaners


def test_DateTimeCleaners_datetimes():
    '''test datetimes'''
    input_series = pd.Series(
                    ['2000-01-01 00:00:00',
                     '1992-04-07 44:44:12',
                     '2045-01-01 03:00:00',
                     '1854-09-21 01:01:01',
                     '2045-01-01 03:00:00',
                     '07/21/54 00:00:00'],
                     name='test_datetime')

    output_df = pd.DataFrame(
        {
            'test_date': pd.to_datetime(pd.Series(
                ['2000-01-01',
                 '1992-04-07',
                 '1945-01-01',
                 '1854-09-21',
                 '1945-01-01',
                 '1954-07-21'])).dt.date,
            'test_time': pd.to_datetime(pd.Series(
                ['00:00:00',
                 np.nan,
                 '03:00:00',
                 '01:01:01',
                 '03:00:00',
                 '00:00:00'])).dt.time
        }
    )
    results = DateTimeCleaners(input_series, 'datetime').clean()
    assert results.equals(output_df)


def test_DateTimeCleaners_dates():
    '''test dates'''
    input_series = pd.Series(
                     ['2000-01-01',
                      '1992-04-07',
                      '1854-09-21',
                      '2045-01-01',
                      '07/21/54'],
                      name='test_date')
    output_df = pd.DataFrame(pd.to_datetime(pd.Series(
                ['2000-01-01',
                 '1992-04-07',
                 '1854-09-21',
                 '1945-01-01',
                 '1954-07-21'],
                 name='test_date')).dt.date)
    results = DateTimeCleaners(input_series, 'date').clean()
    assert results.equals(output_df)


def test_DateTimeCleaners_times():
    '''test for times'''
    input_series = pd.Series(
                    ['0990',
                     1212,
                     '-021',
                     '201.0',
                     '324'],
                     name='test_time')
    output_df = pd.DataFrame(
        {
            'test_time': pd.to_datetime(pd.Series(
                [np.nan,
                 '12:12',
                 '00:21',
                 '02:01',
                 '03:24'])).dt.time
        }
    )
    results = DateTimeCleaners(input_series, 'time').clean()
    assert results.equals(output_df)


def test_GeneralCleaners_int():
    '''test for ints'''
    input_series = pd.Series(
                    ["12.09", "ab21", 20.0, "-267.125", "-1246", "12,311"],
                     name='test_ints')
    output_series = pd.Series(
                    [12, np.nan, 20, -267, -1246, 12311],
                    name='test_ints')
    results = GeneralCleaners(input_series, 'int').clean()
    assert results.equals(output_series)


def test_GeneralCleaners_age():
    '''test for ages'''
    input_series = pd.Series(
                    [-9126, "26.06", 111, 55, "2016-01-01"],
                     name='test_ages')
    output_series = pd.Series(
                    [np.nan, 26, np.nan, 55, np.nan],
                    name='test_ages')
    results = GeneralCleaners(input_series, 'age').clean()

    assert results.equals(output_series)

def test_GeneralCleaners_star():
    '''test for star numbers'''
    input_series = pd.Series(
        [-9126, "26.06", 0, 111, 10000000, "2016-01-01"],
        name='test_stars')
    output_series = pd.Series(
        [np.nan, 26, np.nan, 111, 10000000, np.nan],
        name='test_stars')
    results = GeneralCleaners(input_series, 'star').clean()
    print(results)

    assert results.equals(output_series)
