#! usr/bin/env python3
#
# Author:   Roman Rivera

'''pytest functions in clean_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import clean_functions


def test_clean_date_df_datetimes():
    '''test clean_date_df for datetimes'''
    input_df = pd.DataFrame({
        'test_datetime': ['2000-01-01 00:00:00',
                          '1992-04-07 44:44:12',
                          '1854-09-21 01:01:01',
                          '2045-01-01 03:00:00',
                          '07/21/54 00:00:00']})
    output_df = pd.DataFrame(
        {
            'test_date': pd.to_datetime(pd.Series(
                ['2000-01-01',
                 '1992-04-07',
                 '1854-09-21',
                 '1945-01-01',
                 '1954-07-21'])).dt.date,
            'test_time': pd.to_datetime(pd.Series(
                ['00:00:00',
                 np.nan,
                 '01:01:01',
                 '03:00:00',
                 '00:00:00'])).dt.time
        }
    )
    results = clean_functions.clean_date_df(input_df)
    assert results.equals(output_df)


def test_clean_date_df_dates():
    '''test clean_date_df for dates'''
    input_df = pd.DataFrame({
        'test_date': ['2000-01-01',
                      '1992-04-07',
                      '1854-09-21',
                      '2045-01-01',
                      '07/21/54']})
    output_df = pd.DataFrame(
        {
            'test_date': pd.to_datetime(pd.Series(
                ['2000-01-01',
                 '1992-04-07',
                 '1854-09-21',
                 '1945-01-01',
                 '1954-07-21'])).dt.date
        }
    )
    results = clean_functions.clean_date_df(input_df)
    assert results.equals(output_df)


def test_split_full_names():
    '''test splite_full_names'''
    input_series = pd.Series([np.nan,
                              '----',
                              'Jones, JR, Bob',
                              'Smith Jr, Susan J'])
    output_df = pd.DataFrame({
        'last_name': ['', '', 'Jones, JR', 'Smith Jr'],
        'first_name': ['', '', ' Bob', ' Susan J']
        }, columns=['last_name', 'first_name'])
    results = clean_functions.split_full_names(input_series)
    assert results.equals(output_df)


def test_clean_name_col_first_name():
    '''test clean_name_col on for first_name series'''
    input_series = pd.Series(['DE ANDRE',
                              'BOB D',
                              'A RICHARD  A JR',
                              'KIM-TOY',
                              'JO ANN',
                              'J T',
                              'ABDUL-AZIZ',
                              'MARCELLUS. H',
                              'ERIN  E M',
                              'J EDGAR',
                              'GEORGE H W'],
                             name='first_name')

    output_df = pd.DataFrame(
        {'first_name_NS': ['DEANDRE', 'BOB', 'ARICHARD', 'KIMTOY',
                           'JOANN', 'JT', 'ABDULAZIZ', 'MARCELLUS',
                           'ERIN', 'JEDGAR', 'GEORGE'],
         'f_MI': ['', 'D', 'A', '', '', '', '', 'H', 'E', '', 'H'],
         'f_MI2': ['', '', '', '', '', '', '', '', 'M', '', 'W'],
         'f_SN': ['', '', 'JR', '', '', '', '', '', '', '', ''],
         'first_name': ['DE ANDRE', 'BOB', 'A RICHARD', 'KIM-TOY',
                        'JO ANN', 'J T', 'ABDUL-AZIZ', 'MARCELLUS',
                        'ERIN', 'J EDGAR', 'GEORGE']},
        columns=['first_name_NS', 'f_MI', 'f_MI2', 'f_SN', 'first_name'])
    results = clean_functions.clean_name_col(input_series)
    assert results.equals(output_df)


def test_clean_name_col_last_name():
    '''test clean_name_col on for last_name series'''
    input_series = pd.Series(['DE LA O',
                              'JONES V',
                              'LUQUE-ROSALES',
                              'MC CARTHY',
                              'O BRIEN',
                              'SADOWSKY, JR',
                              'NORWOOD II',
                              'BURKE JR J',
                              'VON KONDRAT',
                              'HOOVER',
                              'BUSH'],
                             name='last_name')
    output_df = pd.DataFrame(
        {'last_name_NS': ['DELAO', 'JONES', 'LUQUEROSALES', 'MCCARTHY',
                          'OBRIEN', 'SADOWSKY', 'NORWOOD', 'BURKE',
                          'VONKONDRAT', 'HOOVER', 'BUSH'],
         'l_MI': ['', '', '', '', '', '', '', 'J', '', '', ''],
         'l_MI2': ['', '', '', '', '', '', '', '', '', '', ''],
         'l_SN': ['', 'V', '', '', '', 'JR', 'II', 'JR', '', '', ''],
         'last_name': ['DE LA O', 'JONES', 'LUQUE-ROSALES', 'MC CARTHY',
                       'O BRIEN', 'SADOWSKY', 'NORWOOD', 'BURKE',
                       'VON KONDRAT', 'HOOVER', 'BUSH']},
        columns=['last_name_NS', 'l_MI', 'l_MI2', 'l_SN', 'last_name'])

    results = clean_functions.clean_name_col(input_series)
    assert results.equals(output_df)


def test_compare_columns_middle_initials():
    '''test compare_columns for middle initial dataframes'''
    input_df = pd.DataFrame(
        {'f_MI': ['J', 'D', '', '', '', ''],
         'f_MI2': ['', '', 'H', '', '', ''],
         'l_MI': ['T', '', 'W', 'A', '', ''],
         'l_MI2': ['', 'V', '', 'L', '', 'G']},
        columns=['f_MI', 'f_MI2', 'l_MI', 'l_MI2'])
    input_colnames = ['middle_initial', 'middle_initial2']

    output_df = pd.DataFrame(
        {'middle_initial': ['J', 'D', 'H', 'A', np.nan, 'G'],
         'middle_initial2': ['T', 'V', 'W', 'L', np.nan, None]})

    results = clean_functions.compare_columns(input_df, input_colnames)
    assert results.equals(output_df)

def test_compare_columns_suffix_name():
    '''test compare_columns for suffix name dataframes'''
    input_df = pd.DataFrame(
        {'f_SN': ['JR', 'V', '', '', '', ''],
         'l_SN': ['', '', 'I', 'SR', 'IV', '']},
        columns=['f_SN', 'l_SN'])
    input_colnames = ['suffix_name']

    output_df = pd.DataFrame(
        {'suffix_name': ['JR', 'V', 'I', 'SR', 'IV', np.nan]})

    results = clean_functions.compare_columns(input_df, input_colnames)
    assert results.equals(output_df)


def  test_clean_names():
    '''test clean_names'''
    input_df = pd.DataFrame(
        {'first_name': ['DE ANDRE', 'BOB D', 'A RICHARD  A', 'KIM-TOY',
                        'JO ANN', 'J T', 'ABDUL-AZIZ', 'MARCELLUS. H',
                        'ERIN  E M', 'J EDGAR', 'GEORGE H W'],
         'last_name': ['DE LA O', 'JONES V', 'LUQUE-ROSALES', 'MC CARTHY',
                       'O BRIEN', 'SADOWSKY, JR', 'NORWOOD II', 'BURKE JR J',
                       'VON KONDRAT', 'HOOVER', 'BUSH']})

    output_df = pd.DataFrame(
        {'first_name': ['DE ANDRE', 'BOB', 'A RICHARD', 'KIM-TOY',
                        'JO ANN', 'J T', 'ABDUL-AZIZ', 'MARCELLUS',
                        'ERIN', 'J EDGAR', 'GEORGE'],
         'middle_initial': ['', 'D', 'A', '', '', '', '', 'H', 'E', '', 'H'],
         'middle_initial2': ['', '', '', '', '', '', '', 'J', 'M', '', 'W'],
         'suffix_name': ['', 'V', '', '', '', 'JR', 'II', 'JR', '', '', ''],
         'last_name': ['DE LA O', 'JONES', 'LUQUE-ROSALES', 'MC CARTHY',
                       'O BRIEN', 'SADOWSKY', 'NORWOOD', 'BURKE',
                       'VON KONDRAT', 'HOOVER', 'BUSH'],
         'first_name_NS': ['DEANDRE', 'BOB', 'ARICHARD', 'KIMTOY',
                           'JOANN', 'JT', 'ABDULAZIZ', 'MARCELLUS',
                           'ERIN', 'JEDGAR', 'GEORGE'],
         'last_name_NS': ['DELAO', 'JONES', 'LUQUEROSALES', 'MCCARTHY',
                          'OBRIEN', 'SADOWSKY', 'NORWOOD', 'BURKE',
                          'VONKONDRAT', 'HOOVER', 'BUSH']},
        columns=['last_name', 'last_name_NS',
                 'first_name', 'first_name_NS',
                 'middle_initial', 'middle_initial2', 'suffix_name'])

    results = clean_functions.clean_names(input_df)

    assert sorted(results.columns) == sorted(output_df.columns)
    for col in results.columns:
        assert results[col].equals(output_df[col])
