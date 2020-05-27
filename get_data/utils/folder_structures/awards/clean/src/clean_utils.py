#!usr/bin/env python3
#
# Author(s):   Roman Rivera (Invisible Institute)

'''script containing utility functions used for clean step'''

import re
import yaml
import os
import numpy as np
import pandas as pd


class GeneralCleaners:
    def __init__(self, col, col_type, log=None):
        """Takes in column and column type for standardization/cleaning

        Prepares column for cleaning/standardization based on type.

        Parameters
        ----------
        col : pandas Series
            Column to be cleaned
        col_type : str
            String indicating type of column
        log : logging object
            Used for info/warnings during cleaning process (used if not None)
        """
        self.col = col
        self.col_type = col_type
        self.log = log

    def clean_from_yaml(self, fill=''):
        """Cleans integers based on specifications
        Parameters
        ----------
        col : pandas Series
        col_type : str
            col_type + "_types.yaml" must be file in hand/ directory
        fill : str

        Returns
        -------
        cleaned_col : pandas Series
        """
        working_path = os.getcwd()
        names_path = '/'.join(working_path.split('/')[:-2])
        names_path += f'/share/hand/{self.col_type}_types.yaml'
        with open(names_path, 'r') as file:
            type_dict = yaml.load(file)
        cleaned_col = self.col.str.upper().replace(type_dict)
        fill_locs =  ~cleaned_col.isin(type_dict.values())
        if (cleaned_col[fill_locs].size > 0 and
                self.log):
            self.log.warning("%s values not in %s_types.yaml file."
                             " %d cases replaced with '%s'",
                             cleaned_col[fill_locs].unique().tolist(),
                             self.col_type,
                             cleaned_col[fill_locs].size,
                             fill)
        cleaned_col[fill_locs] = fill
        return cleaned_col

    def clean_int(self, integer,
                  na_value=np.nan):
        """Cleans integers based on specifications
        Parameters
        ----------
        integer : str, int, float, np.nan

        Returns
        -------
        cleaned_int : int, np.nan
        """
        if isinstance(integer, str):
            try:
                integer = integer.replace(',', '')
                if '.' in integer:
                    integer = float(integer)
                cleaned_int = int(integer)
            except ValueError:
                cleaned_int = na_value
        elif np.isfinite(integer):
            cleaned_int = int(float(integer))
        else:
            cleaned_int = na_value
        return cleaned_int

    def clean_age(self, age):
        """Cleans ages
        Parameters
        ----------
        age : str, int, float, np.nan

        Returns
        -------
        cleaned_age : int, np.nan
        """
        cleaned_int = self.clean_int(age)
        cleaned_age = cleaned_int if 1 <= cleaned_int <= 110 else np.nan
        return cleaned_age

    def clean_star(self, star):
        """Cleans stars
        Parameters
        ----------
        star : str, int, float, np.nan

        Returns
        -------
        cleaned_star : int, np.nan
        """
        cleaned_int = self.clean_int(star)
        cleaned_star = cleaned_int if cleaned_int >= 1 else np.nan
        return cleaned_star

    def clean(self):
        """ Picks cleaning function for col based on col_type and cleans col

        Returns
        -------
        cleaned_col : pandas Series
            Column cleaned based on col_type
        """
        if ('hand' in os.listdir() and
            self.col_type + '_types.yaml' in os.listdir('hand/')):
            cleaned_col = self.clean_from_yaml()
        else:
            clean_func = eval("self.clean_%s" % self.col_type)
            cleaned_col = self.col.map(clean_func)
        return cleaned_col


class DateTimeCleaners:
    def __init__(self, dt_col, dt_type, log=None):
        """Takes in date/time/datetime column and col_type for cleaning

        Prepares date/time/datetime column for cleaning based on type.

        Parameters
        ----------
        dt_col : pandas Series
            Date/time/datetime column to be cleaned
        dt_type : str
            String indicating type of column ('date', 'time', 'datetime')
        log : logging object
            Used for info/warnings during cleaning process (used if not None)
        """
        self.dt_col = dt_col
        self.dt_type = dt_type
        self.log = log
        self.col_name = self.dt_col.name

    def clean(self):
        """Determines and appropriately cleans dt_col

        Returns
        -------
        cleaned_df : pandas DataFrame
            Dataframe of cleaned column (datetime split to date and time)
        """
        cleaned_df = pd.DataFrame()
        col_prefix_list = self.col_name.split('_')[:-1]
        date_name = '_'.join(col_prefix_list + ['date'])
        time_name = '_'.join(col_prefix_list + ['time'])

        if self.dt_type == 'time':
            prepped_times = self.prep_numeric_time_col(self.dt_col)
            cleaned_df[time_name] = self.clean_times(prepped_times)
        elif self.dt_type == 'date':
            cleaned_df[date_name] = self.clean_dates(self.dt_col)
        elif self.dt_type == 'datetime':
            date_col = self.dt_col.map(lambda x: x.split(' ')[0])
            time_col = self.dt_col.map(lambda x: x.split(' ')[-1])
            cleaned_df[date_name] = self.clean_dates(date_col)
            cleaned_df[time_name] = self.clean_times(time_col)
        return cleaned_df

    def correct_dates(self, date_col):
        """Corrects dates that are past the current date
        Parameters
        ----------
        date_col : pandas Series

        Returns
        -------
        corrected_col : pandas Series
        """
        today = pd.to_datetime('today').date()
        corrected_col = date_col.map(
            lambda x: x.replace(year=x.year-100)
            if (pd.notnull(x) and x >= today) else x)
        return corrected_col

    def clean_dates(self, date_col):
        """Cleans dates and notifies if errors in series
        Parameters
        ----------
        date_col : pandas Series

        Returns
        -------
        cleaned_dates : pandas Series
        """
        try:
            cleaned_dates = \
                pd.to_datetime(date_col, errors='raise').dt.date
        except:
            cleaned_dates = \
                pd.to_datetime(date_col, errors='coerce').dt.date
            if self.log:
                self.log.warning('Some errors in %s. Returned as NaT:\n %s.',
                                 self.col_name,
                                 self.dt_col[cleaned_dates.isnull() &
                                             self.dt_col.notnull()])
        cleaned_dates = self.correct_dates(cleaned_dates)
        return cleaned_dates

    def clean_times(self, time_col):
        """Cleans times and notifies if errors in series
        Parameters
        ----------
        time_col : pandas Series

        Returns
        -------
        cleaned_times : pandas Series
        """
        try:
            cleaned_times = \
                pd.to_datetime(time_col).dt.time
        except:
            cleaned_times = \
                pd.to_datetime(time_col, errors='coerce').dt.time
            if self.log:
                self.log.warning('Some errors in %s. Returned as NaT:\n %s.',
                                 self.col_name,
                                 self.dt_col[cleaned_times.isnull() &
                                             self.dt_col.notnull()])

        return cleaned_times

    def prep_time(self, time):
        """Converts string of integer to HH:MM time format
        Parameters
        ----------
        time : str

        Returns
        -------
        prepped_time_col : pandas Series
        """
        if pd.isnull(time) or len(time) > 4:
            return np.nan
        else:
            time = "0" * (4 - len(time)) + time
            time = time[:2] + ":" + time[2:]
            return time

    def prep_numeric_time_col(self, time_col):
        """Converts series of numeric times to HH:MM format
        Parameters
        ----------
        time_col : pandas Series

        Returns
        -------
        prepped_time_col : pandas Series
        """
        prepped_time_col = time_col.fillna("").map(
            lambda x: re.sub(r'[^0-9|.]', "", str(x)))
        prepped_time_col = pd.Series(
            GeneralCleaners(prepped_time_col, "int").clean())\
            .map(lambda x: str(int(x)) if x == x else x)\
            .map(self.prep_time)
        return prepped_time_col

#
# end
