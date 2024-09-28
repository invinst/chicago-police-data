from general_utils import reshape_data, list_intersect
import pandas as pd
import logging
from typing import List, Dict, Tuple
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re

class FoiaData:
    def __init__(self, 
                 df: pd.DataFrame, 
                 id: str, 
                 add_cols: List[str] = [], 
                 null_flag_cols: List[str] = ['birth_year',],
                 from_year: int = 2017, 
                 one_to_one: bool = True,
                 log: logging.Logger = None) -> None:
        """FOIA data class, encapsulates data from a foia request and its metadata

        Has methods to prepare data for merging

        Parameters
        ----------
        df : pd.DataFrame
            dataframe of cleaned foia data
        id : str
            id for this foia_data
        add_cols : List[str], optional
            list of columns to add to the data, by default []
        from_year : int, optional
            the year this data began on, by default 2017 based on first roster
            for supplemental foias, this is the year the data was given
        log : logging.Logger, optional
            logging object, by default None
        """        
        self.id = id
        self.add_cols = add_cols
        self.null_flag_cols = null_flag_cols
        self.from_year = from_year
        self.one_to_one = one_to_one
        self.log = log or logging.getLogger(__name__)

        self.df = df.pipe(self.drop_unmergeable_rows) \
            .pipe(self.reshape_star_cols_to_rows, id=id) \
            .pipe(self.reshape_cr_id_cols_to_rows, id=id) \
            .pipe(self.add_columns, add_cols=add_cols, from_year=from_year) \
            .pipe(self.calculate_null_flags, cols=self.null_flag_cols)

        self.unmerged = self.df.copy()

    def prepare_data(self, df: pd.DataFrame, id: str, add_cols: List[str] = [], from_year: int = 2017) -> pd.DataFrame:
        """Prepare data for merging

        Parameters
        ----------
        df : pd.DataFrame
            dataframe of cleaned foia data
        id : str
            id for this foia_data
        add_cols : List[str], optional
            list of columns to add to the data, by default []
        from_year : int, optional
            the year this data began on, by default 2017 based on first roster
            for supplemental foias, this is the year the data was given

        Returns
        -------
        pd.DataFrame
            dataframe of cleaned foia data
        """        
        return df.pipe(self.drop_unmergeable_rows) \
            .pipe(self.reshape_star_cols_to_rows, id=id) \
            .pipe(self.reshape_cr_id_cols_to_rows, id=id) \
            .pipe(self.add_columns, add_cols=add_cols, from_year=from_year) \
            .pipe(self.calculate_null_flags, cols=self.null_flag_cols)

    def drop_unmergeable_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        row_count = df.shape[0]
        if 'merge' in df.columns:
            drop_row_count = df[df['merge'] != 1].shape[0]
            df = df[df['merge'] == 1].drop('merge', axis=1)
            self.log.info(f'{drop_row_count} rows dropped due to merge!=1. {df.shape[0]} rows remaining')
        return df
    
    def reshape_star_cols_to_rows(self, df: pd.DataFrame, id: str) -> pd.DataFrame:
        row_count = df.shape[0]
        if 'star1' in df.columns:
            df = reshape_data(df, 'star', id)
            self.log.info(f'Data reshaped wide ({row_count} rows) '
                          f'to long ({df.shape[0]} rows) by star columns')
        if 'current_star' in df.columns and 'star' not in df.columns:
            df['star'] = df['current_star']
        return df
    
    def reshape_cr_id_cols_to_rows(self, df: pd.DataFrame, id: str) -> pd.DataFrame:
        row_count = df.shape[0]
        if 'cr_id1' in df.columns:
            df = df.reset_index()
            df = reshape_data(df, 'cr_id', 'index')
            df.drop('index', axis=1, inplace=True)
            self.log.info(f"Data reshaped wide ({row_count} rows) "
                          f'to long ({df.shape[0]} rows) by cr_id columns')
        return df

    
    def add_columns(self, df, add_cols: List[str]=[], from_year:int = 2017) -> pd.DataFrame:
        # TODO: refactor this, this is just copied from the old code
        for add_col in add_cols:
            assert isinstance(add_col, str) or isinstance(add_col, dict)

            if isinstance(add_col, dict) and (add_col['id'] in [self.id, '']):
                # TODO: switch to using df.assign instead of python exec
                assert not any(cond in add_col['exec']
                               for cond in ['import', 'exec', 'eval'])
                self.log.info('Adding column by executing \n%s\n'
                              'to data with ID = %s', add_col['exec'], self.id)

                exec(add_col['exec'])

            if isinstance(add_col, str):
                if re.search("[F|L][0-9][F|L]N", add_col):
                    use_col = 'first_name_NS' if add_col[2] == 'F'\
                               else 'last_name_NS'
                    start, end = (0, int(add_col[1])) if add_col[0] == 'F'\
                                 else (-int(add_col[1]), None)
                    df[add_col] = df[use_col].map(lambda x: x[start:end])
                if add_col == 'current_age' and "current_age" in df.columns:
                    df['current_age_p1'] = df['current_age'] + (2017-from_year)
                    df['current_age_m1'] = df['current_age'] + (2017-from_year)
                    df.drop('current_age', axis=1, inplace=True)
                if add_col == 'BY_to_CA' and 'birth_year' in df.columns:
                    df['current_age_p1'] = 2017 - df['birth_year']
                    df['current_age_m1'] = 2016 - df['birth_year']
                self.log.info('Adding column [%s] to data with ID = %s',
                              add_col, self.id)

        return df
    
    def add_current_age_from_birth_year(self, df: pd.DataFrame, from_year:int = 2017) -> pd.DataFrame:
        if 'birth_year' in df.columns:
            df['current_age_p1'] = 2017 - df['birth_year']
            df['current_age_m1'] = 2016 - df['birth_year']
        return df
    
    def copy_current_age_to_plus_minus_1(self, df: pd.DataFrame, from_year:int=2017) -> pd.DataFrame:
        if 'current_age' in df.columns:
            df['current_age_p1'] = df['current_age'] + (2017-from_year)
            df['current_age_m1'] = df['current_age'] - (2017-from_year)
        return df

    def calculate_null_flags(self, df: pd.DataFrame, cols: List[str]):
        """Adds a flag that suggests if a column is null for that every row for a uid
        Transformed so it is associated with every row

        Useful for custom merge queries, where merges should be done only when there is no data for that column

        Parameters
        ----------
        df : DataFrame
            dataframe to add null columns
        uid : str
            id to group by
        cols : list[str]
            columns to add null flags for

        Returns
        -------
        DataFrame
            df with new columns (suffixed with _null), true if col is always null for this uid group, false otherwise
        """  
        # drop existing columns      
        df = df.drop(df.columns[df.columns.str.endswith("_null")], axis=1)
        null_flags = df[cols].isnull().groupby(df[self.id]).transform('all').add_suffix("_null")

        return df.join(null_flags)
    
    def filter_merges(self, merged_df, unmerged):
        return unmerged[~unmerged[self.id].isin(merged_df[self.id])]

    def merged_percent(self, merged_count):
        return round(100 * (merged_count / self.df[self.id].nunique()), 2)
    
    def unmerged_percent(self):
        return round(100 * (self.unmerged[self.id].nunique() / self.df[self.id].nunique()), 2)

    def get_column_changes(self, col: str, keep_cols: List[str] = None) -> pd.DataFrame:
        """Get changes in a column for the same id

        Parameters
        ----------
        col : str
            column to get changes for
        keep_cols : List[str]
            columns to keep in the output, by default None, which keeps just the change col

        Returns
        -------
        pd.DataFrame
            dataframe showing unique values change column grouped by id
        """        
        change_ids = self.df.groupby(self.id, as_index=False)[col] \
            .nunique().query(f"{col} > 1").UID.values

        if not keep_cols:
            keep_cols = [col]

        return self.df[self.df[self.id].isin(change_ids)].groupby(self.id, as_index=False)[keep_cols] \
                .agg(lambda x: set(x) if x.nunique() > 1 else x.iloc[0]) 
    
    def get_matches_with_cols(self, cols: List[str], data_id: str=None, keep_cols: List[str]=None):
        keep_cols = self.get_keep_cols(keep_cols)

        # can't just use matched_on key since this may not be the full key, or be in a different order
        # if data_id is not None, only look at UIDs with at least one data_id
        if data_id:
            mask = self.df[data_id].notnull().groupby(self.df[self.id]).transform('any')
        else:
            mask = pd.Series([True] * self.df.shape[0], index=self.df.index)

        for col in cols:
            mask &= self.df.matched_on.str.contains(col)

        match_ids = self.df[mask][self.id].unique()
        return self.df[self.df[self.id].isin(match_ids)] \
            .sort_values(self.id)[keep_cols]
    
    def get_matches_for_merge(self, merged_df: pd.DataFrame, data_id: str=None, keep_cols: List[str] = None):
        keep_cols = self.get_keep_cols(keep_cols)

        if data_id:
            keep_cols.append(data_id)
            
        return self.df.merge(merged_df[self.id]) \
                        .sort_values(self.id)[keep_cols]

    def get_keep_cols(self, keep_cols: List[str] = None):
        if not keep_cols:
            keep_cols = ['UID', 'star', 'first_name_NS', 'last_name_NS', 'appointed_date', 
                         'birth_year', 'current_age', 'gender', 'race', 
                         'current_unit', 'current_rank', 'matched_on']  

        return list_intersect(keep_cols, self.df.columns)      

    def get_fig_ax(self, 
                   title: str, 
                   xlabel: str, 
                   ylabel: str, 
                   plot_size: Tuple[int, int] = (20, 10)) -> Tuple[plt.Figure, plt.Axes]:
        fig, ax = plt.subplots()
        fig.set_size_inches(plot_size) 

        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel) 

        return fig, ax

    def convert_to_datetime(self, date_col):
        if not np.issubdtype(self.df[date_col].dtype, np.datetime64):
            self.df[date_col] = pd.to_datetime(self.df[date_col])

        return self

    def plot_date(self, 
                  date_col : str='appointed_date', 
                  title: str = "Officer Profiles - Appointed Dates", 
                  xlabel: str = "Appointed Date",
                  ylabel: str = "Count",
                  period: str = "Y",
                  plot_size = (20, 10)) -> None:
        fig, ax = self.get_fig_ax(title, xlabel, ylabel, plot_size)

        self.convert_to_datetime(date_col)
        self.df \
            .groupby(self.id)[date_col].min().dt.to_period(period) \
            .value_counts().sort_index().plot(kind="bar", ax=ax)
        
        plt.show()

    def plot_date_by_merge(self, 
                        date_col: str = 'appointed_date',
                        title: str = "Officer Profiles - Appointed Dates Stacked by Merge",
                        xlabel: str = "Appointed Date", 
                        ylabel: str ="Officer Count", 
                        plot_size: Tuple[int, int] = (20,10),
                        period: str = "Y") -> None:
        fig, ax = self.get_fig_ax(title, xlabel, ylabel, plot_size)

        self.convert_to_datetime(date_col)

        merged_uids = self.df[self.df.matched_on.notnull()]['UID'].unique()
        self.df.assign(merged=self.df['UID'].isin(merged_uids)) \
            .groupby([self.df[date_col].dt.to_period(period), 'merged']) \
            .UID \
            .nunique() \
            .unstack() \
            .plot(kind='bar', stacked=True, ax=ax)

        plt.show()


