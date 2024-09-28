import pandas as pd
from general_utils import reshape_data, list_diff, list_intersect, keep_duplicates
import logging
from typing import List, Mapping, Any, Callable, Optional
from typing_extensions import Self
import re
import numpy as np
from fuzzywuzzy import fuzz
from foia_data import FoiaData
import itertools

# postprocess filter: takes in ref_unmerged, sup_unmerged, ref_id, sup_id, merged_df returns merged_df
MergePostProcessor = Callable[
    [pd.DataFrame, # ref_unmerged 
     pd.DataFrame, #sup_unmerged,
     int, #ref_id
     int,  #sup_id
     pd.DataFrame], pd.DataFrame] # merged_df and filtered_merged_df

DataFrameFilter = Callable[[pd.DataFrame], pd.DataFrame]

class Merge: 
    def __init__(self, 
                 name: str = "",
                 merge_dict: Mapping[str, List[str]] = None, 
                 custom_merges: List[List[str]] = None,
                 query: Optional[str] = None,
                 reference_preprocess: Optional[DataFrameFilter | str] = None, 
                 supplemental_preprocess: Optional[DataFrameFilter | str] = None, 
                 merge_postprocess: MergePostProcessor = None,
                 filter_reference_merges_flag: bool = True,
                 filter_supplemental_merges_flag: bool = True,
                 check_duplicates: bool = True) -> None:
        """Encapsulates a valid merge criteria, including a list of columns to merge on, 
        pre-processing transformation, and any post-merge transformations

        Has methods for applying this merge to reference and supplemental data objects

        NOTE: this includes multiple lists of columns to merge on, not just a single list of columns
        It's a criteria, not a single pandas merge iteration
        
        Parameters
        ----------
        merge_dict : Mapping[str, List[str]]
            dictionary of columns representing list of valid merges, see generate_on_lists for more details
        query: Optional[str], optinoal
            query to filter data by before merging, by default None
            mostly legacy, use reference_pre_process and supplemental_pre_process instead
        reference_pre_process : Optional[str], optional
            query , by default None
        supplemental_preprocess : Optional[str], optional
            _description_, by default None
        merge_postprocess : Optional[Callable[[pd.DataFrame], pd.DataFrame]], optional
            _description_, by default None
        common_columns : List[str], optional
        filter_reference_merge_flag: bool, optional
            whether to filter out already merged rows from the reference unmerged data, by default True
            set to false if supplemental data is not deduplicated, i.e., a supplemental 
        filter_supplemental_merge_flag: bool, optional
            whether to filter out already merged rows from the supplemental unmerged data, by default True
            set to false usually when merge_postprocess looks for multiple types of merges and consolidates them
        """        
        self.name = name
        self.merge_dict = merge_dict or {}
        self.custom_merges = custom_merges or [[]]

        if query:
            self.reference_preprocess: DataFrameFilter = self.get_dataframe_filter(query)
            self.supplemental_preprocess: DataFrameFilter = self.get_dataframe_filter(query) 
        else:
            self.reference_preprocess: DataFrameFilter = self.get_dataframe_filter(reference_preprocess) 
            self.supplemental_preprocess: DataFrameFilter = self.get_dataframe_filter(supplemental_preprocess)

        self.post_process: MergePostProcessor = merge_postprocess or (lambda ref_unmerged, sup_unmerged, ref_id, sup_id, merged_df: merged_df)
        self.filter_reference_merges_flag = filter_reference_merges_flag
        self.filter_supplemental_merges_flag = filter_supplemental_merges_flag
        self.check_duplicates = check_duplicates

        self.merged_df = pd.DataFrame()


    def apply_merge(self, reference: FoiaData, supplemental: FoiaData, common_columns: List[str] = None) -> Self:
        """
        Applies a merge operation to the unmerged dataframes of the reference and supplemental FoiaData objects, using the merge rules and custom merges specified in the object's attributes. 

        Parameters
        ----------
        reference: FoiaData
            The reference FoiaData object containing the merged and unmerged dataframes to be used as the base for the merge operation.
        supplemental: FoiaData
            The supplemental FoiaData object containing the unmerged dataframe to be merged with the reference dataframe.
        common_columns: List[str], optional
            A list of column names to be used as the join keys for the merge operation. 
            If not given will calculate the intersection of the columns in the reference and supplemental dataframes, 
            excluding the unique identifier columns.

        Returns
        -------
        merged_df: pd.DataFrame
            The merged dataframe resulting from the merge operation, 
            after applying the post-processing filters specified in the object's attributes.

        Raises
        ------
        AssertionError:
            If the merged dataframe contains duplicate unique identifier values after the merge operation.
        """
        ref_unmerged = self.reference_preprocess(reference.unmerged)
        sup_unmerged = self.supplemental_preprocess(supplemental.unmerged)

        if not common_columns:
            common_columns = list_diff(list_intersect(reference.df.columns, supplemental.df.columns), 
                                       [reference.id, supplemental.id])

        cols_list = self.generate_on_lists(self.merge_dict, common_columns, self.custom_merges)
        self.cols_list = cols_list

        all_merges = self.get_all_merges(ref_unmerged, sup_unmerged, reference.id, supplemental.id, cols_list)

        self.merged_df =  self.post_process(ref_unmerged, sup_unmerged, reference.id, supplemental.id, pd.concat(all_merges))
        self.merged_df['merge_name'] = self.name
        
        if self.check_duplicates:
            duplicated = keep_duplicates(self.merged_df, [reference.id])
            assert duplicated.empty != 0, f'Same UID matched to multiple sup_ids {duplicated}'

        return self.merged_df
    
    def get_all_merges(self, 
                       ref_unmerged: pd.DataFrame, 
                       sup_unmerged: pd.DataFrame, 
                       ref_id: str,
                       sup_id: str,
                       cols_list: List[List[str]]) -> pd.DataFrame:
        """Returns a list of dataframes resulting from merging the reference and supplemental unmerged dataframes on all possible combinations of columns specified in the cols_list parameter.

        Parameters
        ----------
        ref_unmerged: pd.DataFrame
            The unmerged dataframe of the reference FoiaData object.
        sup_unmerged: pd.DataFrame
            The unmerged dataframe of the supplemental FoiaData object.
        ref_id: str
            The name of the unique identifier column in the reference dataframe.
        sup_id: str
            The name of the unique identifier column in the supplemental dataframe.
        cols_list: List[List[str]]
            A list of lists of column names to be used as the join keys for the merge operation. Each inner list represents a set of columns that can be used to merge the dataframes.

        Returns
        -------
        all_merges: List[pd.DataFrame]
            A list of dataframes resulting from merging the reference and supplemental unmerged dataframes on all possible combinations of columns specified in the cols_list parameter.
        """
        all_merges = []
        for on_cols in cols_list:
            merged_df = self.merge_on_cols(ref_unmerged, sup_unmerged, ref_id, sup_id, on_cols)
            all_merges.append(merged_df)

            if self.filter_supplemental_merges_flag:
                sup_unmerged = self.filter_merges(merged_df, sup_unmerged, sup_id)
            if self.filter_reference_merges_flag:
                ref_unmerged = self.filter_merges(merged_df, ref_unmerged, ref_id)

        return all_merges

        
    def merge_on_cols(self, 
                      ref_unmerged: pd.DataFrame, 
                      sup_unmerged: pd.DataFrame, 
                      ref_id: int, 
                      sup_id: int, 
                      on_cols: List[str]) -> pd.DataFrame:
        
        """Performs a single merge operation on the reference and supplemental unmerged dataframes, using the specified columns as join keys.

        Parameters
        ----------
        ref_unmerged: pd.DataFrame
            The unmerged dataframe of the reference FoiaData object.
        sup_unmerged: pd.DataFrame
            The unmerged dataframe of the supplemental FoiaData object.
        ref_id: str
            The name of the unique identifier column in the reference dataframe.
        sup_id: str
            The name of the unique identifier column in the supplemental dataframe.
        on_cols: List[str]
            A list of column names to be used as the join keys for the merge operation.

        Returns
        -------
        merged_df: pd.DataFrame
            The merged dataframe resulting from the merge operation, containing only the unique identifier columns and the specified join key columns.
        """
        id_cols = [ref_id, sup_id]

        # drop nulls from each side for relevant columns
        merged_df = ref_unmerged[[ref_id] + on_cols] \
            .dropna(how='any') \
            .merge(sup_unmerged[[sup_id] + on_cols].dropna(how='any'), 
                    on=on_cols) \
            [id_cols].drop_duplicates()
        
        merged_df['matched_on'] = "-".join(on_cols)
        merged_df['matched_to'] = sup_id

        if merged_df.shape[0] > 0:
            print('%d Matches on \n %s columns'
                    %(merged_df.shape[0], on_cols))

        return merged_df

    def generate_on_lists(self, 
                          merge_dict: Mapping[str, Any], 
                          common_columns: List[str],
                          custom_merges: List[List[str]]) -> List[List[str]]:
        """Generates a list of lists of column names to be used as join keys for the merge operation.
        
        Takes merge_dict, a dictionary mapping each class of column to a list of column names that are considered a valid merge,  
        and creates all possible combinations of columns based on the merge_dict. Does this as a cartesian product. 

        NOTE: if a list of column names contains an empty string, it is considered optional for a merge. 

        Example: 
        merge_dict = {'star': ['star', '']
                      'first_name': ['first_name_NS', 'F4FN'],
                        'last_name': ['last_name_NS', 'F4LN']
                    }

        Would expand out to the following lists of columns to merge on:
        [['star', 'first_name_NS', 'last_name_NS'],
        ['star', 'first_name_NS', 'F4LN'],
        ['star', 'F4FN', 'last_name_NS'],
        ['star', 'F4FN', 'F4LN'],
        ['', 'first_name_NS', 'last_name_NS'],
        ['', 'first_name_NS', 'F4LN'],
        ['', 'F4FN', 'last_name_NS'],
        ['', 'F4FN', 'F4LN']]

        Those empty strings would be removed, so that those last 4 lists would have just the 2 name columns to merge on, no star

        Also adds custom columns at the end of the list of lists of columns to merge on.

        Filters on common columns to ensure that only columns that exist in both the reference and supplemental dataframes are used as join keys.

        Parameters
        ----------
        merge_dict: Mapping[str, Any]
            A dictionary mapping a type of column to a list of column names that can be considered an alternative merge for that column
            NOTE: if a column name is an empty string, it is considered optional for a valid merge
        common_columns: List[str]
            A list of column names that are common to both the reference and supplemental dataframes.
        custom_merges: List[List[str]]
            A explicit list of custom merge keys to be appended to the generated list
        Returns
        -------
        on_lists: List[List[str]]
            A list of lists of column names to be used as join keys for the merge operation, based on the merge rules and custom merges specified in the `merge_dict` and `custom_merges` parameters.
        """
        common_columns.append('')

        # filter out columns from merge dict not found in common columns
        base_lists = [list_intersect(col_list, common_columns)
                      for col_list in merge_dict.values()]
        # remove any empty lists
        base_lists = [fbl for fbl in base_lists if fbl]

        on_lists = list(itertools.product(*base_lists)) + custom_merges

        # filter out the empty strings and empty lists
        return [[column for column in on_list if column] for on_list in on_lists if on_list]

    def get_dataframe_filter(self, filter: Optional[DataFrameFilter | str] = None) -> DataFrameFilter:
        """Get a dataframe filter function from a string or function
        
        If none, return a function that returns the dataframe unchanged

        Parameters
        ----------
        filter : Optional[DataFrameFilter  |  str], optional
            filter that is either already a dataframe filter, or a string, or none, by default None

        Returns
        -------
        DataFrameFilter
            dataframe filter function
        """        
        if not filter:
            return lambda df: df
        elif isinstance(filter, str):
            return lambda df: df.query(filter)
        else:
            return filter

    def filter_merges(self, merged_df: pd.DataFrame, unmerged: pd.DataFrame, id: str) -> pd.DataFrame:
        """Filter merged ids out of unmerged dataframe 

        Parameters
        -----------
        merged_df : pd.DataFrame
            dataframe with merges
        unmerged : pd.DataFrame
            unmerged dataframe to filter from
        id : str
            id column name

        Returns:
        --------
        DataFrame
            unmerged dataframe with ids that were merged removed
        """
        return unmerged[~unmerged[id].isin(merged_df[id])]
    
    def get_merge_criteria_report(self, idx: int, total: int) -> str:
        def add_to_report(condition, message):
            return message if condition else ""

        # filter out empty strings
        alternate_columns = [f"{col_type}: {', '.join(col_list)}" for col_type, col_list in self.merge_dict.items() 
                            if len([col for col in col_list if col]) > 1]

        report = f"Running merge ({idx+1} of {total}): {self.name}\n"
        report += add_to_report(self.required_columns, f"Required column types: {', '.join(self.required_columns)}\n")
        report += add_to_report(self.optional_columns, f"Optional column types: {', '.join(self.optional_columns)}\n")
        report += add_to_report(alternate_columns, f"Alternate columns for types: {'; '.join(alternate_columns)}\n")

        return report
    
    
    @property
    def required_columns(self):
        return [col for col, col_list in self.merge_dict.items() 
                if '' not in col_list]
    
    @property
    def optional_columns(self):
        return [col for col, col_list in self.merge_dict.items() 
                if '' in col_list]        

    def __repr__(self):
        return f"Merge ({self.name})"