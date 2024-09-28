import pandas as pd
from general_utils import reshape_data, list_diff, list_intersect, keep_duplicates
import logging
from typing import List, Mapping, Any, TypedDict, Callable, Optional
from typing_extensions import Self
import re
from foia_data import FoiaData
from merge_data import Merge

class ReferenceData:
    def __init__(self, 
                 df: pd.DataFrame, 
                 id: str, 
                 add_cols: List[str] = [], 
                 null_flag_cols: List[str] = ['birth_year', 'appointed_date'],
                 log: logging.Logger = None) -> None:
        self.reference = FoiaData(df, id=id, add_cols=add_cols, null_flag_cols=null_flag_cols, log=log)
        self.merged_df = pd.DataFrame()
        self.merges = []
        self.log = log or logging.getLogger(__name__)

    @classmethod
    def from_first_file(cls,
                        df: pd.DataFrame,
                        id: str="UID",
                        data_id: str="ID",
                        starting_id: int=100001,
                        add_cols: List[str]=[],
                        null_flag_cols: List[str]=[],
                        log: logging.Logger=None) -> Self:
        """Initializes ReferenceData object from the first file in a series of files, root of merges

        Assigns initial unique ids to each row in the first file based on the data_id column
        """
        # remove non merges from first file
        if 'merge' in df.columns:
            df = df[df['merge'] == 1].drop('merge', axis=1)

        log.info('Creating reference data from file'
                        'with intrafile ID: %s', data_id)

        uid_df = df[[data_id]].drop_duplicates().sort_values(data_id)\
            .reset_index(drop=True)
        uid_df[id] = uid_df.index + starting_id

        df = df.merge(uid_df, on=data_id, how='left')\
            .sort_values(id)\
            .reset_index(drop=True)
        
        return cls(df, id=id, add_cols=add_cols, null_flag_cols=null_flag_cols, log=log)

    def add_sup_data(self, 
                     sup_df: pd.DataFrame, 
                     data_id: str, 
                     add_cols: List[str], 
                     one_to_one: bool = True,
                     from_year: int = 2017) -> Self:
        self.supplemental = FoiaData(sup_df, id=data_id, add_cols=add_cols, one_to_one=one_to_one,
                                     null_flag_cols=self.reference.null_flag_cols, from_year=from_year, 
                                     log=self.log)
        return self

    def loop_merge(self, merges) -> Self:
        self.merges += merges
        for idx, merge in enumerate(merges):
            merged_df = merge.apply_merge(self.reference, self.supplemental)

            self.supplemental.unmerged = self.supplemental.filter_merges(merged_df, self.supplemental.unmerged)
        
            if self.supplemental.one_to_one:
                self.reference.unmerged = self.reference.filter_merges(merged_df, self.reference.unmerged)
            self.log_merge(merge, idx)

        self.merged_df = pd.concat([merge.merged_df for merge in merges])

        # do this again here after loop in case supplemental is not one_to_one and filtering never happened during each merge
        self.reference.unmerged = self.reference.filter_merges(self.merged_df, self.reference.unmerged)

        report = f"Final Merge Report: \n{self.get_merge_report(self.merged_df)}"
        report += f"\n{self.merged_df.matched_on.value_counts()}"
        self.log.info(report)

        return self

    def append_to_reference(self, keep_sup_um: bool = True, drop_cols: List[str] = [], sequential_ids: bool=True) -> Self:
        id_cols = [self.reference.id, self.supplemental.id]
        # combine merged df with unmerged reference ids to get all reference ids
        link_df = pd.concat([self.merged_df,
                            self.reference.unmerged[[self.reference.id]].drop_duplicates()])

        # if there are unmerged sup rows to keep, assign them a new unique reference id 
        if keep_sup_um and not self.supplemental.unmerged.empty:
            sup_link = self.supplemental.unmerged[[self.supplemental.id]]\
                .drop_duplicates()\
                .reset_index(drop=True)
            sup_link[self.reference.id] = sup_link.index + max(link_df[self.reference.id]) + 1

            # if reference file has sequential ids, check that the new ids are sequential
            if sequential_ids:
                assert max(sup_link[self.reference.id]) - min(link_df[self.reference.id]) + 1 == \
                    (link_df[self.reference.id].nunique() +
                    self.supplemental.unmerged[self.supplemental.id].nunique()),\
                    'Link DF + sup_link wrong size.'

            link_df = pd.concat([link_df, sup_link])\
                .sort_values(self.reference.id)\
                .reset_index(drop=True)

        self.supplemental.df = self.supplemental.df.merge(link_df, on=self.supplemental.id)
        
        self.reference.df = pd.concat([self.reference.df, self.supplemental.df.drop(drop_cols, axis=1)],
                                      ignore_index=True)
        
        if sequential_ids:
            id_diff = max(self.reference.df[self.reference.id]) - min(self.reference.df[self.reference.id]) + 1 
            assert id_diff == self.reference.df[self.reference.id].nunique(), 'Missing some uids'

        self.reference.df[id_cols] = self.reference.df[id_cols]\
            .apply(pd.to_numeric)
        
        return self

    def write_reference(self, output_path, csv_opts) -> Self:
        self.reference.df.to_csv(output_path, **csv_opts)
        return self

    def remerge_to_file(self, input_path, output_path, csv_opts) -> Self:
        """Merges sup_df (with uids) to input_path, writes data to output_path

        Parameters
        ----------
        input_path : str
            Path of input file (csv)
        output_path : str
            Path of output file (csv)
        csv_opts : dict
            Dictionary of pandas .to_csv options

        Returns
        ----------
        self
        """
        full_df = pd.read_csv(input_path)
        rows = full_df.shape[0]
        link_df = self.reference.df[[self.reference.id, self.supplemental.id]].drop_duplicates()
        full_df = full_df.merge(link_df, on=self.supplemental.id, how='left')
        assert full_df.shape[0] == rows, 'Missing rows!'
        full_df.to_csv(output_path, **csv_opts)
        return self
    
    def get_merge_report(self, merged_df: pd.DataFrame) -> str:
        total_merged = merged_df.shape[0]
        
        merge_report = ""
        merge_report += f"{total_merged} rows merged. "
        merge_report += f"{self.reference.merged_percent(total_merged)}% of ref and "
        merge_report += f"{self.supplemental.merged_percent(total_merged)}% of sup merged.\n"
        merge_report += f"{self.reference.unmerged[self.reference.id].nunique()} unmerged in ref. "
        merge_report += f"{self.reference.unmerged_percent()}% unmerged.\n"
        merge_report += f"{self.supplemental.unmerged[self.supplemental.id].nunique()} unmerged in sup. "
        merge_report += f"{self.supplemental.unmerged_percent()}% unmerged."

        return merge_report
    
    def log_merge(self, merge: Merge, idx: int) -> str:
        report = merge.get_merge_criteria_report(idx, len(self.merges))
        report += f"{self.get_merge_report(merge.merged_df)}\n"

        self.log.info(report)

    def reset_reference(self) -> Self:
        # helper function for testing
        self.reference.unmerged = self.reference.df.copy()

        return self

    @property
    def custom_merges(self):
        return [col for merge in self.merges for col in merge.custom_merges]
    


