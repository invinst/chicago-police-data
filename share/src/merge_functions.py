#!usr/bin/env python3
#
# Author(s): Roman Rivera (Invisible Institute)

'''functions used to merge datasets'''

from collections import OrderedDict
import re
import copy
import itertools
from typing import Dict
import pandas as pd
import numpy as np

from general_utils import remove_duplicates, keep_duplicates, \
                          reshape_data, fill_data,\
                          list_intersect, list_diff, list_union

np.seterr(divide='ignore')
pd.options.display.max_rows = 99
pd.options.display.max_columns = 99


class ReferenceData:
    def __init__(self, data_df, uid, log, data_id=None, null_flag_cols=[], starting_uid=1):
        """Creates reference data which can continually absorb other data sets

        Creates a reference data object, which holds a reference DataFrame that
        contains unique ids for all observations determined to be from the same
        officer. This data is used to match against other data sets and absorb
        the information of both matched and unmatched observations.

        Supplementary (sup) data sets can be added using add_sup_data() method,
        then merged to the reference data with loop_merge(),
        and the reference data can be updated based on matched and unmatched
        data using append_to_reference().
        A file containing the same intra-file IDs as the sup data can be
        given the uids using remerge_to_file().

        Parameters
        ----------
        data_df : pandas DataFrame
            Initial data set used to match other data sets against
        uid : str
            Name of Unique ID column used to identify individuals
        log : logging object
        data_id : str
            Name of intra-file ID column in initial input data
        starting_uid : int
            Starting value for uids
        """
        self.log = log

        if uid not in data_df.columns:
            assert (data_id and data_id in data_df.columns), "Need data_id"
            self.log.info('Creating reference data from file'
                          'with intrafile ID: %s', data_id)
            ref_df = self.prepare_data(data_df, data_id)
            uid_df = ref_df[[data_id]]\
                .drop_duplicates()\
                .sort_values(data_id)\
                .reset_index(drop=True)
            uid_df[uid] = uid_df.index + starting_uid
            self.ref_df = ref_df.merge(uid_df, on=data_id, how='left')\
                .sort_values(uid)\
                .reset_index(drop=True)
        else:
            self.log.info('Loading reference data with '
                          '%d rows and %d universal IDs',
                          data_df.shape[0], data_df[uid].nunique())
            if null_flag_cols:
                data_df = self.calculate_null_flags(data_df, uid, null_flag_cols)
            else:
                self.null_flag_cols = []
                
            self.ref_df = data_df



        self.uid = uid
        self.sup_id = data_id
        self.null_flag_cols = []

    def prepare_data(self, df, df_id,
                     add_cols=None, fill_cols=None, reshape_cols=[]):
        """Prepares dataframe for merging

        Parameters
        ----------
        df : pandas DataFrame
        df_id : str
            Name of unique identifier in df
        add_cols : list
            List of column names for columns to be added in add_columns()
        fill_cols : list
            List of columns to be selected for fill_data()
        reshape_cols: list or dict
            List of columns, or dict of ids mapping to columns, to be reshaped from wide to long.
            

        Returns
        ----------
        df : pandas DataFrame
        """
        rows = df.shape[0]
        if 'merge' in df.columns:
            df = df[df['merge'] == 1].drop('merge', axis=1)
            self.log.info('%d rows dropped due to merge!=1. %d rows remaining',
                          rows - df.shape[0], df.shape[0])
            rows = df.shape[0]
        if 'star1' in df.columns:
            df = reshape_data(df, 'star', df_id)
            self.log.info('Data reshaped wide (%d rows) '
                          'to long (%d rows) by star columns',
                          rows, df.shape[0])
        if 'current_star' in df.columns and 'star' not in df.columns:
            df['star'] = df['current_star']

        if isinstance(reshape_cols, list):
            for col in reshape_cols:
                df = reshape_data(df, col, df_id)
                self.log.info(f'Data reshaped wide ({rows} rows) to long ({df.shape[0]} rows) by {col} columns')
        elif isinstance(reshape_cols, dict):
            for id, col in reshape_cols.items():
                # add index if id col not unique, just for pivot, then drop
                if id == 'index':
                    df = reshape_data(df.reset_index(drop=False), col, 'index').drop("index", axis=1)
                else:
                    df = reshape_data(df.reset_index(drop=False), col, id)
                self.log.info(f'Data reshaped wide ({rows} rows) to long ({df.shape[0]} rows) by {col} columns')

        if isinstance(fill_cols, list):
            self.log.info('Beginning fill_data() on ref_df for %s columns'
                          ' by %s, with initial rows = %d',
                          fill_cols, self.uid, df.shape[0])
            df = fill_data(df[[self.uid] + fill_cols], self.uid)
            self.log.info('fill_data() complete, final rows = %d', df.shape[0])

        if add_cols:
            from_year = (int(df_id.split('_')[2][:4])
                         if df_id != self.uid else 2017)
            df = self.add_columns(df, df_id, add_cols, from_year)

        return df
    

    def add_columns(self, df, df_id, add_cols, from_year):
        """Adds columns to dataframe

        Parameters
        ----------
        df : pandas DataFrame
        df_id : str
        add_cols : list
            List of columns to be added (or dict)
        from_year : int
            Year of data's original production (for calculating current ages)

        Returns
        ----------
        df : pandas DataFrame
        """
        for add_col in add_cols:
            if (isinstance(add_col, Dict) and (add_col['id'] in [df_id, ''])):
                self.log.info('Adding column by executing \n%s\n'
                              'to data with ID = %s', add_col['exec'], df_id)
                assert not any(cond in add_col['exec']
                               for cond in ['import', 'exec', 'eval'])
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
                    df['current_age_m1'] = df['current_age'] + (2016-from_year)
                    df.drop('current_age', axis=1, inplace=True)
                if add_col == 'BY_to_CA' and 'birth_year' in df.columns:
                    df['current_age_p1'] = 2017 - df['birth_year']
                    df['current_age_m1'] = 2016 - df['birth_year']
                self.log.info('Adding column [%s] to data with ID = %s',
                              add_col, df_id)

        return df

    def generate_on_lists(self, data_cols, custom_merges, base_OD_edits):
        """Generates list of lists on which ref and sup data can be merged

        Filters self.base_OD values by data_cols, and generates each possible
        combination of the column names remaining in base_lists.
        Then custom_merges are added to the end of the generated on_lists.

        Parameters
        ----------
        data_cols : list
            List of columns that appear in both ref and sup data
        custom_merges : list (of lists)
        base_OD_edits : OrderedDict

        Returns
        ----------
        on_lists : list
            List of lists (or dicts), used as 'on' arguments in loop_merge()
        """
        merge_lists = []
        data_cols.append('')
        base_OD_edits = (base_OD_edits
                         if isinstance(base_OD_edits, dict)
                         else dict(base_OD_edits))
        for k, v in base_OD_edits.items():
            self.base_OD[k] = v
        filtered_base_lists = [list_intersect(col_list, data_cols)
                               for col_list in self.base_OD.values()]

        if filtered_base_lists == []:
            return custom_merges
        else:
            filtered_base_lists = [fbl for fbl in filtered_base_lists if fbl]
            on_lists = list(itertools.product(*filtered_base_lists))
            on_lists = [[i for i in ol if i]
                        for ol in on_lists]
            on_lists.extend(custom_merges)
            return on_lists
        
    def calculate_null_flags(self, df, uid, cols):
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
        null_flags = df[cols].isnull().groupby(df[uid]).transform('all').add_suffix("_null")
        self.null_flag_cols = list(null_flags.columns)

        return df.join(null_flags)

    def log_merge_report(self, total_merged, total_ref, total_sup):
        """logs formatted report of merging process as info

        Parameters
        ----------
        total_merged : int
            Total number of unique merged pairs
        total_ref : int
            Total number of unique uids in ref
        total_sup : int
            Total number of unique intra-file ids in sup
        """
        unmerged_ref = total_ref - total_merged
        unmerged_sup = total_sup - total_merged
        prcnt_m_ref = round(100 * total_merged / total_ref, 2)
        prcnt_m_sup = round(100 * total_merged / total_sup, 2)
        prcnt_um_ref = round(100 * unmerged_ref / total_ref, 2)
        prcnt_um_sup = round(100 * unmerged_sup / total_sup, 2)

        self.merge_report = pd.Series({
            "total_ref": total_ref,
            "total_sup": total_sup,
            "unmerged_ref": unmerged_ref,
            "unmerged_sup": unmerged_sup,
            "total_merged": total_merged,
            "prcnt_m_ref": prcnt_m_ref,
            "prcnt_m_sup": prcnt_m_sup,
            "prcnt_um_ref": prcnt_um_ref,
            "prcnt_um_sup": prcnt_um_sup})
        merge_report_log = ('Merge Report:\n'
                        '{0} Total Merged. {1}% of ref and {2}% of sup Merged.'
                        '\n{3} Unmerged in ref. {4}% Unmerged.\n'
                        '{5} Unmerged in sup. {6}% Unmerged.'
                        '').format(total_merged, prcnt_m_ref, prcnt_m_sup,
                                   unmerged_ref, prcnt_um_ref,
                                   unmerged_sup, prcnt_um_sup)
        self.log.info(merge_report_log)

    def add_sup_data(
            self, sup_df, add_cols,
            base_OD=dict([
                ('star', ['star', '']),
                ('first_name', ['first_name_NS', 'F4FN']),
                ('last_name', ['last_name_NS', 'F4LN']),
                ('appointed_date', ['appointed_date']),
                ('birth_year', ['birth_year', 'current_age', '']),
                ('middle_initial', ['middle_initial', '']),
                ('middle_initial2', ['middle_initial2', '']),
                ('gender', ['gender', '']),
                ('race', ['race', '']),
                ('suffix_name', ['suffix_name', '']),
                ('current_unit', ['current_unit', ''])]),
            fill_cols=None,
            reshape_cols=[]):
        """Adds and prepares supplementary (sup) data to ReferenceData object

        Adds supplementary data to ReferenceData object.
        Prepares sup_df for appending to reference.
        Creates/prepares both ref_um and sup_um (unmerged) data
        with added columns in prepratation for loop_merge()

        Parameters
        ----------
        sup_df : pandas DataFrame
        add_cols : list
            List of columns/codes to be added to both ref_um and sup_um
        base_OD : OrderedDict
        fill_cols : list
            columns used for fill_data
        reshape_cols : list
            columns to be reshaped from wide to long in prepare

        Returns
        ----------
        self
        """
        self.sup_id = [i for i in sup_df.columns if i.endswith('_ID')][0]
        self.base_OD = (base_OD if isinstance(base_OD, OrderedDict)
                        else OrderedDict(base_OD))
        self.log.info('Adding file with intrafile ID: %s', self.sup_id)
        self.sup_df = self.prepare_data(sup_df, self.sup_id, reshape_cols=reshape_cols)
        if (("birth_year" not in
             list_intersect(self.ref_df.columns, self.sup_df.columns))
                and ('current_age' in
                     list_union(self.ref_df.columns, self.sup_df.columns))):
            add_cols.extend(["BY_to_CA", "current_age"])
        self.ref_um = self.prepare_data(self.ref_df.copy(), self.uid, add_cols, fill_cols=fill_cols)
        self.sup_um = self.prepare_data(self.sup_df.copy(), self.sup_id, add_cols)
        return self

    def loop_merge(
            self, custom_merges=[], verbose=True, one_to_one=True, multiple_merges=False,
            base_OD_edits=OrderedDict()):
        """Performs iterative pairwise joins to produce dataframe of merges

        Loops over on_lists to iteratively merge ref_um and sup_um, continually
        removing unique ids in sup_um (and ref_um if one_to_one=True) that
        were successfully merged.

        Parameters
        ----------
        custom_merges : list (of lists or dicts)
            List of user entered custom merge lists
        verbose : bool
            If True successful merges are printed
        one_to_one : bool
            If True assumes sup_um is successfully deduplicated and uids are
            dropped from ref_um after successful merges
            If False uids are not dropped from ref_um after successful merges
        multiple_merges : bool 
            If False, uids are removed from sup_um after being merged to ref_df
            If True, uids are not removed from sup_um. 
            Allows for composite merging, usually as a second remerge step
        base_OD_edits : OrderedDict
            Ordereddict of alterations to base_OD (preserving initial order)
        Returns
        ----------
        self
        """
        intersect_cols = list_diff(list_intersect(self.ref_um.columns,
                                                  self.sup_um.columns),
                                   [self.uid, self.sup_id])
        self.ref_um = self.ref_um[[self.uid] +
                                  intersect_cols + 
                                  self.null_flag_cols].drop_duplicates()
        self.sup_um = self.sup_um[[self.sup_id] +
                                  intersect_cols].drop_duplicates()

        ref_ids = self.ref_um[self.uid].nunique()
        sup_ids = self.sup_um[self.sup_id].nunique()
        multi = self.sup_um[self.sup_id].size > sup_ids

        self.on_lists = self.generate_on_lists(intersect_cols, custom_merges,
                                               base_OD_edits)
        self.id_cols = [self.uid, self.sup_id]
        self.merged_df = pd.DataFrame(columns=self.id_cols + ['matched_on'])
        self.log.info('Beginning loop_merge.')
        for merge_cols in self.on_lists:
            assert len(merge_cols) > 0
            reft = self.ref_um
            supt = self.sup_um
            if isinstance(merge_cols, dict):
                # allow split to separate sup and merge query
                if ("sup_query" in merge_cols) and ("ref_query" in merge_cols):
                    reft = reft.query(merge_cols['ref_query'])
                    supt = supt.query(merge_cols['sup_query'])
                else:
                    reft = reft.query(merge_cols['query'])
                    supt = supt.query(merge_cols['query'])
                merge_cols = merge_cols['cols']
            reft = remove_duplicates(reft[[self.uid] +
                                          merge_cols].dropna(how='any'),
                                     merge_cols, True)
            if one_to_one:
                supt = remove_duplicates(supt[[self.sup_id] +
                                              merge_cols].dropna(how='any'),
                                         merge_cols, True)
            else:
                supt = supt[[self.sup_id] + merge_cols].dropna(how='any')
            mergedt = reft.merge(supt,
                                 on=merge_cols, how='inner')[self.id_cols]
            if multi:
                mergedt = mergedt.drop_duplicates()
            if mergedt.shape[0] > 0:
                if verbose:
                    print('%d Matches on \n %s columns'
                          %(mergedt.shape[0], merge_cols))
                mergedt['matched_on'] = '-'.join(merge_cols)
                self.merged_df = self.merged_df\
                    .append(mergedt[self.id_cols + ['matched_on']])\
                    .reset_index(drop=True)
                if one_to_one:
                    self.ref_um = self.ref_um.loc[
                        ~self.ref_um[self.uid].isin(self.merged_df[self.uid])
                        ]
                if not multiple_merges:
                    self.sup_um = self.sup_um.loc[
                        ~self.sup_um[self.sup_id].isin(self.merged_df[self.sup_id])
                        ]
        self.merged_df.reset_index(drop=True, inplace=True)
        if verbose:
            self.log_merge_report(self.merged_df.shape[0], ref_ids, sup_ids)
            self.log.info('\n%s', self.merged_df['matched_on'].value_counts())
        if one_to_one:
            kds = keep_duplicates(self.merged_df, [self.uid])
            assert kds.empty,\
                print('Same UID matched to multiple sup_ids %s'
                      '\n Ref: %s \n Sup: %s'
                      % (kds,
                         self.ref_df[
                            self.ref_df[self.uid].isin(kds[self.uid])
                            ].sort_values(self.uid),
                         self.sup_df[
                            self.sup_df[self.sup_id].isin(kds[self.sup_id])
                            ].sort_values(self.sup_id)))
        if not multiple_merges:
            kds = keep_duplicates(self.merged_df, [self.sup_id])
            assert kds.empty,\
                print('Same sup_id matched to multiple UIDs %s\nRef: %s\nSup: %s'
                    % (kds,
                        self.ref_df[
                            self.ref_df[self.uid].isin(kds[self.uid])
                            ].sort_values(self.uid),
                        self.sup_df[
                            self.sup_df[self.sup_id].isin(kds[self.sup_id])
                            ].sort_values(self.sup_id)))

        return self
    
    def multirow_loop_merge(self, required_columns, sup_um=None):
        """Merges instances where required columns match to the same UID, but not all on the same row. 
        E.g., if first name and appointed date match one row, but appointed date match another for the same UID in ref
        
        Merges the same unmerged rows back to ref_df but with only one required column at a time
        Does this without removing matches
        Then combines all match columns to see if each required column was used at least once

        Parameters
        ----------
        required_columns : list[str]
            columns that must be matched on at least one row 
        sup_um : pd.DataFrame, optional
            supplemental unmerged, option to provide it manually
            useful if there's a known query/criteria to avoid trying to remerge, by default None

        Returns
        -------
        self
            reference data object, with merges added
        """        
        # keep original state: current code is heavily state coupled so need to copy and resave
        # TODO: refactor merge code so this is isn't necessary
        self.log.info("Beginning multirow loop merge")

        original_merged_df = self.merged_df.copy()
        original_sup_df = self.sup_df.copy()
        original_ref_um = self.ref_um.copy()
        original_base_OD = copy.deepcopy(self.base_OD)

        if not sup_um:
            original_sup_um = self.sup_um.copy()
        else:
            original_sup_um = sup_um


        for require_col in required_columns:
            if not original_sup_um.empty:
                self.log.info(f"Readding unmerged, calling loop merge fixing {require_col}")
                # require only required column, loosen restriction on others
                base_OD_edits = {col: original_base_OD[col] + [''] for col in required_columns 
                                if col != require_col}

                self.add_sup_data(original_sup_um, add_cols=[]) 

                # reset base OD since it's being edited every iteration
                self.base_OD = copy.deepcopy(original_base_OD)

                self.loop_merge(custom_merges=[], 
                                multiple_merges=True, 
                                one_to_one=False, 
                                base_OD_edits=base_OD_edits,
                                verbose=False) \

                relaxed_merges = self.merged_df.assign(matched_on=self.merged_df.matched_on.str.split('-')) \
                    .explode('matched_on') \
                    .drop_duplicates() \
                    .groupby(["UID", self.sup_id]) \
                    .agg(matched_on=('matched_on', '-'.join))
                
                def get_mask(df, cols):
                    mask = [True]
                    for col in cols:
                        mask = mask & df.matched_on.str.contains(col)
                    return mask
                
                core_mask = get_mask(relaxed_merges, required_columns)
                
                new_matches = relaxed_merges[core_mask].reset_index()
                
                self.log.info(f"Adding merges with fixed {require_col}:")
                self.log.info(f"\n{new_matches['matched_on'].value_counts()}")
                # update original values 
                original_merged_df = pd.concat([original_merged_df, new_matches], axis=0).reset_index(drop=True)
                original_ref_um = original_ref_um.loc[
                                        ~original_ref_um[self.uid].isin(original_merged_df[self.uid])
                                        ]
                original_sup_um = original_sup_um.loc[
                    ~original_sup_um[self.sup_id].isin(original_merged_df[self.sup_id])
                    ]
                
        # Restore original states now updated 
        self.sup_df = original_sup_df
        self.base_OD = original_base_OD
        self.sup_um = original_sup_um
        self.ref_um = original_ref_um
        self.merged_df = original_merged_df

        self.log.info("\nFinal Merge Report:")
        self.log_merge_report(self.merged_df.shape[0], 
                              self.ref_df[self.uid].nunique(), 
                              self.sup_df[self.sup_id].nunique())
        self.log.info(self.merged_df.matched_on.value_counts())
        

        return self

    def append_to_reference(self, keep_sup_um=True, drop_cols=[]):
        """Appends supplementary data to reference data, by appending both
        merged sup_df, and appending and assigning new uids to sup_um

        Parameters
        ----------
        keep_sup_um : bool
        drop_cols : list
            List of columns to drop when appending sup_df to ref_df

        Returns
        ----------
        self
        """
        link_df = pd.concat([self.merged_df[self.id_cols + ['matched_on']],
                             self.ref_um[[self.uid]].drop_duplicates()])

        if keep_sup_um and not self.sup_um.empty:
            sup_link = self.sup_um[[self.sup_id]]\
                .drop_duplicates()\
                .reset_index(drop=True)
            sup_link[self.uid] = sup_link.index + max(link_df[self.uid]) + 1
            assert max(sup_link[self.uid]) - min(link_df[self.uid]) + 1 == \
                (link_df[self.uid].nunique() +
                 self.sup_um[self.sup_id].nunique()),\
                'Link DF + sup_link wrong size.'

            link_df = link_df.append(sup_link)\
                .sort_values(self.uid)\
                .reset_index(drop=True)

        self.sup_df = self.sup_df.merge(
            link_df[self.id_cols + ['matched_on']], on=self.sup_id, how='left')
        self.ref_df = self.ref_df\
            .append(self.sup_df.drop(drop_cols, axis=1))\
            .dropna(subset=[self.uid], axis=0, how='any')\
            .reset_index(drop=True)
        assert max(self.ref_df[self.uid]) - min(self.ref_df[self.uid]) + 1 == \
            self.ref_df[self.uid].nunique(),\
            'Missing some uids'
        self.ref_df[self.id_cols] = self.ref_df[self.id_cols]\
            .apply(pd.to_numeric)
        return self

    def remerge_to_file(self, input_path, output_path, csv_opts):
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
        link_df = self.ref_df[[self.uid, self.sup_id]].drop_duplicates()
        full_df = full_df.merge(
            link_df, on=self.sup_id, how='left')
        assert full_df.shape[0] == rows, 'Missing rows!'
        full_df.to_csv(output_path, **csv_opts)
        return self

    def write_reference(self, output_path, csv_opts):
        """Writes reference (ref_df) to output_path

        Parameters
        ----------
        output_path : str
            Path of output file (csv)
        csv_opts : dict
            Dictionary of pandas .to_csv options

        Returns
        ----------
        self
        """
        self.ref_df.to_csv(output_path, **csv_opts)
        return self

    def generate_merge_report(self, output_filename=None, ordered_cols = ["first_name_NS", "last_name_NS", "star", "cr_id", "log_no", "appointed_date", 
                                                                        "birth_year", "gender", "race", "current_unit", "middle_initial", 
                                                                        "middle_initial2", "suffix_name"]):
        """Generates a report showing each row of the merge: what was used in match and what came from each side
           Shows which columns were used in merged (concat and one hot) and respective columns from ref_df and sup_df with muliindex
           Useful for manually inspecting merges, particularly with filtering

           If given output_filename, will output to excel. If given ordered cols, will preserve that order. 
        """
        match_cols = [col for col in ordered_cols if (col in self.ref_df.columns) and (col in self.sup_df.columns)]
        matches = self.merged_df.join(self.merged_df.matched_on.str.replace("-", "|").str.get_dummies()).set_index([self.uid, self.sup_id])
        ref_df_matches = self.ref_df.groupby(self.uid)[match_cols].agg(lambda x: ", ".join(list(set(x.dropna().astype(str)))))
        sup_df_matches = self.sup_df.groupby(self.sup_id)[match_cols].agg(lambda x: ", ".join(list(set(x.dropna().astype(str)))))

        def make_columns_multiindex(df, key):
            df.columns = pd.MultiIndex.from_product([[key], df.columns])
            return df

        matches = make_columns_multiindex(matches, "match")
        ref_df_matches = make_columns_multiindex(ref_df_matches, "ref")
        sup_df_matches = make_columns_multiindex(sup_df_matches, "sup")

        self.merge_report = matches.join(ref_df_matches).join(sup_df_matches)

        if output_filename:
            self.merge_report.to_excel(output_filename)

        return self

    def add_file_column(self):
        """Adds a file column representing a list of what files (ids) each row came from
        
           Just a convenience method for inspecting merges and ref_df
        """        
        id_cols = self.ref_df.columns[self.ref_df.columns.str.contains("_ID")]
        self.ref_df.loc[:, "file"] = self.ref_df[id_cols].notnull().dot(self.ref_df[id_cols].columns+", ").str.rstrip(", ")

        return self


    def generate_foia_dates(self):
        """Generates column of FOIA dates based on _ID columns, also adds FOIA filename back as a column

        Returns
        ----------
        self
        """
        id_cols = [col for col in self.ref_df.columns if col.endswith("_ID")]
        for idc in id_cols:
            fd = idc.split('_')[2] + "-01"
            foia_name = idc.split("_ID")[0]
            assert len(fd.split('-')) == 3, print(fd)
            if 'foia_date' in self.ref_df.columns:
                assert self.ref_df[(self.ref_df[idc].notnull() & 
                                    self.ref_df['foia_date'].notnull())].empty, "Overlapping IDs"

            self.ref_df.loc[self.ref_df[idc].notnull(), 'foia_date'] = fd

            self.ref_df.loc[self.ref_df[idc].notnull(), "foia_name"] = foia_name

        self.ref_df['match'] = self.ref_df['foia_name'] + ": " + self.ref_df['matched_on'].fillna("")

        self.parse_foia_names()

        return self
    
    def parse_foia_names(self):
        foias = self.ref_df.foia_name

        no_foia_number_mask = foias.str.count("_") == 2

        foias.loc[no_foia_number_mask] = foias.loc[no_foia_number_mask] + "_"

        self.ref_df[['foia_type', 'foia_range', 'foia_received_on', 'foia_number']] = foias.str.split("_", expand=True)

        # remap some types to be their base type, e.g., "complaints-accused" to just "complaints"
        types = ['complaints', 'TRR']

        for type in types:
            type_mask = self.ref_df.foia_type.str.contains(type)
            self.ref_df.loc[type_mask, 'foia_type'] = type

        self.ref_df.foia_received_on = pd.to_datetime(self.ref_df.foia_received_on)

    def final_profiles(
            self, aggregate_data_args, foia_type_order=[], output_path='', column_order=[], 
            add_columns=[], rename_columns={}, csv_opts={}, include_IDs=True):
        """Generates unique profiles from reference data and writes to csv

        Parameters
        ----------
        aggregate_data_args : dict
            Dictionary of arguments for aggregate_data
        column_order : list
            List of columns in specified order
        rename_columns: dict
            Dictionary for renaming columns, especially those with current prefixed from aggregate_data
        output_path : str
        csv_opts : dict
        include_IDs : bool
            If True, keep ID columns, if False drop them

        Returns
        -------
        self
        """

        from assign_unique_ids_functions import aggregate_data
        if foia_type_order:
            foia_type_order_map = dict(zip(foia_type_order, range(len(foia_type_order))))
            self.ref_df = self.ref_df \
                .assign(foia_order=self.ref_df.foia_type.map(foia_type_order_map)) \
                .sort_values(['UID', 'foia_order', 'foia_received_on'], ascending=[True, True, False])
            
        profiles = aggregate_data(self.ref_df, self.uid,
                                  **aggregate_data_args)
        count_df = pd.DataFrame(
            self.ref_df[
                [col for col in self.ref_df.columns
                 if col.endswith("_ID") or col == self.uid]
                ].drop_duplicates()[self.uid].value_counts())
        count_df.columns = ['profile_count']
        count_df[self.uid] = count_df.index
        profiles = profiles.merge(count_df, left_on=self.uid, right_index=True)
        assert profiles.shape[0] == self.ref_df[self.uid].nunique(),\
            print(profiles.shape[0], self.ref_df[self.uid].nunique())

        if include_IDs:
            ID_cols = [col for col in profiles.columns if col.endswith('_ID')]
        else:
            ID_cols = []

        profiles = profiles.rename(columns=rename_columns)

        if column_order:
            cols = [col for col in column_order if col in profiles.columns]

        profiles = profiles[[self.uid] + cols + ID_cols + ['profile_count']]
        self.log.info('Officer profile count: {}'.format(profiles.shape[0]))

        if output_path:
            profiles.to_csv(output_path, **csv_opts)
        else:
            self.profiles = profiles
        return self
    
    def interfile_column_changes(self, col):
        """Get instances where given col value changes between files in existing ref_df for the same UID

        Parameters
        ----------
        col : str
            column to get changes for 

        Returns
        -------
        DataFrame
            dataframe containing all rows in ref_df that have interfile changes in them
        """            
        if "foia_name" not in self.ref_df:
            self.generate_foia_dates()

        change_ids = self.ref_df.groupby("UID", as_index=False)[col] \
            .nunique() \
            .query(f'{col} > 1') \
            .UID
        
        changes = self.ref_df.merge(change_ids, on="UID")

        change_counts = changes.groupby(["UID", col], as_index=False) \
            .agg(**{f"{col}_count": ('foia_name', pd.Series.nunique)})
        
        # split changes by min and max occurrences
        min_max_counts = change_counts.groupby("UID").agg(**{
            f"min_{col}_count": (f'{col}_count', min), 
            f"max_{col}_count": (f'{col}_count', max)})
        
        min_col_value = change_counts.loc[change_counts.groupby("UID")[f'{col}_count'].idxmin()][["UID", col]] \
            .rename(columns={col: f"min_{col}"})
        
        max_col_value = change_counts.loc[change_counts.groupby("UID")[f'{col}_count'].idxmax()][["UID", col]] \
            .rename(columns={col: f"max_{col}"})
        
        changes = changes.merge(change_counts, on=["UID", col]) \
            .merge(min_max_counts, on=["UID"]) \
            .merge(min_col_value, on=["UID"]) \
            .merge(max_col_value, on=["UID"])
        
        # type column: whether this row is from a foia with a column value that is min or max, or other
        min_type_mask = changes[f"min_{col}_count"] == changes[f"{col}_count"]
        max_type_mask = changes[f"max_{col}_count"] == changes[f"{col}_count"]

        changes.loc[min_type_mask, 'type'] = 'min'
        changes.loc[max_type_mask, 'type'] = 'max'
        changes['type'] = changes['type'].fillna('other')

        # flag for which foia is the most recently received foia
        changes.loc[changes.groupby("UID").foia_received_on.idxmax(), 'most_recent_foia'] = True
        changes.most_recent_foia = changes.most_recent_foia.fillna(False)

        changes.loc[changes.most_recent_foia, 'most_recent_foia_name'] = changes.loc[changes.most_recent_foia, 'foia_name']
        changes.most_recent_foia_name = changes.groupby("UID").most_recent_foia_name.ffill().bfill()

        # flag for first recieved foia
        changes.loc[changes.groupby("UID").foia_received_on.idxmin(), 'first_received_foia'] = True
        changes.first_received_foia = changes.first_received_foia.fillna(False)

        changes.loc[changes.first_received_foia, 'first_received_foia_name'] = changes.loc[changes.first_received_foia, 'foia_name']
        changes.first_received_foia_name = changes.groupby("UID").first_received_foia_name.ffill().bfill()

        return changes
    
    def pivot_changes(self, changes, col):
        """Given changes dataframe from interfile_column_changes, show values and foia name 

        Parameters
        ----------
        changes : DataFrame
            same format as from return value of interfile_column_changes
        """        
        pivoted_changes = changes[["UID", 'type', 'foia_name', col]] \
            .pivot_table(index=["UID"], values=[col, 'foia_name'], columns=['type'], aggfunc=lambda x: ', '.join(np.unique(x))) \
            .rename_axis(columns=['values', 'type'])
        
        pivoted_changes.columns = pivoted_changes.columns.reorder_levels(['type', 'values'])
        pivoted_changes = pivoted_changes.sort_index(axis=1)
        
        stable_cols = ['UID', 'last_name_NS', 'first_name_NS', 'appointed_date', 'gender', 'race', 'matched_on']
        stable_cols = [c for c in stable_cols if c != col]
        stable_col_funcs = {stable_col: (stable_col, lambda x: ', '.join(np.unique(x.fillna("")))) for stable_col in stable_cols if stable_col != "UID"}

        stable_values = changes[stable_cols] \
            .groupby("UID") \
            .agg(**stable_col_funcs)
        
        stable_values.columns = pd.MultiIndex.from_product([['stable'], stable_values.columns], names=['type', 'values'])

        return pd.concat([pivoted_changes, stable_values], axis=1)






