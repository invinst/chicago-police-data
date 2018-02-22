#!usr/bin/env python3
#
# Author(s): Roman Rivera (Invisible Institute)

'''functions used to merge datasets'''

from collections import OrderedDict
import re
import copy
import itertools
import pandas as pd
import numpy as np

from general_utils import remove_duplicates, keep_duplicates, \
                          reshape_data, fill_data,\
                          list_intersect, list_diff, list_union

np.seterr(divide='ignore')
pd.options.display.max_rows = 99
pd.options.display.max_columns = 99


class ReferenceData:
    def __init__(self, data_df, uid, log, data_id=None, starting_uid=1):
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

        if uid not in data_df:
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
            self.ref_df = data_df

        self.uid = uid
        self.sup_id = data_id

    def prepare_data(self, df, df_id,
                     add_cols=None, fill_cols=None):
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
            if (isinstance(add_col, dict) and
                    add_col['id'] in [df_id, '']):
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
                    df['current_age_m1'] = df['current_age'] + (2017-from_year)
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
                         if isinstance(base_OD_edits, OrderedDict)
                         else OrderedDict(base_OD_edits))
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
        merge_report = ('Merge Report:\n'
                        '{0} Total Merged. {1}% of ref and {2}% of sup Merged.'
                        '\n{3} Unmerged in ref. {4}% Unmerged.\n'
                        '{5} Unmerged in sup. {6}% Unmerged.'
                        '').format(total_merged, prcnt_m_ref, prcnt_m_sup,
                                   unmerged_ref, prcnt_um_ref,
                                   unmerged_sup, prcnt_um_sup)
        self.log.info(merge_report)

    def add_sup_data(
            self, sup_df, add_cols,
            base_OD=OrderedDict([
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
            fill_cols=None):
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

        Returns
        ----------
        self
        """
        self.sup_id = [i for i in sup_df.columns if i.endswith('_ID')][0]
        self.base_OD = (base_OD if isinstance(base_OD, OrderedDict)
                        else OrderedDict(base_OD))
        self.log.info('Adding file with intrafile ID: %s', self.sup_id)
        self.sup_df = self.prepare_data(sup_df, self.sup_id)
        if (("birth_year" not in
             list_intersect(self.ref_df.columns, self.sup_df.columns))
                and ('current_age' in
                     list_union(self.ref_df.columns, self.sup_df.columns))):
            add_cols.extend(["BY_to_CA", "current_age"])
        self.ref_um = self.prepare_data(self.ref_df.copy(), self.uid, add_cols,
                                        fill_cols=fill_cols)
        self.sup_um = self.prepare_data(self.sup_df.copy(), self.sup_id, add_cols)
        return self

    def loop_merge(
            self, custom_merges=[], verbose=True, one_to_one=True,
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
                                  intersect_cols].drop_duplicates()
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
                self.sup_um = self.sup_um.loc[
                    ~self.sup_um[self.sup_id].isin(self.merged_df[self.sup_id])
                    ]
        self.merged_df.reset_index(drop=True, inplace=True)
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
        link_df = pd.concat([self.merged_df[self.id_cols],
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
            link_df[self.id_cols], on=self.sup_id, how='left')
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


    def generate_foia_dates(self):
        """Generates column of FOIA dates based on _ID columns

        Returns
        ----------
        self
        """
        id_cols = [col for col in self.ref_df.columns if col.endswith("_ID")]
        for idc in id_cols:
            fd = idc.split('_')[2] + "-01"
            assert len(fd.split('-')) == 3, print(fd)
            if 'foia_date' in self.ref_df.columns:
                assert self.ref_df[
                    (self.ref_df[idc].notnull() &
                     self.ref_df['foia_date'].notnull())
                    ].empty, "Overlapping IDs"

            self.ref_df.loc[self.ref_df[idc].notnull(),
                            'foia_date'] = fd
        return self

    def final_profiles(
            self, aggregate_data_args, output_path='',
            column_order=[], csv_opts={}, include_IDs=True):
        """Generates unique profiles from reference data and writes to csv

        Parameters
        ----------
        aggregate_data_args : dict
            Dictionary of arguments for aggregate_data
        column_order : list
            List of columns in specified order
        output_path : str
        csv_opts : dict
        include_IDs : bool
            If True, keep ID columns, if False drop them

        Returns
        ----------
        self
        """

        from assign_unique_ids_functions import aggregate_data
        self.generate_foia_dates()
        profiles = aggregate_data(self.ref_df, self.uid,
                                  **aggregate_data_args)
        count_df = pd.DataFrame(
            self.ref_df[
                [col for col in self.ref_df.columns
                 if col.endswith("_ID") or col == self.uid]
                ].drop_duplicates()[self.uid].value_counts())
        count_df.columns = ['profile_count']
        count_df[self.uid] = count_df.index
        profiles = profiles.merge(count_df, on=self.uid)
        assert profiles.shape[0] == self.ref_df[self.uid].nunique(),\
            print(profiles.shape[0], self.ref_df[self.uid].nunique())

        if include_IDs:
            ID_cols = [col for col in profiles.columns if col.endswith('_ID')]
        else:
            ID_cols = []

        if column_order:
            cols = [col for col in column_order if col in profiles.columns]

        profiles = profiles[[self.uid] + cols + ID_cols + ['profile_count']]
        self.log.info('Officer profile count: {}'.format(profiles.shape[0]))

        if output_path:
            profiles.to_csv(output_path, **csv_opts)
        else:
            self.profiles = profiles
        return self
