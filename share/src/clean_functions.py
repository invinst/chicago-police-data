#!usr/bin/env python3
#
# Author(s):   Roman Rivera (Invisible Institute)

'''functions used in the clean step'''

import yaml
import numpy as np
import pandas as pd

from general_utils import collapse_data, expand_data
from clean_utils import GeneralCleaners, DateTimeCleaners


def clean_data(df, log, skip_cols=None, clean_dict=None, types_dict=None,
               use_middle_names=False):
    """Cleans input dataframe in using standard cleaning functions

    Parameters
    ----------
    df : pandas DataFrame
    log : logging object
        Used for info/warnings during cleaning process
    skip_cols : list
        List of column names in df that are explicitly not cleaned
    clean_dict : dict (of dicts)
        Dictionary containing dictionaries for cleaning (main key = column name)
    types_dict : dict
        Dictionary of column types

    Returns
    -------
        cleaned_df : pandas DataFrame
    """
    name_cols = []
    cleaned_df = pd.DataFrame()
    if clean_dict is None: clean_dict = {}
    if skip_cols is None: skip_cols = []
    if types_dict is None:
        with open('hand/column_types.yaml', 'r') as file:
            types_dict = yaml.load(file)

    for col_name in df.columns.values:
        if col_name in skip_cols or col_name not in types_dict.keys():
            log.info("Column '%s' was not cleaned.", col_name)
            cleaned_df[col_name] = df[col_name].copy()

        elif col_name in clean_dict.keys():
            log.info("Column '%s' cleaned using dictionary", col_name)
            cdict = clean_dict[col_name]
            cleaned_col = df[col_name].replace(cdict)
            missing = cleaned_col[~cleaned_col.isin(cdict.values())]
            if missing.size > 0 and log:
                log.warning("%s values not in dictionary."
                            " %d cases replaced with '%s'"
                            % (missing.unique().tolist(), missing.size, ''))
            cleaned_col[~cleaned_col.isin(cdict.values())] = ''
            cleaned_df[col_name] = cleaned_col

        elif types_dict[col_name] == 'name':
            name_cols.append(col_name)

        elif types_dict[col_name] in ['datetime', 'date', 'time']:
            log.info("Column '%s' cleaned as %s using DateTimeCleaners.",
                     col_name, types_dict[col_name])
            collapsed_data, stored_df = collapse_data(df[[col_name]])
            cleaned_dt_df = DateTimeCleaners(collapsed_data[col_name],
                                             types_dict[col_name], log).clean()
            cleaned_dt_df = expand_data(cleaned_dt_df, stored_df)
            cleaned_df = cleaned_df.join(cleaned_dt_df)

        else:
            log.info("Column '%s' cleaned as %s using GeneralCleaners.",
                     col_name, types_dict[col_name])
            cleaned_df[col_name] = GeneralCleaners(df[col_name],
                                                   types_dict[col_name],
                                                   log).clean()

    if name_cols:
        from clean_name_utils import NameCleaners, clean_human_names

        log.info("Column(s) '%s' cleaned as name(s) using NameCleaners.",
                 tuple(name_cols))
        name_df, stored_df = collapse_data(df[name_cols])
        name_df = name_df.fillna('')
        if name_df.shape[1] == 1 and name_df.columns[0] == 'human_name':
            cleaned_names_df = \
                clean_human_names(name_df[0].tolist(),
                                  use_middle_names=use_middle_names)
        else:
            for col in name_df.columns:
                name_df[col] = name_df[col].str.upper()

            cleaned_names_df = pd.DataFrame([NameCleaners(**row.to_dict()).clean()
                                             for i, row in name_df.iterrows()])
            cleaned_names_df[cleaned_names_df == ''] = np.nan
            cleaned_names_df = expand_data(cleaned_names_df,
                                           stored_df)
            cleaned_df = cleaned_df.join(cleaned_names_df)

    df_cols = cleaned_df.columns.tolist()
    cleaned_df.dropna(axis=1, how='all', inplace=True)
    dropped_cols = tuple(set(df_cols) - set(cleaned_df.columns))
    log.info('Columns dropped due to all NA values: %s', dropped_cols)

    return cleaned_df


#
# end
