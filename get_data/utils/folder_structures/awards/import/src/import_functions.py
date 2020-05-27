#!usr/bin/env python3
#
# Author(s):   RR, TH, GS

'''functions used in the import step'''

import datetime
import io
import re
import yaml
import pandas as pd
import numpy as np
import os


def read_p046957_file(input_file, original_crid_col,
                      notnull="", isnull="", drop_col="",
                      drop_col_val=(), add_skip=1,
                      original_crid_mixed=False):
    """Imports Excel file from FOIA p046957 (Nov 2016) and makes it usable
       by dropping rows and columns when necessary and isolating metadata.
       Returns a formatted dataframe, report produced datetime, and FOIA string

    Parameters
    ----------
    input_file : str
        File path
    original_crid_col: str
        Name of column which holds cr_id information
    notnull : str
        Name of column from which rows with null values will be dropped
    isnull : str
        Name of column from which rows with null values will be kept
    drop_col : str
        Name of column to be dropped
    drop_col_val : tuple (str, str/int/float/etc.)
        Tuple of column name and value for which rows will be dropped
    add_skip : int
        Number of additional rows that should be skipped to avoid metadata
    original_crid_mixed : bool
        If original_crid_col is mixed with other data,
        specifying True will drop rows containing non-crids

    Returns
    -------
    out_df : pandas dataframe
    report_produced_date : datetime
    FOIA_request : str
    """

    df = pd.read_excel(input_file, rows=20,
                       keep_default_na=False, na_values=[''])

    col_list = df.columns.tolist()
    report_produced_date = [x for x in col_list
                            if isinstance(x, datetime.datetime)]
    col_list = [x for x in col_list if x not in report_produced_date]
    FOIA_request = [x for x in col_list if 'FOIA' in x][0]

    skip = np.where(df.iloc[:, 0].str.contains('Number', na=False))[0][0] \
        + add_skip

    df = pd.read_excel(input_file, skiprows=skip,
                       keep_default_na=False, na_values=[''])

    df = df.dropna(how='all', axis=(0, 1))

    df.insert(0, 'CRID',
              (pd.to_numeric(df[original_crid_col],
                             errors='coerce')
               .fillna(method='ffill')
               .astype(int)))

    if notnull:
        df = df[df[notnull].notnull()]
    if isnull:
        df = df[df[isnull].isnull()]
    if drop_col_val:
        df = df[df[drop_col_val[0]] != drop_col_val[1]]
    if drop_col:
        df.drop(drop_col, axis=1, inplace=True)

    if original_crid_mixed:
        df = df[df[original_crid_col] != df['CRID'].astype(str)]
    else:
        df.drop(original_crid_col, axis=1, inplace=True)
    df.dropna(thresh=2, inplace=True)

    out_df = df.dropna(how='all', axis=(0, 1))

    return out_df, report_produced_date, FOIA_request


def standardize_columns(col_names, file_path_key):
    """Standardizes input col_names by using column_names.yaml file's
       specified column name changes, determined by file_path_key.
    Parameters
    ----------
    col_names : list
    file_path_key : str
        Key that specifies column_names.yaml file specific column name changes

    Returns
    -------
    standard_cols : list
    """
    # reaching into share folder in utils
    working_path = os.getcwd()
    column_names_path = '/'.join(working_path.split('/')[:-2])
    column_names_path += '/share/hand/column_names.yaml'
    # Try to read the reference file for converting column names
    with open(column_names_path) as file:
        col_dict = yaml.load(file)
    # Ensure that file path key is in col dict keys
    assert file_path_key in col_dict.keys(),\
        ('{0} is the file path key, but it is not in col_dict kets: {1}'
         '').format(file_path_key, col_dict.keys())
    # Get file specific column name dictionary
    colname_dict = col_dict[file_path_key]
    standard_cols = [colname_dict[col_name] for col_name in col_names]
    # Return standardized columns
    return standard_cols


def collect_metadata(df, infile, outfile, notes=0):
    """Assembles metadata on input dataframe into a metadata dataframe,
       includes unique values and non null values in each column,
       as well as the input and output file name, and specificed notes column

    Parameters
    ----------
    df : pandas dataframe
    infile : str
        File name that df was initially read from
    outfile : str
        File name that df will be written to
    notes : pandas series
        Pandas series of notes to be added as a 'Notes' column
        if not a pandas series, no column will be created

    Returns
    -------
    metadata_df : pandas dataframe
    """
    buf = io.StringIO()
    df.info(buf=buf)
    s = buf.getvalue()
    info_values = [re.split("\\s\\s+", x) for x in s.split("\n")]
    info_values = [x for x in info_values if len(x) > 1]
    metadata_df = pd.DataFrame(info_values)
    metadata_df["In File"] = infile
    metadata_df["Out File"] = outfile
    metadata_df.columns = [
        "Column_Name", "Column_Info",
        "Original_Dataset", "Output_Dataset"
        ]

    metadata_df['Non_Null_Count'], metadata_df['Object_Type'] = \
        metadata_df['Column_Info'].str.split(' ', 1).str
    metadata_df["Object_Type"] = \
        metadata_df["Object_Type"].str.replace("non-null ", "")

    uniques_df = df.apply(lambda x: len(x.unique())).reset_index()
    uniques_df.columns = ["Column_Name", "Unique_Count"]
    metadata_df["Unique_Count"] = uniques_df["Unique_Count"]
    cols = [
        "Original_Dataset", "Output_Dataset",
        "Column_Name", "Non_Null_Count",
        "Unique_Count", "Object_Type"
        ]

    if isinstance(notes, pd.Series):
        metadata_df = metadata_df.join(notes.reset_index(drop=True))
        cols.append('Notes')
    metadata_df = metadata_df[cols]

    return metadata_df
