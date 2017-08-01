import pandas as pd
import numpy as np
import datetime
import os
import io
import re

input_opts = {'compression': 'gzip'}
output_opts = {'index': False, 'compression': 'gzip'}

def read_p046957_files(path, add_skip=1):
    df = pd.read_excel(path, rows=20)
    col_list = df.columns.tolist()
    # if [col if '\n' in col or len(col) > 40 for col in col_list]:
    #   metadata = True
    report_produced_date = [x for x in col_list if isinstance(x, datetime.datetime)]
    col_list = [x for x in col_list if x not in Report_Produced_Date]
    FOIA_request = [x for x in col_list if 'FOIA' in x][0]

    skip = np.where(df.iloc[:,0].str.contains('Number', na=False))[0][0]+add_skip
    df = (pd.read_excel(path, skiprows=skip)
            .dropna(how='all', axis=0)
            .dropna(how='all', axis=1))

    return df

def standardize_columns(cols):
    try:
        col_df = pd.read_csv("hand/column_dictionary.csv")
    except:
        print("Column dictionary not in directory.")
        col_df = pd.DataFrame()
    if not isinstance(cols, list):
        cols = cols.tolist()
    col_dict = dict(zip(col_df.ix[:,0], col_df.ix[:,1]))
    new_cols = [col_dict[col] for col in cols]
    return new_cols

def collect_metadata(df,infile, outfile, notes = 0):
    buf = io.StringIO()
    df.info(buf=buf)
    s = buf.getvalue()
    info_values = [re.split("\\s\\s+",x) for x in s.split("\n")]
    info_values = [x for x in info_values if len(x)>1]
    metadata_df = pd.DataFrame(info_values)
    metadata_df["In File"] = infile
    metadata_df["Out File"] = outfile
    metadata_df.columns = ["Column_Name","Column_Info","Original_Dataset", "Output_Dataset"]
    ## Column Info Split
    metadata_df['Non_Null_Count'], metadata_df['Object_Type'] = metadata_df['Column_Info'].str.split(' ', 1).str
    metadata_df["Object_Type"] = metadata_df["Object_Type"].str.replace("non-null ","")
    ## unique counts for each variable
    uniques_df = df.apply(lambda x: len(x.unique())).reset_index()
    uniques_df.columns = ["Column_Name","Unique_Count"]
    metadata_df["Unique_Count"] = uniques_df["Unique_Count"]
    cols = ["Original_Dataset", "Output_Dataset", "Column_Name","Non_Null_Count","Unique_Count","Object_Type"]
    if isinstance(notes, pd.Series):
        metadata_df = metadata_df.join(notes.reset_index(drop=True))
        cols.append('Notes')
    metadata_df = metadata_df[cols]
    return metadata_df
