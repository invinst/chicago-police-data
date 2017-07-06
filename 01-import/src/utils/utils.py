import dropbox
import os
import tempfile
import pandas as pd
import numpy as np
import io
import sys
import re

def null_dropper(df):
    buf = io.StringIO()
    df.info(buf=buf)
    s = buf.getvalue()
    info_values = [re.split("\\s\\s+",x) for x in s.split("\n")]
    info_values = [x for x in info_values if len(x)>1]
    info_values = [x[0] for x in info_values if x[1].startswith('0 non-null')]
    df = df.drop(info_values,axis=1)
    return df

def metadata_dataset(df,file):
    buf = io.StringIO()
    df.info(buf=buf)
    s = buf.getvalue()
    info_values = [re.split("\\s\\s+",x) for x in s.split("\n")]
    info_values = [x for x in info_values if len(x)>1]
    metadata_df = pd.DataFrame(info_values)
    metadata_df["File"] = file
    metadata_df.columns = ["Column_Name","Column_Info","Original_Dataset"]
    ## Column Info Split
    metadata_df['Non_Null_Count'], metadata_df['Object_Type'] = metadata_df['Column_Info'].str.split(' ', 1).str
    metadata_df["Object_Type"] = metadata_df["Object_Type"].str.replace("non-null ","")
    ## unique counts for each variable
    uniques_df = df.apply(lambda x: len(x.unique())).reset_index()
    uniques_df.columns = ["Column_Name","Unique_Count"]
    metadata_df["Unique_Count"] = uniques_df["Unique_Count"]
    metadata_df = metadata_df[["Original_Dataset","Column_Name","Non_Null_Count","Unique_Count","Object_Type"]]
    return metadata_df

def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

def city_state_zip_splitter(df):
    new_states_list=[]
    for value in df["City_State_Zip"]:
        ## check if it contains a number (zipcode)
        if hasNumbers(value):
            split_state = value.split(" ")
            ## join city names until we have 3 values
            while len(split_state)>3:
                split_state = [split_state[0]+' '+split_state[1]]+split_state[2:]
        else:
            split_state = value.split(" ")
            ## join city names until we have 2 values
            while len(split_state)>2:
                split_state = [split_state[0]+' '+split_state[1]]+split_state[2:]
        new_states_list.append(split_state)
    city_state_zip =  pd.DataFrame(new_states_list)
    city_state_zip.columns = ["City","State","Zip"]
    return city_state_zip

class dropbox_handler:
    def __init__(self):
        self.auth_token = os.environ.get('DROPBOX_OAUTH_TOKEN')
        self.dbx = dropbox.Dropbox(self.auth_token)

    def list_files(self, dbx_path):
        res = self.dbx.files_list_folder(dbx_path)
        return [entry.name for entry in res.entries]

    def download_file(self,
                      dbx_path,
                      name,
                      return_sheets=False,
                      sheetname=None,
                      skip=None,
                      rows=None):
        if dbx_path[-1]!='/':
            dbx_path = dbx_path+'/'
        with tempfile.NamedTemporaryFile(mode='w') as temp:
            name = name.lower()
            download_file = dbx_path + name
            print(download_file)
            self.dbx.files_download_to_file(temp.name,download_file)

            temp.flush()
            temp.seek(0)

            if return_sheets==True:
                return pd.ExcelFile(temp.name).sheet_names
            elif '.csv' in name:
                return pd.read_csv(temp.name,low_memory=False)
            elif '.xls' in name:
                if sheetname==None:
                    return pd.read_excel(temp.name,
                                         skiprows=skip,
                                         nrows=rows)
                else:
                    return pd.read_excel(temp.name,
                                         sheet=sheetname,
                                         skiprows=skip,
                                         nrows=rows)
            else:
                print("Download of file not supported. File is not a .csv, .xls, or .xlsx")
