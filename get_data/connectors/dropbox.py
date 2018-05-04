import dropbox
import os
import tempfile
import pandas as pd
import numpy as np
import io
import sys
import re

class dropbox_handler:
    def __init__(self):
        self.auth_token = os.environ.get('DROPBOX_OAUTH_PASSWORD')
        self.dbx = dropbox.Dropbox(self.auth_token)

    def list_files(self, dbx_path):
        res = self.dbx.files_list_folder(dbx_path)
        return [entry.name for entry in res.entries]

    def download_directory(self,dbx_path):
        res = self.dbx.files_list_folder(dbx_path,recursive=True)
        filenames = [entry.path_lower for entry in res.entries]
        for filename in filenames:
            ## get name of last filepath
            name = filename.split('/')[-1]
            github_fileloc='/'.join(filename.split('/')[5:])
            if '.' not in name[1:] and name!='makefile':
                os.makedirs('/app/'+github_fileloc)
            if '.' in name[1:] or name=='makefile':
                download_file = '/app/'+github_fileloc
                self.dbx.files_download_to_file(download_file,filename)

    def upload_directory(self,
                         local_path,
                         dbx_path,
                         overwrite=True):
        ## create output path
        if '/input' in dbx_path:
            dbx_output_path = '/'.join(dbx_path.split('/')[:-1]+['output2/'])
        filenames = [filename for filename in os.listdir(local_path) if filename[0]!='.']
        print('Files to Upload:')
        print(filenames)
        print('DBX Path:')
        print(dbx_output_path)
        try:
            self.dbx.files_create_folder(dbx_output_path)
        except:
            print('Output Path Exists')
        for filename in filenames:
            with open(local_path+filename,'rb') as f:
                self.dbx.files_upload(f.read(),
                                      dbx_output_path+filename,
                                      mode=dbx.files.WriteMode('overwrite', None))

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
                return pd.read_csv(temp.name,compression='gzip',low_memory=False)
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
