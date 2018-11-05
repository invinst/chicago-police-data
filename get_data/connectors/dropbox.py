import dropbox
import os
import tempfile
import pandas as pd


class dropbox_handler:
    def __init__(self):
        self.auth_token = os.environ.get('DROPBOX_OAUTH_PASSWORD')
        self.dbx = dropbox.Dropbox(self.auth_token)

    def list_files(self, dbx_path):
        res = self.dbx.files_list_folder(dbx_path)
        return [entry.name for entry in res.entries]

    def download_directory(self,
                           dbx_path,
                           split_value=5):
        res = self.dbx.files_list_folder(dbx_path, recursive=True)
        filenames = [entry.path_lower for entry in res.entries]
        for filename in filenames:
            print('File to Download:')
            print(filename)
            # get name of last filepath
            name = filename.split('/')[-1]
            github_fileloc = '/'.join(filename.split('/')[split_value:])
            if '.' not in name[1:] and name != 'makefile':
                os.makedirs('/app/'+github_fileloc)
            if '.' in name[1:] or name == 'makefile':
                download_file = '/app/'+github_fileloc
                self.dbx.files_download_to_file(download_file, filename)

    def upload_directory(self,
                         local_path,
                         dbx_path,
                         local_dbx_same,
                         overwrite=True):
        # create output path
        if '/input' in dbx_path:
            dbx_output_path = '/'.join(dbx_path.split('/')[:-1]+['output/'])
        elif local_dbx_same is False:
            dbx_output_path = dbx_path
        else:
            dbx_output_path = local_path
        filenames = [filename for filename in os.listdir(local_path)
                     if filename[0] != '.']
        print('DBX Path:')
        print(dbx_output_path)
        try:
            self.dbx.files_create_folder(dbx_output_path)
        except:
            print('Output Path Exists')
        for filename in filenames:
            if os.path.isdir(dbx_output_path+filename):
                print('Folder to Create:')
                print(filename)
                self.dbx.files_create_folder(dbx_output_path+filename)
            else:
                print('File to Upload:')
                print(filename)
                with open(local_path+filename, 'rb') as f:
                    self.dbx.files_upload(f.read(),
                                          dbx_output_path+filename,
                                          mode=dropbox.files
                                          .WriteMode('overwrite', None))

    def download_file(self,
                      dbx_path,
                      name,
                      return_sheets=False,
                      sheetname=None,
                      skip=None,
                      rows=None):
        if dbx_path[-1] != '/':
            dbx_path = dbx_path+'/'
        with tempfile.NamedTemporaryFile(mode='w') as temp:
            name = name.lower()
            download_file = dbx_path + name
            print(download_file)
            self.dbx.files_download_to_file(temp.name, download_file)

            temp.flush()
            temp.seek(0)

            if return_sheets is True:
                return pd.ExcelFile(temp.name).sheet_names
            elif '.csv' in name:
                return pd.read_csv(temp.name, compression='gzip',
                                   low_memory=False)
            elif '.xls' in name:
                if sheetname is None:
                    return pd.read_excel(temp.name,
                                         skiprows=skip,
                                         nrows=rows)
                else:
                    return pd.read_excel(temp.name,
                                         sheet=sheetname,
                                         skiprows=skip,
                                         nrows=rows)
            else:
                print('''Download of file not supported.
                         File is not a .csv, .xls, or .xlsx''')
