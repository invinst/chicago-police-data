from .connectors.dropbox import dropbox_handler
import civis
import os
import argparse
from shutil import copy
from .utils import sterilize
from datetime import datetime


def init_args():
    """Init"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--path_to_execute',
                        default=os.environ.get('Dropbox_Path'))
    parser.add_argument('--file_type',
                        default='TRR')
    # default=os.environ.get('file_type'))
    parser.add_argument('--data_parent_folder',
                        default='/Data/Data_Testing_Copy/')
    parser.add_argument('--pdf_location',
                        default='foia/')
    parser.add_argument('--csv_or_xlsx_location',
                        default='frozen/')
    parser.add_argument('--folders',
                        default='/app/get_data/utils/folder_structures/')
    parser.add_argument('--individual',
                        default='/Data/Data_Testing_Copy/individual/')
    parser.add_argument('--new_name',
                        default='test_run')
    # default=os.environ.get('folder_identifier'))
    return parser.parse_args()


def trr_handler(path_to_execute,
                trr_filename):
    trr_filename = '/app'+path_to_execute.lower() + '/' + trr_filename
    output_trr_filename, trr_files = sterilize.sterilize(trr_filename)
    return output_trr_filename, trr_files


def create_path(data_parent_folder,
                pdf_location,
                csv_or_xlsx_location,
                path_to_execute):

    downloaded_files = os.listdir('/app'+path_to_execute.lower())
    new_path = ('_').join(path_to_execute.split('/')[-2:]) + '/'
    output_path_dict = {}
    output_path_dict['pdf'] = (data_parent_folder + pdf_location + new_path)
    output_path_dict['csv'] = (data_parent_folder +
                               csv_or_xlsx_location + new_path)
    os.makedirs(output_path_dict['pdf'])
    os.makedirs(output_path_dict['csv'])

    for file in downloaded_files:
        file_path = '/app'+path_to_execute.lower() + '/' + file
        if '.pdf' in file:
            output_path = '/app' + output_path_dict['pdf']
            os.makedirs(output_path)
            output_path_dict['pdf_file'] = file
            copy(file_path, output_path + file)
        elif '.csv' in file or '.xlsx' in file:
            output_path = '/app' + output_path_dict['csv']
            os.makedirs(output_path)
            output_path_dict['csv_file'] = file
            copy(file_path, output_path + file)
            print('List Output Path Files: {}'.format(
                os.listdir('/app' + output_path_dict['csv'])))
            '''if 'trr' in file.lower():
                output_trr_filename, trr_files = trr_handler(path_to_execute,
                                                             file)
                copy(file_path, output_path_dict['csv'] + output_trr_filename)
                output_path_dict['trr'] = (output_trr_filename)'''
    return output_path_dict


def append_to_folder_structure(folders,
                               output_path_dict,
                               folder_parameter_name,
                               file_type):
    folder_structure = [x.lower() for x in os.listdir(folders)
                        if file_type.lower() in x.lower()]
    print(folder_structure)
    new_folder_structure = []
    for folder in folder_structure:
        new_folder_name = '_'.join([folder,
                                    folder_parameter_name,
                                    datetime.today().strftime('%Y%m%d')])
        new_folder_structure.append(new_folder_name)
        frozen = '/app' + output_path_dict['csv'] + \
            output_path_dict['csv_file'].lower()
        input = folders + folder + '/import/input/' + \
            output_path_dict['csv_file'].lower()
        # empty folders don't exist in github
        os.makedirs(folders + folder + '/import/input/')
        os.makedirs(folders + folder + '/import/output/')
        copy(frozen, input)
        os.rename(folders+folder, folders+new_folder_name)
    return new_folder_structure

if __name__ == "__main__":
    print('Start Directory Download'
    ARGUMENTS = init_args()
    client = civis.APIClient()

    print('Start Dropbox Handler')
    dropbox = dropbox_handler()

    dropbox.download_directory(ARGUMENTS.path_to_execute,
                               split_value=0)

    output_path_dict = create_path(ARGUMENTS.data_parent_folder,
                                   ARGUMENTS.pdf_location,
                                   ARGUMENTS.csv_or_xlsx_location,
                                   ARGUMENTS.path_to_execute)
    print('------')
    print(os.listdir('/app' + output_path_dict['pdf']))
    print(os.listdir('/app' + output_path_dict['csv']))
    print('------')
    dropbox.upload_directory(output_path_dict['pdf'],
                             ARGUMENTS.data_parent_folder +
                             ARGUMENTS.pdf_location,
                             local_dbx_same=True)

    dropbox.upload_directory(output_path_dict['csv'],
                             ARGUMENTS.data_parent_folder +
                             ARGUMENTS.csv_or_xlsx_location,
                             local_dbx_same=True)

    folder_structure = append_to_folder_structure(ARGUMENTS.folders,
                                                  output_path_dict,
                                                  ARGUMENTS.new_name,
                                                  ARGUMENTS.file_type)
    print(folder_structure)
    '''for folder in folder_structure:
        local = ARGUMENTS.folders + folder + '/'
        db_location = ARGUMENTS.individual + folder + '/'
        dropbox.upload_directory(local,
                                 db_location,
                                 local_dbx_same=False)'''
