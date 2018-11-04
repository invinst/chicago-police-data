from .connectors.dropbox import dropbox_handler
import civis
import os
import argparse
from shutil import copy
from .utils import sterilize


def init_args():
    """Init"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--path_to_execute',
                        default=os.environ.get('Dropbox_Path'))
    parser.add_argument('--file_type',
                        default='TRR')
    # default=os.environ.get('file_type'))
    parser.add_argument('--data_parent_folder',
                        default='/Data/roman/Github/chicago-police-data/')
    parser.add_argument('--pdf_location',
                        default='foia/')
    parser.add_argument('--csv_or_xlsx_location',
                        default='frozen/')
    parser.add_argument('--folders',
                        default='/app/chicago-police-data/\
                                get_data/utils/folder_structures/')
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
            output_path = output_path_dict['pdf'] + file
            copy(file_path, output_path)
        elif '.csv' in file or '.xlsx' in file:
            output_path = output_path_dict['csv'] + file
            copy(file_path, output_path)
            #if 'trr' in file.lower():
                #output_trr_filename, trr_files = trr_handler(path_to_execute,
                #                                             file)
                #copy(file_path, output_path_dict['csv'] + output_trr_filename)
                #output_path_dict['trr'] = (output_trr_filename)
    return output_path_dict


def append_to_folder_structure(folders, output_path_dict, file_type):
    folder_structure = [x.lower() for x in os.listdir(folders)
                        if file_type.lower() in x.lower()]
    print(folder_structure)
    for folder in folder_structure:
        frozen = output_path_dict['csv'] + file_type.lower()
        input = folders + folder + '/import/input/' + file_type.lower()
        copy(frozen, input)
    return folder_structure

if __name__ == "__main__":
    ARGUMENTS = init_args()
    client = civis.APIClient()
    dropbox = dropbox_handler()

    print('Start Directory Download')
    dropbox.download_directory(ARGUMENTS.path_to_execute,
                               split_value=0)

    output_path_dict = create_path(ARGUMENTS.data_parent_folder,
                                   ARGUMENTS.pdf_location,
                                   ARGUMENTS.csv_or_xlsx_location,
                                   ARGUMENTS.path_to_execute)
    print(os.listdir(output_path_dict['pdf']))
    print(os.listdir(output_path_dict['csv']))

    append_to_folder_structure(ARGUMENTS.folders,
                               output_path_dict,
                               ARGUMENTS.file_type)

    dropbox.upload_directory(output_path_dict['pdf'],
                             ARGUMENTS.path_to_execute)

    dropbox.upload_directory(output_path_dict['csv'],
                             ARGUMENTS.path_to_execute)
