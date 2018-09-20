from .connectors.dropbox import dropbox_handler
import civis
import os
import argparse
from shutil import copy


def init_args():
    """Init"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--path_to_execute',
                        default=os.environ.get('Dropbox_Path'))
    parser.add_argument('--data_parent_folder',
                        default='/Data/roman/Github/chicago-police-data/')
    parser.add_argument('--pdf_location',
                        default='foia/')
    parser.add_argument('--csv_or_xlsx_location',
                        default='frozen/')
    return parser.parse_args()


def create_path(data_parent_folder,
                pdf_location,
                csv_or_xlsx_location,
                path_to_execute):

    downloaded_files = os.listdir('/app'+path_to_execute)
    print(downloaded_files)
    new_path = ('_').join(path_to_execute.split('/')[-2:])
    pdf_path = data_parent_folder + pdf_location + new_path
    csv_path = data_parent_folder + csv_or_xlsx_location + new_path
    print(pdf_path)
    print(csv_path)

    for file_path in downloaded_files:
        file = file_path.split('/')[-1]
        if '.pdf' in file:
            output_path = pdf_path + '/' + file
            copy(file_path, output_path)
        elif '.csv' in file or '.xlsx' in file:
            output_path = csv_path + '/' + file
            copy(file_path, output_path)
    return pdf_path, csv_path

if __name__ == "__main__":
    ARGUMENTS = init_args()
    client = civis.APIClient()
    dropbox = dropbox_handler()

    dropbox.download_directory(ARGUMENTS.path_to_execute,
                               split_value=0)

    pdf_path, csv_path = create_path(ARGUMENTS.data_parent_folder,
                                     ARGUMENTS.pdf_location,
                                     ARGUMENTS.csv_or_xlsx_location,
                                     ARGUMENTS.path_to_execute)

    #dropbox.upload_directory(pdf_path,
    #                         ARGUMENTS.path_to_execute)

    #dropbox.upload_directory(csv_path,
    #                         ARGUMENTS.path_to_execute)
