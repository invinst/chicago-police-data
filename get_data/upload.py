from .connectors.dropbox import dropbox_handler
import civis
import pandas as pd
import numpy as np
import os
import argparse

def init_args():
    """Init"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--path_to_execute',
                        default=os.environ.get('Dropbox_Path'))
    return parser.parse_args()

def create_local_file_path(dropbox_path_to_execute):
    ## removes '/Data/roman/Github/chicago-police-data' from the location
    file_path_list = ['app']+dropbox.path_to_execute.split('/')[5:-1]+['output/']
    return '/'.join(file_path_list)

if __name__=="__main__":
    ARGUMENTS = init_args()
    client = civis.APIClient()
    dropbox = dropbox_handler()

    local_file_path = create_local_file_path(ARGUMENTS.path_to_execute)
    print(local_file_path)
    dropbox.upload_directory(local_file_path,
                             ARGUMENTS.path_to_execute)
