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

if __name__=="__main__":
    ARGUMENTS = init_args()
    client = civis.APIClient()
    dropbox = dropbox_handler()

    ## paths where things are in dropbox
    #print(dropbox.list_files(path))
    local_file_path = '/'.join(['/app']+ARGUMENTS.path_to_execute.split['/'][5:])
    dropbox.upload_directory(local_file_path,
                             ARGUMENTS.path_to_execute)
