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

def run_file(path_to_execute):
    full_path = 'dropbox'+path_to_execute
    os.chdir(full_path)
    print(os.listdir())
    os.system("make -f src/makefile")

if __name__=="__main__":
    ARGUMENTS = init_args()
    client = civis.APIClient()
    dropbox = dropbox_handler()

    ## paths where things are in dropbox
    #print(dropbox.list_files(path))
    dropbox.download_directory(ARGUMENTS.path_to_execute)
    ##run_file(ARGUMENTS.path_to_execute)
