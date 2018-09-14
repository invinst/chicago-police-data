from .connectors.dropbox import dropbox_handler
import civis
import pandas as pd
import numpy as np
import os
import argparse
import logging
import sys

LOG = logging.getLogger()


def init_args():
    """Init"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--path_to_execute',
                        default=os.environ.get('Dropbox_Path'))
    return parser.parse_args()


def create_local_file_path(dropbox_path_to_execute):
    # removes '/Data/roman/Github/chicago-police-data' from the location
    file_path_list = ['/app'] + dropbox_path_to_execute.split('/')[5:-1] \
        + ['output/']
    return '/'.join(file_path_list)


if __name__ == "__main__":

    LOGGING_PARAMS = {
        'stream': sys.stdout,
        'level': logging.INFO,
        'format': '%(message)s'
    }

    logging.basicConfig(**LOGGING_PARAMS)

    ARGUMENTS = init_args()
    client = civis.APIClient()
    dropbox = dropbox_handler()

    local_file_path = create_local_file_path(ARGUMENTS.path_to_execute)
    LOG.info(local_file_path)

    dropbox.upload_directory(local_file_path,
                             ARGUMENTS.path_to_execute)
