#!usr/bin/env python3
#
# Author(s): PB

'''functions to prepare scripts for logging and tracking constants'''

import logging
import sys
import os
from collections import namedtuple


def get_basic_logger(name, script_path=None):
    '''initialized a basic logger'''
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s[%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S')

    stream_out = logging.StreamHandler(sys.stdout)
    stream_out.setFormatter(formatter)
    logger.addHandler(stream_out)

    logfile = 'output/{}.log'.format(name[:-3])
    file_handler = logging.FileHandler(logfile, mode='w')
    logger.addHandler(file_handler)

    logger.info("running {}".format(name))
    return logger


def do_setup(script_path, args, cmdargs=None):
    ''' called at the end of each script's specific get_setup
        or get_args() function.
    '''
    script_name = os.path.basename(script_path)
    script_dir = os.path.dirname(script_path)
    # cmdargs = a name space created by argparse.ArgumentParser.parse_args
    assert script_dir.endswith('src') and script_name.endswith('.py')

    yaml_path = "output/{}.yaml".format(script_name)

    def write_yamlvar(var, val):
        with open(yaml_path, 'at') as yfile:
            yfile.write("{}: {}\n".format(var, val))

    # initialize yfile
    with open(yaml_path, 'wt') as yfile:
        yfile.write("# from {}\n".format(script_path))

    constants = {
        'csv_opts': {'index': False, 'compression': 'gzip'},
        'write_yamlvar': write_yamlvar,
        'yaml_path': yaml_path
    }
    constants.update(args)

    if cmdargs is not None:
        # cmdargs is a Namespace
        constants.update(vars(cmdargs))

    constants = namedtuple('Arguments', constants.keys())(**constants)
    logger = get_basic_logger(script_name, script_path)
    return constants, logger

if __name__ == '__main__':
    pass
#
# end
