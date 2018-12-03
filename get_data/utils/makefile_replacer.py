import os
import re
import logging


def makefile_finder(starting_path):
    filenames = [filename for filename in os.walk(starting_path)]
    Makefile_paths = []
    for filename in filenames:
        if 'Makefile' in filename[2]:
            Makefile_paths.append(filename[0]+'/Makefile')
    return Makefile_paths


def makefile_reader(path):
    with open(path, 'r') as makefile_path:
        Makefile = makefile_path.read()
    return Makefile


def makefile_deleter(path):
    os.remove(path)
    logging.info("File Removed: {}".format(path))


def makefile_writer(path, Makefile):
    with open(path, 'w') as makefile_path:
        makefile_path.write(Makefile)


def makefile_updater(input_file, output_file, Makefile):
    # find all .csv.gz files in the makefile
    regex = re.compile(r'[^ \t\n]*.csv.gz')
    filenames_to_replace = regex.findall(Makefile)
    # find .xslx if there are any
    regex = re.compile(r'[^ \t\n]*.xlsx')
    filenames_to_replace = filenames_to_replace + regex.findall(Makefile)
    for filename in filenames_to_replace:
        if 'input/' in filename:
            Makefile = Makefile.replace(filename, input_file)
        elif 'output/' in filename:
            Makefile = Makefile.replace(filename, output_file)
        else:
            logging.info("Neither input nor output: {}".format(filename))
    return Makefile
