import os
import re
import logging


def makefile_finder(starting_path):
    filenames = [filename for filename in os.walk(starting_path)]
    makefile_paths = []
    for filename in filenames:
        if 'Makefile' in filename[2]:
            logging.info('Makefile file path: {}'.format(filename[0]))
            makefile_paths.append(filename[0]+'/Makefile')
    return makefile_paths


def update_makefiles(input_file, folder_parameter_name, makefile_paths):
    for makefile_path in makefile_paths:
        folder = makefile_path.split('/')[-4]
        output_file = ''.join([folder, '.csv'])
        if 'import/src' in makefile_path:
            makefile = makefile_reader(makefile_path)
            makefile = makefile_updater(input_file,
                                        output_file,
                                        makefile)
            makefile_deleter(makefile_path)
            makefile_writer(makefile_path, makefile)
        else:
            makefile = makefile_reader(makefile_path)
            makefile = makefile_updater(output_file,
                                        output_file,
                                        makefile)
            makefile_deleter(makefile_path)
            makefile_writer(makefile_path, makefile)


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
    filenames_to_replace += regex.findall(Makefile)
    # find all .csv only
    regex = re.compile(r'[^ \t\n]*.csv')
    filenames_to_replace += regex.findall(Makefile)
    filenames_to_replace = list(set(filenames_to_replace))
    for filename in filenames_to_replace:
        if 'input/' in filename  and 'input/' not in input_file:
            input_file = 'input/' + input_file
        if '.csv.gz' in filename and '.csv.gz' not in input_file:
            input_file += '.gz'
        if '_profiles' in filename and '_profiles' not in input_file:
            input_file = input_file.split('.')[0]+'_profiles.csv.gz'

        if 'output/' in filename and 'output/' not in output_file:
            output_file = 'output/' + output_file
        if '.csv.gz' in filename and '.csv.gz' not in output_file:
            output_file = output_file + '.gz'
        if '_profiles' in filename and '_profiles' not in output_file:
            output_file = output_file.split('.')[0]+'_profiles.csv.gz'

        if 'input/' in filename:
            Makefile = Makefile.replace(filename, input_file)
        elif 'output/' in filename:
            Makefile = Makefile.replace(filename, output_file)
        else:
            logging.info("Neither input nor output: {}".format(filename))
    # passing parameters to python job
    input_and_output = ''.join(["$< '", input_file, "' '", output_file, "'"])
    # Makefile = Makefile.replace('$<', input_and_output)
    return Makefile
