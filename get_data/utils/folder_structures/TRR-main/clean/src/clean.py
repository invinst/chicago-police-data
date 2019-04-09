#!usr/bin/env python3
#


'''clean script for TRR-main_2004-2016_2016-09_p046360'''

import pandas as pd
import __main__
import yaml
import sys


from clean_functions import clean_data
import setup


def create_metadata_filename(filename):
    file_split = filename.split('/')
    return file_split[0] + '/metadata_' + file_split[1]



def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'input_file': sys.argv[1],
        'output_file': sys.argv[2],
        'trr_loc_file': 'hand/trr_locations.yaml',
        'trr_loc': 'location',
        'trr_loc_recode': 'location_recode'
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_csv(cons.input_file)
df = clean_data(df, log)
log.info('Creating %s column by recoding some values'
         'in %s column using %s file.',
         cons.trr_loc_recode, cons.trr_loc, cons.trr_loc_file)
with open(cons.trr_loc_file, 'r') as file:
    trr_loc_dict = yaml.load(file)
df[cons.trr_loc_recode] = df[cons.trr_loc].replace(trr_loc_dict)
df.to_csv(cons.output_file, **cons.csv_opts)
