#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''clean script for TRR-main_2017-2020_2020-06_p583646'''

import pandas as pd
import __main__
import yaml

from clean_functions import clean_data
import setup


def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'input_file': 'input/TRR-main_2017-2020_2020-06_p583646.csv.gz',
        'output_file': 'output/TRR-main_2017-2020_2020-06_p583646.csv.gz',
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
df[['trr_date', 'trr_time']] = df.trr_time.str.split(" ", expand=True)
df = clean_data(df, log)
log.info('Creating %s column by recoding some values'
         ' in %s column using %s file.',
         cons.trr_loc_recode, cons.trr_loc, cons.trr_loc_file)
with open(cons.trr_loc_file, 'r') as file:
    trr_loc_dict = yaml.safe_load(file)
df[cons.trr_loc_recode] = df[cons.trr_loc].replace(trr_loc_dict)


df.to_csv(cons.output_file, **cons.csv_opts)
