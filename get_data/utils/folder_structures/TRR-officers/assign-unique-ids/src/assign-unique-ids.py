#!usr/bin/env python3
#


'''assign-unique-ids script for TRR-officers_2004-2016_2016-09_p046360'''

import pandas as pd
import __main__
import sys


from assign_unique_ids_functions import assign_unique_ids, aggregate_data
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
        'output_profiles_file': 'output/TRR-officers_2004-2016_2016-09_profiles.csv.gz',
        'id_cols': [
            "first_name", "last_name", "first_name_NS", "last_name_NS",
            "middle_initial", 'middle_initial2', "suffix_name",
            "appointed_date", "gender", "race", "current_star"
            ],
        'id': 'TRR-officers_2004-2016_2016-09_ID'
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

df = assign_unique_ids(df, cons.id, cons.id_cols,
                       log=log)
df.to_csv(cons.output_file, **cons.csv_opts)

agg_df = aggregate_data(df, cons.id, cons.id_cols)
agg_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
