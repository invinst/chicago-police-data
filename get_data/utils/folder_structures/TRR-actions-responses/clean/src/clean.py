#!usr/bin/env python3
#


'''clean script for TRR-actions-responses_2004-2016_2016-09_p046360'''

import pandas as pd
import numpy as np
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
        'member_action_other_file' : 'hand/trr_member_action_other.yaml',
        'force_type_file' : 'hand/trr_member_action_force_type.yaml',
        'force_category_file' : 'hand/trr_member_action_category.yaml',
        'action_resistance_file' : 'hand/trr_member_action_resistance.yaml',
        'member_action_col' : 'member_action',
        'force_type_col' : 'force_type',
        'action_category_col' : 'action_sub_category',
        'action_general_category_col': 'action_category',
        'action_resistance_col' : 'resistance_level'
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

log.info("Generating %s, %s, %s, %s, and %s columns for member action observations",
         cons.member_action_col, cons.force_type_col,
         cons.action_category_col, cons.action_general_category_col,
         cons.action_resistance_col)
with open(cons.member_action_other_file, 'r') as file:
    mao_dict = yaml.load(file)
with open(cons.force_type_file, 'r') as file:
    ft_dict = yaml.load(file)
with open(cons.force_category_file, 'r') as file:
    fc_dict = yaml.load(file)
with open(cons.action_resistance_file, 'r') as file:
    ar_dict = yaml.load(file)

mao_df = pd.DataFrame.from_dict(mao_dict, orient='index')\
    .reset_index(drop=False)\
    .rename(columns={'index': 'other_description',
                     0: cons.member_action_col})
mao_df.insert(0, 'person', 'Member Action')
df = df.merge(mao_df, on=['person', 'other_description'], how='left')

subset = (df[cons.member_action_col].isnull()) & (df['person'] == 'Member Action')
df.loc[subset, cons.member_action_col] = df.loc[subset, 'action']

df[cons.force_type_col] = df[cons.member_action_col].replace(ft_dict)

df[cons.action_category_col] = df[cons.force_type_col].replace(fc_dict)
df[cons.action_general_category_col] = np.floor(df[cons.action_category_col])

df[cons.action_resistance_col] = df[cons.member_action_col].replace(ar_dict)

df.to_csv(cons.output_file, **cons.csv_opts)
