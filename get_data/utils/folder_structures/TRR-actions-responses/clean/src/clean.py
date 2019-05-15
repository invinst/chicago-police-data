#!usr/bin/env python3
#


'''clean script for TRR-actions-responses_2004-2016_2016-09_p046360'''

from fuzzywuzzy import process
import pandas as pd
import numpy as np
import __main__
import yaml
import sys

from clean_functions import clean_data
import setup

# http://michelleful.github.io/code-blog/2015/05/20/cleaning-text-with-fuzzywuzzy/

def correct_member_action(raw_member_action, correct_member_actions):
    ''' attempts to correct member actions in the raw data by comparing raw member actions
        to the list of correct member actions

        example: if 'TASER (PROBE DISCHARGE) 1' exists in the raw data, it will
        score highly against 'TASER (PROBE DISCHARGE)' and will be cleaned to
        be 'TASER (PROBE DISCHARGE)'
    '''

    if pd.isna(raw_member_action):
        return raw_member_action
 
    if raw_member_action in correct_member_actions:
        return raw_member_action

    new_name, score = process.extractOne(raw_member_action, correct_member_actions)
    if score < 85:
        return raw_member_action
    else:
        return new_name

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

# retrieve list of correct member actions from force type file
correct_member_actions = list(ft_dict.keys())

# standardize member action column 
df[cons.member_action_col] = df[cons.member_action_col].apply(correct_member_action, correct_member_actions=correct_member_actions)

# retrieve member actions that were unable to be matched against ft_dict
diff = set(filter(lambda x: x == x , \
           set(correct_member_actions).symmetric_difference(df[cons.member_action_col].unique())))

if len(diff) > 0:
    raise Exception("The following member actions from raw data were unable to be matched: {}".format(diff))

# force type recoding
df[cons.force_type_col] = df[cons.member_action_col].replace(ft_dict)

df[cons.action_category_col] = df[cons.force_type_col].replace(fc_dict)

df[cons.action_general_category_col] = \
    np.floor(np.array(df[cons.action_category_col], dtype=np.float64))

df[cons.action_resistance_col] = df[cons.member_action_col].replace(ar_dict)

df.to_csv(cons.output_file, **cons.csv_opts)