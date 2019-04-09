#!usr/bin/env python3
#


'''assign-unique-ids script for TRR-subjects_2004-2016_2016-09_p046360'''

import pandas as pd
import __main__
import sys


from assign_unique_ids_functions import assign_unique_ids, aggregate_data
from general_utils import list_diff, union_group
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
        'output_profiles_file': 'output/TRR-subjects_2004-2016_2016-09_profiles.csv.gz',
        'group_cols': ['sr_no', 'se_no'],
        'id_cols': ['event_id'],
        'conflict_cols': ['subject_no', 'gender', 'race', 'birth_year', 'age'],
        'max_cols': ['armed', 'injured', 'alleged_injury'],
        'id': 'subject_ID'
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
df = union_group(df, cons.id_cols[0], cons.group_cols)
log.info('%d group_ids' % df[cons.id_cols[0]].nunique())
df = assign_unique_ids(df, cons.id, cons.id_cols, cons.conflict_cols,
                       log=log, unresolved_policy='distinct')
adf = aggregate_data(df, cons.id, id_cols=cons.id_cols,
                     max_cols=cons.max_cols + cons.conflict_cols)
df.to_csv(cons.output_file, **cons.csv_opts)
adf.to_csv(cons.output_profiles_file, **cons.csv_opts)
