#!usr/bin/env python3
#
# Author(s):    Roman Rivera / Ashwin Sharma (Invisible Institute)

'''assign-unique-ids script for complaints-accused_1967-1999_2016-12_'''

import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
from general_utils import union_group
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
        'input_file': 'input/complaints-accused_1967-1999_2016-12.csv.gz',
        'output_file': 'output/complaints-accused_1967-1999_2016-12.csv.gz',
        'output_profiles_file': 'output/complaints-accused_1967-1999_2016-12_profiles.csv.gz',
        'ind_auid' : {'id_cols': [
                         'first_name_NS', 'last_name_NS', 'appointed_date'
                        ],
                    'conflict_cols': [
                        'middle_initial', 'middle_initial2', 'suffix_name'
                        ]},
        'ind_uid' : 'I_UID',
        'cr_auid' : {'id_cols' : [
                        'star', 'first_name', 'first_name_NS',
                        'last_name', 'last_name_NS', 'middle_initial',
                        'middle_initial2', 'suffix_name', 'cr_id']},
        'cr_uid' : 'CR_UID',
        'list_cols': ["cr_id"],
        'id': 'complaints-accused_1967-1999_2016-12_ID',
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

df = assign_unique_ids(df, cons.cr_uid, **cons.cr_auid, log=log)
df = assign_unique_ids(df, cons.ind_uid, **cons.ind_auid, log=log)
udf = union_group(df[[cons.cr_uid, cons.ind_uid]].drop_duplicates(),
                  cons.id, [cons.cr_uid, cons.ind_uid])
df = df.merge(udf, on=[cons.cr_uid, cons.ind_uid])\
    .drop(cons.cr_uid, axis=1)
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = aggregate_data(df, cons.id, cons.ind_auid['id_cols'],
                             max_cols=cons.ind_auid['conflict_cols'] + ['star'],
                             current_cols=['unit'],
                             time_col='cr_id',
                             list_cols=cons.list_cols)
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
