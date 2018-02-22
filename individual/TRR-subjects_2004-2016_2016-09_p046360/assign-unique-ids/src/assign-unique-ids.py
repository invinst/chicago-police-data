#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''assign-unique-ids script for TRR-subjects_2004-2016_2016-09_p046360'''

import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
from general_utils import list_diff
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
        'input_file': 'input/TRR-subjects_2004-2016_2016-09.csv.gz',
        'output_file': 'output/TRR-subjects_2004-2016_2016-09.csv.gz',
        'id_cols': [
            'subject_no', 'sr_no', 'gender', 'race',
            'birth_year', 'age'
            ],
        'conflict_cols' : ['armed', 'injured', 'alleged_injury'],
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

df_cb = df[df['subject_no'].notnull()]
df_cb = assign_unique_ids(df_cb, cons.id, ['subject_no'],
                          log=log)
agg_df_cb = aggregate_data(df_cb, cons.id, ['subject_no'],
                           max_cols=list_diff(cons.id_cols +
                                              cons.conflict_cols,
                                              ['subject_no']))
df_cb.columns = [col
                 if (col not in cons.id_cols[1:] and
                     col not in cons.conflict_cols)
                 else col + "_original"
                 for col in df_cb.columns]
df_cb = df_cb.merge(agg_df_cb, on=['subject_no', cons.id], how='outer')
assert df_cb.shape[0] == df[df['subject_no'].notnull()].shape[0]

df_no_cb = df[df['subject_no'].isnull()]
df_no_cb = assign_unique_ids(df_no_cb, cons.id, cons.id_cols,
                             conflict_cols=cons.conflict_cols,
                             log=log)
df_no_cb[cons.id] = df_no_cb[cons.id] + max(df_cb[cons.id])
agg_df_no_cb = aggregate_data(df_no_cb, cons.id, cons.id_cols,
                              max_cols=cons.conflict_cols)
df_no_cb.columns = [col
                    if col not in cons.conflict_cols
                    else col + "_original"
                    for col in df_no_cb.columns]
df_no_cb = df_no_cb.merge(agg_df_no_cb,
                          on=cons.id_cols + [cons.id], how='outer')
assert df_no_cb.shape[0] == df[df['subject_no'].isnull()].shape[0]

out_df = df_cb.append(df_no_cb)
assert out_df.shape[0] == df.shape[0]

out_df.to_csv(cons.output_file, **cons.csv_opts)
