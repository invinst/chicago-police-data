#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''merge script for 10_complaints-witnesses_2000-2016_2016-11_p046957'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from default_merges import base_merge
from merge_data import Merge
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
        'input_reference_file': 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID',
        'data_id': 'complaints-witnesses_2000-2016_2016-11_ID',
        'input_profiles_file' : 'input/complaints-witnesses_2000-2016_2016-11_profiles.csv.gz',
        'add_cols' : [
            'F4FN', 'F4LN', 'L4LN', 'F3FN', 'L3FN', 'F1FN', 'BY_to_CA',
            {'id' : 'UID',
             'exec' : ("df.loc[df['appointed_date'].isnull(), 'appointed_date']"
                       " = df.loc[df['appointed_date'].isnull(), 'so_max_date']")},
            {'id' : 'UID',
             'exec' : "df['FN2']=df['last_name_NS']"},
            {'id' : 'UID',
             'exec' : "df['LN2']=df['first_name_NS']"},
            {'id' : 'complaints-witnesses_2000-2016_2016-11_ID',
             'exec' : "df['FN2']=df['first_name_NS']"},
            {'id' : 'complaints-witnesses_2000-2016_2016-11_ID',
             'exec' : "df['LN2']=df['last_name_NS']"}],
        'reshape_cols': ['cr_id'],
        'custom_merges' : [
                ['FN2', 'LN2', 'birth_year', 'appointed_date', 'star'],
                ['first_name_NS', 'L4LN', 'birth_year', 'appointed_date'],
                ['F3FN', 'L3FN', 'last_name_NS', 'birth_year', 'appointed_date', 'star'],
                ['first_name_NS', 'star', 'birth_year', 'appointed_date'],
                ['F4FN', 'star', 'birth_year', 'appointed_date'],
                ['F3FN', 'last_name_NS', 'birth_year', 'appointed_date'],
                ['F1FN', 'L3FN', 'birth_year', 'appointed_date', 'star'],
                ['last_name_NS', 'birth_year', 'appointed_date', 'star'],
                ['first_name_NS', 'birth_year', 'appointed_date'],
                ['F1FN', 'last_name_NS', 'birth_year', 'appointed_date'],
                ['F4LN', 'birth_year', 'appointed_date'],
                ['first_name_NS', 'last_name_NS', 'birth_year'],
                ['star', 'first_name_NS', 'appointed_date'],
                ],
        'input_remerge_file' : 'input/complaints-witnesses_2000-2016_2016-11.csv.gz',
        'output_remerge_file' : 'output/complaints-witnesses_2000-2016_2016-11.csv.gz',
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file)

    custom_merge = Merge(name='custom_merges', custom_merges=cons.custom_merges, check_duplicates=False)

    base_merge.check_duplicates = False
    custom_merge.check_duplicates = False

    base_merge.filter_reference_merges_flag = False
    custom_merge.filter_reference_merges_flag = False

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, one_to_one=False, add_cols=cons.add_cols)\
        .loop_merge(merges=[base_merge, custom_merge])\
        .append_to_reference(keep_sup_um=False)\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)
  