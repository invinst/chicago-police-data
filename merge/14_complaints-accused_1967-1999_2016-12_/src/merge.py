#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''merge script for complaints-accused_1967-1999_2016-12_'''

import pandas as pd
import __main__

from merge_functions import ReferenceData
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
        'input_profiles_file' : 'input/complaints-accused_1967-1999_2016-12_profiles.csv.gz',
        'add_cols' : ["F4FN", "F4LN", "F1FN", "L3FN", "F1LN", "L3LN", "L4LN", "appointed_date"],
        'input_remerge_file' : 'input/complaints-accused_1967-1999_2016-12.csv.gz',
        'output_remerge_file' : 'output/complaints-accused_1967-1999_2016-12.csv.gz',
        'custom_merges' : [
                ["F1FN", "last_name_NS", "star", "appointed_date"],
                ["first_name_NS", "star", "appointed_date"],
                ["F4FN", "L4LN", "appointed_date", "star"],
                ["F1FN", "L3FN", "last_name_NS", "appointed_date", "star"],
                ["F1FN", "L3FN","F1LN", "L3LN", "appointed_date", "star"],
                ["F4FN", "L4LN", "appointed_date"],
                ["F1FN", "L3FN", "L4LN", "appointed_date"]
            ],
        'one_to_one' : False,
        'keep_sup_um' : False
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
    'Input reference file is not correct.'

    return setup.do_setup(script_path, args)

cons, log = get_setup()

ref_df = pd.read_csv(cons.input_reference_file)
ref_df.to_csv(cons.output_reference_file, **cons.csv_opts)
rd = ReferenceData(ref_df, uid=cons.universal_id, log=log)

sup_df = pd.read_csv(cons.input_profiles_file)
rd = (rd.add_sup_data(sup_df, add_cols=cons.add_cols)
        .loop_merge(custom_merges=cons.custom_merges,
                    one_to_one=cons.one_to_one,
                    verbose=True)
        .append_to_reference(keep_sup_um=cons.keep_sup_um)
        .remerge_to_file(cons.input_remerge_file,
                         cons.output_remerge_file,
                         cons.csv_opts))
