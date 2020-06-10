#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''merge script for 08_TRR-officers_2004-2016_2016-09_p046360'''

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
        'input_profiles_file' : 'input/TRR-officers_2004-2016_2016-09_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN'],
        'loop_merge' : {'verbose' : True},
        'input_remerge_file' : 'input/TRR-officers_2004-2016_2016-09.csv.gz',
        'output_remerge_file' : 'output/TRR-officers_2004-2016_2016-09.csv.gz'
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.read_csv(cons.input_reference_file)
sup_df = pd.read_csv(cons.input_profiles_file)

ReferenceData(ref_df, uid=cons.universal_id, log=log)\
    .add_sup_data(sup_df, add_cols=cons.add_cols)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference()\
    .remerge_to_file(cons.input_remerge_file,
                    cons.output_remerge_file,
                    cons.csv_opts)\
    .write_reference(cons.output_reference_file, cons.csv_opts)
