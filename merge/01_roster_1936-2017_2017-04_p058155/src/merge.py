#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''merge script for 01_roster_1936-2017_2017-04_p058155'''

import pandas as pd
import __main__

from reference_data import ReferenceData
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
        'input_profiles_file': 'input/roster_1936-2017_2017-04_profiles.csv.gz',
        'input_remerge_file': 'input/roster_1936-2017_2017-04.csv.gz',
        'output_remerge_file': 'output/roster_1936-2017_2017-04.csv.gz',
        'intrafile_id': 'roster_1936-2017_2017-04_ID',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID',
        'starting_uid': 100001
        }

    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)


if __name__ == "__main__":
    cons, log = get_setup()

    df = pd.read_csv(cons.input_profiles_file)
    rd = ReferenceData.from_first_file(df, 
                                       id=cons.universal_id,
                                       data_id=cons.intrafile_id,
                                       starting_id=cons.starting_uid,
                                       log=log)\
                    .write_reference(cons.output_reference_file, cons.csv_opts)