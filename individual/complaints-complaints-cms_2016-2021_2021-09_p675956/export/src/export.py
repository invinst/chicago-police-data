#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute)

'''export script for complaints-complaints-cms_2016-2021_2021-08_p675956'''

import pandas as pd
import __main__

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
        'input_file': 'input/complaints-complaints-cms_2016-2021_2021-09_p675956.csv.gz',
        'output_file': 'output/complaints-complaints-cms_2016-2021_2021-09_p675956.csv.gz',
        'keep_cols': ['cr_id', 'address', 'street_number','street_direction', 'street_name',
            'city','state','zip','beat','location_code',
            'incident_date','complaint_date','closed_date', 'investigating_agency'],
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

cons, log = get_setup()

df = pd.read_csv(cons.input_file)[cons.keep_cols]
df.to_csv(cons.output_file, **cons.csv_opts)