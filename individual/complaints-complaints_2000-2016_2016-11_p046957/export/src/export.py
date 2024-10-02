#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''export script for complaints-complaints_2000-2016_2016-11_p046957'''

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
        'input_file': 'input/complaints-complaints_2000-2016_2016-11.csv.gz',
        'output_file': 'output/complaints-complaints_2000-2016_2016-11.csv.gz',
        'keep_cols': ['cr_id', 'address', 'beat', 'location_code', 'street_number',
       'apartment_number', 'incident_date', 'incident_time',
       'complaint_date', 'closed_date', 'state', 'zip', 'city',
       'street_direction', 'street_name']
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
