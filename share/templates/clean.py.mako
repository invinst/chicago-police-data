#!usr/bin/env python3
#
# Author(s):    ${author} 

'''clean script for ${foia_name}'''
<%block name="import_block">
import pandas as pd
import __main__

from clean_functions import clean_data
import setup
</%block> \

def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    <%block name="args">
    args = {
        'input_file': 'input/${foia_name}.csv.gz',
        'output_file': 'output/${foia_name}.csv.gz'
        }
    </%block> \

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    <%block name="input">
    df = pd.read_csv(cons.input_file)
    </%block> \
    <%block name="process">
    df = clean_data(df, log)
    </%block> \
    <%block name="output">
    df.to_csv(cons.output_file, **cons.csv_opts) \
    </%block>