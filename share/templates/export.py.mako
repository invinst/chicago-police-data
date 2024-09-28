#!usr/bin/env python3
#
# Author(s):   ${author}

'''export script for ${foia_name}'''
<%block name="import_block">
import pandas as pd
import __main__

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
        % if profile: 
        'input_profiles_file': 'input/${foia_name}_profiles.csv.gz',
        % endif
        'output_file': 'output/${foia_name}.csv.gz',
        % if profile:
        'output_profiles_file': 'output/${foia_name}_profiles.csv.gz'
        % endif
        }
    </%block>

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
    % if profile: 
    profiles_df = pd.read_csv(cons.input_profiles_file)
    % endif 
    </%block> \
    <%block name="process"></%block> \
    <%block name="output"> 
    df.to_csv(cons.output_file, **cons.csv_opts)
    % if profile: 
    profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)\
    % endif
    </%block> 