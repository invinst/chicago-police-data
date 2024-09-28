#!usr/bin/env python3
#
# Author(s):    ${author}

'''assign-unique-ids script for ${foia_name}'''
<%block name="import_block">
import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
import setup
</%block>\

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
        'output_file': 'output/${foia_name}.csv.gz',
        'output_profiles_file': 'output/${foia_name}_profiles.csv.gz',
        'id_cols': [${', '.join([f"'{id_col}'" for id_col in id_cols])}],
        'incident_cols' : [${', '.join([f"'{id_col}'" for id_col in incident_cols])}],
        'id': '${foia_name}_ID',
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
    </%block>\
    <%block name="process">
    df = assign_unique_ids(df, cons.id, cons.id_cols,
                        log=log)
    profiles_df = aggregate_data(df, cons.id, 
                                cons.id_cols, 
                                max_cols=cons.incident_cols)
    </%block> \
    <%block name="output">
    df.to_csv(cons.output_file, **cons.csv_opts)
    profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts) \
    </%block>