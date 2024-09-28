#!usr/bin/env python3
#
# Author(s):    ${author} 

'''import script for ${foia_name}'''
<%block name="import_block">
import pandas as pd
import __main__

from import_functions import standardize_columns, collect_metadata
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
        'input_file': 'input/${input_file}',
        'output_file': 'output/${foia_name}.csv.gz',
        'metadata_file': 'output/metadata_${foia_name}.csv.gz',
        'column_names_key': '${foia_name}',
        % if sheet:
        'sheet_name': '${sheet}'
        % endif
        }
    </%block> \

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    <%block name="input">
    % if ".csv" in input_file:
    df = pd.read_csv(cons.input_file, encoding='latin1')
    % else:
    df = pd.read_excel(cons.input_file, sheet_name='${sheet}')
    % endif
    </%block> \
    <%block name="process">
    df.columns = standardize_columns(df.columns, cons.column_names_key)
    df.insert(0, 'row_id', df.index + 1)
    </%block> \
    <%block name="output">
    df.to_csv(cons.output_file, **cons.csv_opts)

    meta_df = collect_metadata(df, cons.input_file, cons.output_file)
    meta_df.to_csv(cons.metadata_file, **cons.csv_opts) \
    </%block>