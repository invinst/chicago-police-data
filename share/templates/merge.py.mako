#!usr/bin/env python3
#
# Author(s):    ${author}

'''merge script for ${merge_number}_${foia_name}'''
<%block name="import_block">
import pandas as pd
import __main__

from merge_functions import ReferenceData
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
        'input_reference_file': 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID',
        'input_profiles_file' : 'input/${foia_name}_profiles.csv.gz',
        'add_cols' : [],
        'base_OD': [
            ('star', ['star', '']),
            ('first_name', ['first_name_NS']),
            ('last_name', ['last_name_NS']),
            ('appointed_date', ['appointed_date']),
            ('birth_year', ['birth_year', 'current_age', '']),
            ('middle_initial', ['middle_initial', '']),
            ('middle_initial2', ['middle_initial2', '']),
            ('gender', ['gender', '']),
            ('race', ['race', '']),
            ('suffix_name', ['suffix_name', '']),
            ('current_unit', ['current_unit', ''])],
        'loop_merge' : {
            'custom_merges' : [],
            'base_OD_edits' : []
            },
        'input_remerge_file' : 'input/${foia_name}.csv.gz',
        'output_remerge_file' : 'output/${foia_name}.csv.gz',
        }
    </%block> \

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    <%block name="input">
    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file)
    </%block>\

    <%block name="process">
    rd = ReferenceData(ref_df, uid=cons.universal_id, log=log)${"\\"}
        .add_sup_data(sup_df, add_cols=cons.add_cols, base_OD=cons.base_OD)${"\\"}
        .loop_merge(**cons.loop_merge)${"\\"}
        .append_to_reference()${"\\"}
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)${"\\"}
        .write_reference(cons.output_reference_file, cons.csv_opts)
    </%block>\
    <%block name="output"></%block>