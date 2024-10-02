#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''import script for TRR-subject-weapons_2004-2016_2016-09_p046360'''

import pandas as pd
import __main__

from import_functions import standardize_columns, collect_metadata
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
        'input_file': 'input/10655-FOIA-P046360-TRRdata.xlsx',
        'output_file': 'output/TRR-subject-weapons_2004-2016_2016-09.csv.gz',
        'metadata_file': 'output/metadata_TRR-subject-weapons_2004-2016_2016-09.csv.gz',
        'sheet': 'SubjectWeapons',
        'column_names_key': 'TRR-subject-weapons_2004-2016_2016-09_p046360'
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_excel(cons.input_file, sheet_name=cons.sheet)
df.columns = standardize_columns(df.columns, cons.column_names_key)

log.info('Parsing columns with \\n due to error in original file')
er1 = df.loc[25287, 'weapon_description']
new_rows1 = er1.split('\n')
df.loc[25287, 'weapon_description'] = new_rows1[0]
df = df.append(pd.DataFrame([i.split('\t') for i in new_rows1[1:]],
                            columns=df.columns))
er2 = df.loc[47336, 'weapon_description']
new_rows2 = er2.split('\n')
new_rows2[-1] = new_rows2[-1] + df.loc[47337, 'trr_id'] + '\t\t'
df = df.drop(47337)
df.loc[47336, 'weapon_description'] = new_rows2[0]
df = df.append(pd.DataFrame([i.split('\t') for i in new_rows2[1:]],
                            columns=df.columns))

df.weapon_description = df.weapon_description.str.replace("_x000D_", "").str.strip()
df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = collect_metadata(df, cons.input_file, cons.output_file)
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
