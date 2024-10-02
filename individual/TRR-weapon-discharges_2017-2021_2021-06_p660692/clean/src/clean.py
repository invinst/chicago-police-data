#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''clean script for TRR-weapon-discharges_2017-2021_2021-06_p660692'''

import pandas as pd
import __main__

from clean_functions import clean_data
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
        'input_file': 'input/TRR-weapon-discharges_2017-2021_2021-06_p660692.csv.gz',
        'output_file': 'output/TRR-weapon-discharges_2017-2021_2021-06_p660692.csv.gz',
        'firearm_columns': [
            'fadischrg_first_shot_cd', 'total_number_of_shots',
            'fadischrg_fired_at_veh_i', 'fadischrg_make_cd', 'fadischrg_model_cd',
            'firearm_make', 'firearm_model'
            ],
        'taser_columns': [
            'taser_dartid_no', 'taser_prop_inventory_no', 'taser_probe_dischrg_cd',
            'taser_contact_stun_cd', 'taser_spark_display_cd', 'taser_arc_cycle_cd',
            'taser_trigger_cd', 'taser_arc_oth_desc'
            ]
        }
     
    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
    
    df = pd.read_csv(cons.input_file)
         
    df = df.assign(weapon_type=df['weapon_type'].str.split(', ')).explode("weapon_type")
    df = clean_data(df, log)

    # drop all null columns, noting that accidental and destruct animal columns are always non null so excluding those
    cols = df.columns[~df.columns.str.contains("_id") & ~df.columns.str.contains('accidental') & ~df.columns.str.contains('destruct_animal')]
    df = df.dropna(subset=cols, how='all', axis=0)

    # fill in some missing weapon types based on other not null columns
    null_weapon_type_mask = df['weapon_type'].isnull()
    possible_firearm_mask = df[cons.firearm_columns].notnull().any(axis=1)
    possible_taser_mask = df[cons.taser_columns].notnull().any(axis=1)

    df.loc[null_weapon_type_mask & possible_firearm_mask, 'weapon_type'] = "SEMI-AUTO PISTOL"
    df.loc[null_weapon_type_mask & possible_taser_mask, 'weapon_type'] = 'TASER'

    # drop rows with null weapon type
    log.info(f"Dropping {df['weapon_type'].isnull().sum()} rows without weapon_type")
    df = df[df['weapon_type'].notnull()]
    log.info(f"{df.shape[0]} rows remaining.")

    df.to_csv(cons.output_file, **cons.csv_opts)     