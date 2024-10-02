#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''clean script for TRR-weapon-discharges_2021-2023_2023-10_p877988'''

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
        'input_file': 'input/TRR-weapon-discharges_2021-2023_2023-10_p877988.csv.gz',
        'output_file': 'output/TRR-weapon-discharges_2021-2023_2023-10_p877988.csv.gz',
        'firearm_columns': [
            'fadischrg_first_shot_cd', 'fadischrg_mem_shot_fired_cnt',
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
         
    df = clean_data(df, log)
         
    new_weapon_type_mask = df['weapon_type'].isnull() & df['weapon_type_new_trr'].notnull()

    df.loc[new_weapon_type_mask, 'weapon_type'] = df.loc[new_weapon_type_mask, 'weapon_type_new_trr']


    log.info(f"Dropping {df['weapon_type'].isnull().sum()} rows without weapon_type")
    df = df[df['weapon_type'].notnull()]
    log.info(f"{df.shape[0]} rows remaining.")

    df.to_csv(cons.output_file, **cons.csv_opts)     