#!usr/bin/env python3
#
# Author(s):    Ashwin Sharma (Invisible Institute) 

'''import script for TRR-weapon-discharges_2021-2023_2023-10_p877988'''

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
        'input_file': 'input/P877988_TRR_2021-YTD.xlsx',
        'output_file': 'output/TRR-weapon-discharges_2021-2023_2023-10_p877988.csv.gz',
        'metadata_file': 'output/metadata_TRR-weapon-discharges_2021-2023_2023-10_p877988.csv.gz',
        'column_names_key': 'TRR-weapon-discharges_2021-2023_2023-10_p877988',
        'sheet_name': 'Jan1-2021-Sep15-2023 TRR',
        'keep_cols': [
            'REPORT NO', 
            'ResponseWithWeapon New TRR', 'WEAPON DISCHRG DOES NOT APPLY', 'WEAPON_DISCHRG_COUNT_NEW_TRR', 'WEAPON TYPE',
            'WEAPCONTRIBUTE_SUBINJRY', 'WEAPON_DISCHRG_SELFINJUR_MEM_I', 'WEAPON_DISCHRG_SELFINJUR_SUB_I', 'ACCIDENTAL_DISCHARGE', 
            'DISCHARGE_TO_DESTRUCT_ANIMAL', 'OBJSTRCK_DISCH_MEMWEAP_NEW_TRR', 'TASER_DARTID_NO', 'TASER_PROP_INVENTORY_NO', 'TASER_PROBE_DISCHRG_CD',
            'TASER_CONTACT_STUN_CD', 'TASER_SPARK_DISPLAY_CD', 'TASER_ARC_CYCLE_CD', 'TASER_TRIGGER_CD', 'TASER_ARC_OTH_DESC',  
            'FADISCHRG_FIRST_SHOT_CD', 'FADISCHRG_MEM_SHOT_FIRED_CNT', 'FADISCHRG_RELOADED_I', 'FADISCHRG_FIRED_AT_VEH_I', 
            'FADISCHRG_MAKE_CD', 'FIREARM_MAKE', 'FADISCHRG_MODEL_CD', 'FIREARM_MODEL']
    }
     
    assert args['input_file'].startswith('input/'),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    df = pd.read_excel(cons.input_file, sheet_name=cons.sheet_name, usecols=cons.keep_cols)

    df.columns = standardize_columns(df.columns, cons.column_names_key)

    df.insert(0, 'row_id', df.index+1)
    df.to_csv(cons.output_file, **cons.csv_opts)

    meta_df = collect_metadata(df, cons.input_file, cons.output_file)
    meta_df.to_csv(cons.metadata_file, **cons.csv_opts)

