#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''script for adding flags to TRR-main_2004-2016_2016-09_p046360'''

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
        'input_main_file': 'input/TRR-main_2004-2016_2016-09.csv.gz',
        'input_wd_file': 'input/TRR-weapon-discharges_2004-2016_2016-09.csv.gz',
        'input_ar_file': 'input/TRR-actions-responses_2004-2016_2016-09.csv.gz',
        'output_main_file': 'output/TRR-main_2004-2016_2016-09.csv.gz',
        'output_hits_file' : 'output/TRR-discharge-hits_2004-2016_2016-09.csv.gz'
        }

    assert (args['input_main_file'].startswith('input/') and
            args['input_main_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_main_file'])
    assert (args['input_wd_file'].startswith('input/') and
            args['input_wd_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_wd_file'])
    assert (args['input_ar_file'].startswith('input/') and
            args['input_ar_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_ar_file'])
    assert (args['output_main_file'].startswith('output/') and
            args['output_main_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_main_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

tmain = pd.read_csv(cons.input_main_file)

log.info("Identifying taser usage using\n%s and %s"
         % (cons.input_wd_file, cons.input_ar_file))
td = pd.read_csv(cons.input_wd_file)
ta = pd.read_csv(cons.input_ar_file)

taser_trrs = ta[ta['force_type'] == 'Taser'].trr_id.tolist()
taser_trrs.append(td[
    (td['weapon_type'].notnull()) &
    (td['weapon_type'].isin(['TASER (DRIVE STUN MODE)', 'TASER (PROBE DISCHARGE)']))
    ]['trr_id'].tolist())
tmain['taser'] = tmain['trr_id'].map(lambda x: 1 if x in taser_trrs else 0)

log.info("Collecting trr_ids with guns used in %s" % cons.input_wd_file)
gun_tids = td.loc[
    (td['weapon_type'].notnull()) &
    (td['weapon_type'].isin(
        ['SEMI-AUTO PISTOL','REVOLVER', 'RIFLE','SHOTGUN']
        ))]['trr_id'].tolist()

log.info("Collecting weapon-discharge info")
td = td.loc[
    ((td['weapon_type'].isin(['SEMI-AUTO PISTOL','REVOLVER', 'RIFLE','SHOTGUN'])) &
     (td['total_number_of_shots'].notnull()) &
     (td['total_number_of_shots'] > 0)),
     ['trr_id', 'total_number_of_shots', 'weapon_type', 'object_struck_of_discharge']]

log.info("Writing %s for object struck discharges" % cons.output_hits_file)
td[['trr_id', 'weapon_type', 'object_struck_of_discharge']]\
    .dropna(subset=['object_struck_of_discharge'])\
    .to_csv(cons.output_hits_file, **cons.csv_opts)

td = td[['trr_id', 'total_number_of_shots']]\
    .dropna(how='any')\
    .groupby('trr_id', as_index=False)\
    .sum()
tmain = tmain.merge(td, on='trr_id', how='left')
tmain['total_number_of_shots'] = tmain['total_number_of_shots'].fillna(0)

log.info("Identifying which trr_ids have firearm_used based on gun trr_ids")
tmain['firearm_used'] = tmain['trr_id'].map(lambda x: 1 if x in gun_tids else 0)

log.info("Counting number of trr_ids with firearm_used by sr_no\n"
         "(equates to the number of officers using a firearm in an incident)")
officers_fired_sr = tmain[['sr_no', 'firearm_used']]\
    .dropna(how='any')\
    .groupby('sr_no', as_index=False)\
    .sum()\
    .rename(columns={'firearm_used':'number_of_officers_using_firearm'})
tmain = tmain.merge(officers_fired_sr, on='sr_no', how='left')
tmain['number_of_officers_using_firearm'] = tmain['number_of_officers_using_firearm'].fillna(0)

log.info("Writing %s with flag columns" % cons.output_main_file)
tmain.to_csv(cons.output_main_file, **cons.csv_opts)
