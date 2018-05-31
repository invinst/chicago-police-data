#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''resolve_complaints-supplementary generation script'''

import pandas as pd
import numpy as np
from general_utils import keep_duplicates, remove_duplicates

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
    'cw2_file': 'input/complaints-witnesses_2000-2016_2016-11.csv.gz',
    'cw3_file': 'input/complaints-CPD-witnesses_2000-2018_2018-03.csv.gz',
    'cc2_file': 'input/complaints-complainants_2000-2016_2016-11.csv.gz',
    'cc3_file': 'input/complaints-complainants_2000-2018_2018-03.csv.gz',
    'ci2_file': 'input/complaints-investigators_2000-2016_2016-11.csv.gz',
    'ci3_file': 'input/complaints-investigators_2000-2018_2018-03.csv.gz',
    'cv2_file': 'input/complaints-victims_2000-2016_2016-11.csv.gz',
    'cv3_file': 'input/complaints-victims_2000-2018_2018-03.csv.gz',

    'cw_file' : 'output/complaints-CPD-witnesses.csv.gz',
    'cc_file' : 'output/complaints-complainants.csv.gz',
    'cv_file' : 'output/complaints-victims.csv.gz',
    'ci_file' : 'output/complaints-investigators.csv.gz'

        }

    return setup.do_setup(script_path, args)


cons, log = get_setup()


log.info('Assembling Witness, taking more recent file information')
cw2 = pd.read_csv(cons.cw2_file)[['UID', 'cr_id']]
cw2.insert(0, 'cv',2)

cw3 = pd.read_csv(cons.cw3_file)[['UID', 'cr_id']]
cw3.insert(0, 'cv',3)

cw2[~cw2.cr_id.isin(cw3.cr_id)]\
    .append(cw3)\
    .to_csv(cons.cw_file, **cons.csv_opts)


log.info('Assembling Complainant, taking more recent file information')
cc2 = pd.read_csv(cons.cc2_file)[['cr_id', 'gender', 'age', 'race']]
cc2.insert(0, 'cv',2)

cc3 = pd.read_csv(cons.cc3_file)[['cr_id', 'race', 'gender','birth_year']]
cc3.insert(0, 'cv',3)

cc2[~cc2.cr_id.isin(cc3.cr_id)]\
    .append(cc3)\
    .to_csv(cons.cc_file, **cons.csv_opts)


log.info('Assembling Victim, taking more recent file information')
cv2 = pd.read_csv(cons.cv2_file)[['cr_id', 'gender', 'age', 'race']]
cv2.insert(0, 'cv',2)

cv3 = pd.read_csv(cons.cv3_file)[['cr_id', 'race', 'gender','birth_year']]
cv3.insert(0, 'cv',3)

cv2[~cv2.cr_id.isin(cv3.cr_id)]\
    .append(cv3)\
    .to_csv(cons.cv_file, **cons.csv_opts)


log.info('Assembling Investigator, taking more recent file information')
ci2 = pd.read_csv(cons.ci2_file)[
    ['complaints-investigators_2000-2016_2016-11_ID', 'cr_id',
     'first_name', 'last_name', 'middle_initial', 'suffix_name',
      'appointed_date', 'current_star', 'current_rank', 'current_unit',
       'UID']]
ci2.insert(0, 'cv',2)

ci3 = pd.read_csv(cons.ci3_file)[
    ['complaints-investigators_2000-2018_2018-03_ID',
     'cr_id', 'star', 'gender', 'race', 'birth_year',
       'appointed_date', 'current_unit', 'investigator_type',
       'assigned_datetime', 'first_name', 'first_name_NS', 'last_name',
       'last_name_NS', 'middle_initial', 'suffix_name',
       ]
]
ci3.insert(0, 'cv',3)

ci = ci2[~ci2.cr_id.isin(ci3.cr_id)]\
    .append(ci3)
ci['investigator_ID'] = ci[
    ['complaints-investigators_2000-2018_2018-03_ID',
     'complaints-investigators_2000-2016_2016-11_ID',
     'cv', 'UID']
].fillna(0).apply(lambda x:
        x[2] * 100000 + x[0] + x[1] if x[3]==0
        else x[3],
    axis=1)

ci.to_csv(cons.ci_file, **cons.csv_opts)

log.info('Done.')
