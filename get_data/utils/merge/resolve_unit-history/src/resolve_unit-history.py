#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''resolve_unit-history generation script'''

import pandas as pd
import numpy as np
import yaml
import __main__
pd.options.mode.chained_assignment = None

import setup
from unit_history_functions import resolve_units, TODAY

def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'resignation_file': 'input/officer-reference.csv.gz',
        'unit_history_files' : [
            'input/unit-history__2016-03.csv.gz',
            'input/unit-history__2016-12.csv.gz'
            ],
        'output_file': 'output/unit-history.csv.gz',
        'UID': 'UID',
        'START' : 'unit_start_date',
        'END' : 'unit_end_date',
        'UNIT' : 'unit',
        'RES' : 'resignation_date',
        }

    assert args['resignation_file'] == 'input/officer-reference.csv.gz',\
        'Resignation file file is not correct.'
    assert args['output_file'] == 'output/unit-history.csv.gz',\
        'Output file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()
START = cons.START
END = cons.END
UNIT = cons.UNIT
UID = cons.UID
uh_list = [pd.read_csv(f) for f in cons.unit_history_files]
uh_df = pd.DataFrame()
for df in uh_list:
    df = df.loc[:, [UID, UNIT, START, END]]
    df.dropna(subset=[UNIT, UID, START],
              how='any', inplace=True)
    df[START] = pd.to_datetime(df[START])
    df[END] = pd.to_datetime(df[END])
    # mark erroneous with negative time, fill with NA -> 1
    case1 = df[END].notnull() & (df[END] <= df[START])
    # non-erroneous with positive time but no end date -> 2
    case2 = df[END].isnull()
    # non-erroneous with positive time and end date -> 3
    case3 = df[END].notnull() & (df[END] > df[START])
    assert sum(case1 & case2 & case3) == 0
    assert sum(case1) + sum(case2) + sum(case3) == df.shape[0]
    df.loc[case1, 'value'] = 1
    df.loc[case2, 'value'] = 2
    df.loc[case3, 'value'] = 3
    uh_df = uh_df.append(df)
uh_df = uh_df.drop_duplicates()
nUIDs = uh_df[UID].nunique()

log.info("Starting resolve_units")
resolved = pd.concat(
    [resolve_units(g, START, END, UNIT)
     for k, g in uh_df[[UID, UNIT, START, END]].groupby(UID, as_index=False)])
log.info("Done resolve_units")
assert resolved[UID].nunique() == nUIDs, 'Lost UIDs during resolve_units'
resolved.to_csv(cons.output_file, **cons.csv_opts)
