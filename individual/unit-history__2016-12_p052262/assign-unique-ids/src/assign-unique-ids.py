#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''assing-unique-ids script for unit-history__2016-12_p052262'''

import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
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
        'input_file': 'input/unit-history__2016-12.csv.gz',
        'output_file': 'output/unit-history__2016-12.csv.gz',
        'output_profiles_file': 'output/unit-history__2016-12_profiles.csv.gz',
        'id_cols': [
            "first_name", "last_name", "suffix_name",
            "first_name_NS", "last_name_NS",
            "appointed_date", "birth_year", "gender"
            ],
        'conflict_cols': ['middle_initial', 'middle_initial2'],
        'max_cols' : ['race'],
        'current_cols': ['unit'],
        'time_col': 'unit_start_date',
        'id': 'unit-history__2016-12_ID'
        }

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

df = pd.read_csv(cons.input_file)

df["Specify"] = 0
res1_units = [5, 602]
df.loc[(df["first_name"] == "ROBERT") &
       (df["last_name"] == "SMITH") &
       (df["middle_initial"] == "E") &
       (df["birth_year"] == 1947) &
       (df["appointed_date"] == "1971-02-22") &
       (df["unit"].isin(res1_units)),
       "Specify"] = 1
log.info(("Robert E Smith 1947 1971-02-22 in units {}"
          " specified as singular individual.").format(res1_units))

df = assign_unique_ids(df, cons.id,
                       cons.id_cols + ["Specify"],
                       cons.conflict_cols,
                       log=log)
del df["Specify"]
log.info(("Specify column used to manually distinguish individuals"
          " created for AUID then dropped before aggregation"))
df.to_csv(cons.output_file, **cons.csv_opts)

profiles_df = aggregate_data(df, cons.id, cons.id_cols,
                             max_cols=cons.conflict_cols + cons.max_cols,
                             current_cols=cons.current_cols,
                             time_col=cons.time_col)
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
