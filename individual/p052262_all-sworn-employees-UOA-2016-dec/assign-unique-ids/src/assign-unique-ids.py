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
        'input_file': 'input/all-sworn-units.csv.gz',
        'output_file': 'output/all-sworn-units.csv.gz',
        'output_demo_file': 'output/all-sworn-units_demographics.csv.gz',
        'id_cols': [
            "First.Name", "Last.Name", "Suffix.Name",
            "First.Name_NS", "Last.Name_NS",
            "Appointed.Date", "Birth.Year", "Gender", "Race"
            ],
        'conflict_cols': ['Middle.Initial', 'Middle.Initial2'],
        'current_cols': ['Unit'],
        'time_col': 'Start.Date',
        'id': 'all-sworn-units_ID'
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
res1_units = [5.0, 602.0]
df.loc[(df["First.Name"] == "ROBERT") &
       (df["Last.Name"] == "SMITH") &
       (df["Middle.Initial"] == "E") &
       (df["Birth.Year"] == 1947) &
       (df["Appointed.Date"] == "1971-02-22") &
       (df["Unit"].isin(res1_units)),
       "Specify"] = 1
print(("Robert E Smith 1947 1971-02-22 in units {}"
       " specified as singular individual.").format(res1_units))

df, uid_report = assign_unique_ids(df, cons.id,
                                   cons.id_cols + ["Specify"],
                                   cons.conflict_cols)
del df["Specify"]
print(("Specify column used to manually distinguish individuals"
       " created for AUID then dropped before aggregation"))
cons.write_yamlvar('UID Report', uid_report)
df.to_csv(cons.output_file, **cons.csv_opts)

agg_df = aggregate_data(df, cons.id, cons.id_cols,
                        max_cols=cons.conflict_cols,
                        current_cols=cons.current_cols,
                        time_col=cons.time_col)
agg_df.to_csv(cons.output_demo_file, **cons.csv_opts)
