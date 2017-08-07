import pandas as pd
import __main__

from import_functions import read_p046957_file, collect_metadata
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
        'input_file': 'input/p046957_-_report_4_-_victim_data.xls',
        'output_file': 'output/victims.csv.gz',
        'metadata_file': 'output/metadata_victims.csv.gz',
        'column_names': ['CRID', 'Gender', 'Age', 'Race']
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

data_df = pd.DataFrame()
meta_df = pd.DataFrame()

df, report_produced_date, FOIA_request = read_p046957_file(cons.input_file)

cons.write_yamlvar("Report_Produced_Date", report_produced_date)
cons.write_yamlvar("FOIA_Request", FOIA_request)

df.insert(0, 'CRID', df['Number'].fillna(method='ffill').astype(int))
df.drop('Number', axis=1, inplace=True)
df = df.loc[df['Race Desc'] != 'end of record']
df.dropna(thresh=2, inplace=True)

df.columns = cons.column_names

data_df = (data_df
           .append(df)
           .reset_index(drop=True))
data_df.to_csv(cons.output_file, **cons.csv_opts)

meta_df = (meta_df
           .append(collect_metadata(df, cons.input_file, cons.output_file))
           .reset_index(drop=True))
meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
