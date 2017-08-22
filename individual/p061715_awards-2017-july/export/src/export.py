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
        'input_file': 'input/awards.csv.gz',
        'input_demo_file': 'input/awards_demographics.csv.gz',
        'output_file': 'output/awards.csv.gz',
        'output_demo_file': 'output/awards_demographics.csv.gz',
        'export_cols': [
            'Award.ID', 'Award.Type', 'Incident.Start.Date',
            'Current.Award.Status', 'Award.Request.Date',
            'Incident.End.Date', 'Rank', 'Last.Promotion.Date',
            'Requester.Full.Name', 'Ceremony.Date', 'Tracking.NO'
            ],
        'id': 'awards_ID'
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
keeplist = ['PO', 'P O', 'P.O', 'SERGEANT']
droplist = ['LEGAL', 'AIDE', 'CLERK', 'POL ', 'DIR',
            'SUPV', 'SUPT', 'MANAGER', 'TECH']
keep_ids = df[df['Rank'].str.startswith('9', 'UNK')][
              ((df['Rank'] == 'CHIEF') | (df['Rank'] == 'DEP CHIEF')) |
              ((df['Rank'].str.contains('|'.join(keeplist))) &
               (~df['Rank'].str.contains('|'.join(droplist))))][
              cons.id].unique()
drop_ids = list(set(df[cons.id]) - set(keep_ids))
df = df[[cons.id] + cons.export_cols]
df.to_csv(cons.output_file, **cons.csv_opts)

demo_df = pd.read_csv(cons.input_demo_file)
demo_df = demo_df[~demo_df[cons.id].isin(drop_ids)]
print('{} IDs dropped from demographic data'.format(len(drop_ids)))
demo_df.to_csv(cons.output_demo_file, **cons.csv_opts)
