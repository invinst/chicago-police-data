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
        'input_file': 'input/investigators.csv.gz',
        'input_demo_file': 'input/investigators_demographics.csv.gz',
        'output_file': 'output/investigators.csv.gz',
        'output_demo_file': 'output/investigators_demographics.csv.gz',
        'export_cols': [
            'CRID', 'Current.Rank', 'Current.Unit'
            ],
        'id': 'investigators_ID',
        'keep_ranks': [
            'LIEUTENANT OF POLICE', 'SERGEANT OF POLICE',
            'CAPTAIN OF POLICE', 'POLICE OFFICER',
            'POLICE AGENT', 'PO AS DETECTIVE',
            'PO LEGAL OFF 2', 'PO/MOUNTED PAT OFF.',
            'PO/FIELD TRNING OFF', 'PO ASGN EVID. TECHNI',
            'PO/MARINE OFFICER', 'PO ASSG CANINE HANDL',
            'CHIEF', 'DEP CHIEF', 'COMMANDER'
            ]
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
df = df[[cons.id] + cons.export_cols]
drop_ids = df[~df['Current.Rank'].isin(cons.keep_ranks)][cons.id].unique()
df.to_csv(cons.output_file, **cons.csv_opts)

demo_df = pd.read_csv(cons.input_demo_file)
demo_df = demo_df[~demo_df[cons.id].isin(drop_ids)]
print('{} rows dropped from demo_df.'.format(len(drop_ids)))
demo_df.to_csv(cons.output_demo_file, **cons.csv_opts)
