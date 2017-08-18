import pandas as pd
import __main__

from assign_unique_ids_functions import aggregate_data
from merge_functions import append_to_reference, listdiff
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
        'input_files': [
            'input/all-sworn_demographics.csv.gz',
            'input/all-sworn-units_demographics.csv.gz',
            'input/all-members_demographics.csv.gz',
            'input/unit-history_demographics.csv.gz'
        ],
        'output_file': 'output/officer-profiles.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID'
        }

    assert all(input_file.startswith('input/') and
               input_file.endswith('.csv.gz')
               for input_file in args['input_files']),\
        "An input_file is malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.DataFrame()
profile_df = pd.DataFrame()

for input_file in cons.input_files:
    sub_df = pd.read_csv(input_file)
    ref_df = append_to_reference(sub_df=sub_df,
                                 profile_df=profile_df,
                                 ref_df=ref_df)
    profile_df = aggregate_data(ref_df, cons.universal_id,
                                mode_cols=listdiff(ref_df.columns,
                                                   [cons.universal_id]))

profile_df.to_csv(cons.output_file, **cons.csv_opts)
ref_df.to_csv(cons.output_reference_file, **cons.csv_opts)
