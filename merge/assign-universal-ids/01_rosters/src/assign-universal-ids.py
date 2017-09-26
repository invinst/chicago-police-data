import pandas as pd
import __main__

from merge_functions import append_to_reference, generate_profiles,\
                            listdiff, remerge
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
        'arg_dicts': [
            {
                'input_demo_file': 'input/cpd-employees_demographics.csv.gz',
                'input_full_file': 'input/cpd-employees.csv.gz',
                'output_full_file': 'output/cpd-employees.csv.gz',
                'args': {}
            }
           # {
           #     'input_demo_file': 'input/all-members_demographics.csv.gz',
           #     'input_full_file': 'input/all-members.csv.gz',
           #     'output_full_file': 'output/all-members.csv.gz',
           #     'args': {}
           # },
           # {
           #     'input_demo_file': 'input/all-sworn_demographics.csv.gz',
           #     'input_full_file': 'input/all-sworn.csv.gz',
           #     'output_full_file': 'output/all-sworn.csv.gz',
           #     'args': {'no_match_cols': ['Last.Name_NS'],
           #              'return_merge_report': True,
           #              'print_merging': True}
           # }
        ],
        'output_profile_file': 'output/officer-profiles.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID'
        }

    assert args['output_profile_file'] == 'output/officer-profiles.csv.gz',\
        'Output profile file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.DataFrame()
profile_df = pd.DataFrame()

for arg_dict in cons.arg_dicts:
    assert (arg_dict['input_full_file'].startswith('input/') and
            arg_dict['input_demo_file'].startswith('input/') and
            arg_dict['output_full_file'].startswith('output/')),\
        'A file does not start with the proper prefix'
    assert (arg_dict['input_full_file'].endswith('.csv.gz') and
            arg_dict['input_demo_file'].endswith('.csv.gz') and
            arg_dict['output_full_file'].endswith('.csv.gz')),\
        'A file does not start with .csv.gz'

    print('File: {}'.format(arg_dict['input_full_file']))
    sub_df = pd.read_csv(arg_dict['input_demo_file'])
    atr_dict = append_to_reference(sub_df=sub_df,
                                   profile_df=profile_df,
                                   ref_df=ref_df,
                                   **arg_dict['args'])

    ref_df = atr_dict['ref']
    cons.write_yamlvar('File added', arg_dict['input_demo_file'])
    cons.write_yamlvar('Officers Added',
                       len(ref_df['UID']) - profile_df.shape[0])
    if not profile_df.empty:
        cons.write_yamlvar('Merge Report', atr_dict['MR'])
        cons.write_yamlvar('Merge List',
                           atr_dict['ML'].value_counts())

    profile_df = generate_profiles(ref_df, cons.universal_id,
                                   mode_cols=listdiff(ref_df.columns,
                                                      [cons.universal_id]))

    full_df = pd.read_csv(arg_dict['input_full_file'])
    id_col = [col for col in full_df.columns
              if col.endswith('_ID')][0]
    full_df = remerge(full_df, profile_df,
                      cons.universal_id, id_col)
    full_df.to_csv(arg_dict['output_full_file'], **cons.csv_opts)

profile_df.to_csv(cons.output_profile_file, **cons.csv_opts)
ref_df.to_csv(cons.output_reference_file, **cons.csv_opts)
