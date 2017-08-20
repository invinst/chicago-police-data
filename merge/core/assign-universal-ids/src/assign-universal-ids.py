import pandas as pd
import __main__

from assign_unique_ids_functions import aggregate_data
from merge_functions import append_to_reference, listdiff, remerge
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
        'input_demo_files': [
            'input/all-sworn_demographics.csv.gz',
            'input/all-sworn-units_demographics.csv.gz',
            'input/all-members_demographics.csv.gz',
            'input/unit-history_demographics.csv.gz'
        ],
        'input_full_files': [
            'input/all-sworn.csv.gz',
            'input/all-sworn-units.csv.gz',
            'input/all-members.csv.gz',
            'input/unit-history.csv.gz'
        ],

        'output_files': [
            'output/all-sworn.csv.gz',
            'output/all-sworn-units.csv.gz',
            'output/all-members.csv.gz',
            'output/unit-history.csv.gz'
        ],
        'output_profile_file': 'output/officer-profiles.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID'
        }

    assert all(input_demo_file.startswith('input/') and
               input_demo_file.endswith('.csv.gz')
               for input_demo_file in args['input_demo_files']),\
        "An input_demo_file is malformed: {}".format(args['input_demo_files'])
    assert all(input_full_file.startswith('input/') and
               input_full_file.endswith('.csv.gz')
               for input_full_file in args['input_full_files']),\
        "An input_full_file is malformed: {}".format(args['input_full_files'])
    assert all(output_file.startswith('output/') and
               output_file.endswith('.csv.gz')
               for output_file in args['output_files']),\
        "An output_file is malformed: {}".format(args['output_files'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

ref_df = pd.DataFrame()
profile_df = pd.DataFrame()

for idf, iff, of in zip(cons.input_demo_files,
                        cons.input_full_files,
                        cons.output_files):
    sub_df = pd.read_csv(idf)
    atr_dict = append_to_reference(sub_df=sub_df,
                                   profile_df=profile_df,
                                   ref_df=ref_df,
                                   return_merge_report=True,
                                   return_merge_list=True)

    ref_df = atr_dict['ref']
    cons.write_yamlvar('File added', idf)
    cons.write_yamlvar('Officers Added',
                       len(ref_df['UID']) - profile_df.shape[0])
    if not profile_df.empty:
        cons.write_yamlvar('Merge Report', atr_dict['MR'])
        cons.write_yamlvar('Merge List',
                           atr_dict['ML'].value_counts())

    profile_df = aggregate_data(ref_df, cons.universal_id,
                                mode_cols=listdiff(ref_df.columns,
                                                   [cons.universal_id]))

    full_df = pd.read_csv(iff)
    id_col = [col for col in full_df.columns
              if col.endswith('_ID')][0]
    full_df = remerge(full_df, profile_df,
                      cons.universal_id, id_col)
    full_df.to_csv(of, **cons.csv_opts)

profile_df.to_csv(cons.output_profile_file, **cons.csv_opts)
ref_df.to_csv(cons.output_reference_file, **cons.csv_opts)
