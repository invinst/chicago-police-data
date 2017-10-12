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
        'input_files': [
            'input/p046957_-_report_2.1_-_identified_accused.xls',
            'input/p046957_-_report_2.2_-_identified_accused.xls',
            'input/p046957_-_report_2.3_-_identified_accused.xls',
            'input/p046957_-_report_2.4_-_identified_accused.xls',
            'input/p046957_-_report_2.5_-_identified_accused.xls'
            ],
        'output_file': 'output/accused.csv.gz',
        'metadata_file': 'output/metadata_accused.csv.gz',
        'column_names': [
            'CR_ID', 'full_name', 'birth_year', 'gender', 'race',
            'appointed_date', 'current_unit', 'current_rank',
            'current_star', 'complaint_category',
            'recommended_finding', 'recommended_discipline',
            'final_finding', 'final_discipline'
            ]
        }

    assert all(input_file.startswith('input/')
               for input_file in args['input_files']),\
        "An input_file is malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)


cons, log = get_setup()

data_df = pd.DataFrame()
meta_df = pd.DataFrame()

for input_file in cons.input_files:
    df, report_produced_date, FOIA_request = \
                                read_p046957_file(input_file,
                                                  original_crid_col='Number:')
    log.info(('Processing {0} file, of FOIA number {1}, produced on {2}'
              '').format(input_file, FOIA_request, report_produced_date))
    cons.write_yamlvar("{}-Report_Produced_Date".format(input_file),
                       report_produced_date)
    cons.write_yamlvar("{}-FOIA_Request".format(input_file),
                       FOIA_request)
    df.columns = cons.column_names
    log.info(('final_finding and recommended_finding'
              'column values of NA changed to NAF'))
    df.loc[df['final_finding'] == 'NA', 'final_finding'] = 'NAF'
    df.loc[df['recommended_finding'] == 'NA', 'recommended_finding'] = 'NAF'
    data_df = (data_df
               .append(df)
               .reset_index(drop=True))
    meta_df = (meta_df
               .append(collect_metadata(df, input_file, cons.output_file))
               .reset_index(drop=True))


data_df.to_csv(cons.output_file, **cons.csv_opts)

meta_df.to_csv(cons.metadata_file, **cons.csv_opts)
