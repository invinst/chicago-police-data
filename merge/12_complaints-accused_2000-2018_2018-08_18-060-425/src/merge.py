#!usr/bin/env python3
#
# Author(s):    Roman Rivera / Ashwin Sharma (Invisible Institute)

'''merge script for 12_complaints-accused_2000-2018_2018-03_18-060-157'''

import pandas as pd
import numpy as np
import __main__

from reference_data import ReferenceData
from default_merges import base_merge, married_merge
from merge_data import Merge
from general_utils import keep_duplicates, remove_duplicates
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
        'input_reference_file': 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'universal_id': 'UID',
        'data_id': 'complaints-accused_2000-2018_2018-07_ID',
        'input_profiles_file' : 'input/complaints-accused_2000-2018_2018-07_profiles.csv.gz',
        'add_cols' : ["F4FN", "F4LN"],
        'reshape_cols': {'index': 'cr_id'},
        'custom_merges' : [
                ['star', 'cr_id'] # registered name change: michael j boccasini to michael j brideson: https://www.newspapers.com/article/the-daily-herald-michael-joseph-bocc-nam/9637169/
        ],
       'date_cols' : ['incident_date', 'complaint_date', 'closed_date'],
        'outcome_lists' : [
            ['000', 'Violation Noted'],
            ['001', '1 Day Suspension'],
            ['002', '2 Day Suspension'],
            ['003', '3 Day Suspension'],
            ['004', '4 Day Suspension'],
            ['005', '5 Day Suspension'],
            ['006', '6 Day Suspension'],
            ['007', '7 Day Suspension'],
            ['008', '8 Day Suspension'],
            ['009', '9 Day Suspension'],
            ['010', '10 Day Suspension'],
            ['011', '11 Day Suspension'],
            ['012', '12 Day Suspension'],
            ['013', '13 Day Suspension'],
            ['014', '14 Day Suspension'],
            ['015', '15 Day Suspension'],
            ['016', '16 Day Suspension'],
            ['017', '17 Day Suspension'],
            ['018', '18 Day Suspension'],
            ['019', '19 Day Suspension'],
            ['020', '20 Day Suspension'],
            ['021', '21 Day Suspension'],
            ['022', '22 Day Suspension'],
            ['023', '23 Day Suspension'],
            ['024', '24 Day Suspension'],
            ['025', '25 Day Suspension'],
            ['026', '26 Day Suspension'],
            ['027', '27 Day Suspension'],
            ['028', '28 Day Suspension'],
            ['029', '29 Day Suspension'],
            ['030', '30 Day Suspension'],
            ['045', '45 Day Suspension'],
            ['060', '60 Day Suspension'],
            ['090', '90 Day Suspension'],
            ['100', 'Reprimand'],
            ['120', 'Suspended for 120 Days'],
            ['180', 'Suspended for 180 Days'],
            ['200', 'Suspended over 30 Days'],
            ['300', 'Administrative Termination'],
            ['400', 'Separation'],
            ['500', 'Reinstated by Police Board'],
            ['600', 'No Action Taken'],
            ['700', 'Reinstated by Court Action'],
            ['800', 'Resigned'],
            ['900', 'Penalty Not Served'],
            [-1, 'Unknown']],
        'input_remerge_file' : 'input/complaints-accused_2000-2018_2018-07.csv.gz',
        'output_remerge_file' : 'output/complaints-accused_2000-2018_2018-07.csv.gz',
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
        'Input reference file is not correct.'
    assert args['output_reference_file'] == 'output/officer-reference.csv.gz',\
        'Output reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file)

    base_merge.merge_dict['cr_id'] = ['cr_id', '']
    base_merge.merge_dict['birth_year'] = ['birth_year']
    married_merge.merge_dict['cr_id'] = ['cr_id', '']

    null_sup_birth_year_merge = Merge(name='null supplemental birth year merge', 
                                  merge_dict={**base_merge.merge_dict, 
                                              'birth_year': ['birth_year', 'current_age', '']},
                                              supplemental_preprocess="birth_year_null")

    null_ref_birth_year_merge = Merge(name='null reference birth year merge',
                                      merge_dict={**base_merge.merge_dict,
                                                  'birth_year': ['birth_year', '']},
                                                  reference_preprocess="birth_year_null")
    
    custom_merge = Merge(name='custom_merge', 
                         custom_merges=cons.custom_merges)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, null_flag_cols=['birth_year'], log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2018)\
        .loop_merge(merges=[base_merge, 
                            married_merge, 
                            null_sup_birth_year_merge, 
                            null_ref_birth_year_merge,
                            custom_merge])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)

    log.info('Assembling 2000 - 2018 accused')
    df = pd.read_csv(cons.output_remerge_file)
    cr_ids = set(df.cr_id)
    df = df.query('merge == 1')

    dropped = cr_ids - set(df.cr_id)
    log.warning(f"Dropped {len(dropped)} ids due to merge flag")

    # reset cr_ids
    cr_ids = set(df.cr_id)

    df = df[['cr_id', 'UID', 'complaint_code', 'accusation_id','final_finding_USE', 'final_outcome_USE']]\
        .rename(columns={'final_finding_USE' : 'final_finding','final_outcome_USE' : 'final_outcome'})

    df['final_finding'] = df['final_finding'].replace({'NOT SUSTAINED' : 'NS', 
        'SUSTAINED' : 'SU',
        'UNFOUNDED': 'UN', 
        'NO AFFIDAVIT' : 'NAF',
        'EXONERATED' : 'EX', 
        'ADDITIONAL INVESTIGATION REQUESTED' : np.nan})

    df['ff_val'] = df['final_finding'].replace({'SU' : 1, 'EX' : 2, 'UN' : 3, 'NS' : 4, 'NAF' : 5})

    df = pd.concat([
            df[df['UID'].notnull()]\
                .sort_values(['cr_id', 'UID', 'ff_val'])\
                .groupby(['cr_id', 'UID'], as_index=False)\
                .first(), 
            df[df['UID'].isnull()]])\
        .drop('ff_val', axis=1)

    assert keep_duplicates(df, ['cr_id', 'UID']).empty, "Should not have duplicates on cr_id and UID"
    assert set(df.cr_id) == cr_ids, "Missing cr_ids"
    assert df[df.cr_id.isnull()].empty, "Row without cr_id" 

    df.drop('accusation_id', axis=1, inplace=True)

    df.cr_id = df.cr_id.astype(int).astype(str)

    df.to_csv(cons.output_remerge_file, **cons.csv_opts)