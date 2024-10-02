#!usr/bin/env python3
#
# Author(s):    Roman Rivera / Ashwin Sharma (Invisible Institute)

'''merge script for 06_complaints-accused_2000-2016_2016-11_p046957'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from merge_data import Merge
from default_merges import default_merges, base_merge
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
        'data_id': 'complaints-accused_2000-2016_2016-11_ID',
        'input_profiles_file' : 'input/complaints-accused_2000-2016_2016-11_profiles.csv.gz',
        'add_cols' : ['F4FN', 'F4LN'],
        'reshape_cols': ['cr_id'],
        'loop_merge' : {'verbose' : True},
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
        'input_remerge_file' : 'input/complaints-accused_2000-2016_2016-11.csv.gz',
        'output_remerge_file' : 'output/complaints-accused_2000-2016_2016-11.csv.gz'
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

    rd = ReferenceData(ref_df, cons.universal_id, add_cols=cons.add_cols, log=log)\
        .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, from_year=2016)\
        .loop_merge(merges=[base_merge])\
        .append_to_reference()\
        .remerge_to_file(cons.input_remerge_file,
                        cons.output_remerge_file,
                        cons.csv_opts)\
        .write_reference(cons.output_reference_file, cons.csv_opts)

    # TODO: split this out into separate task, probably within resolve    
    log.info('Assembling 2000 - 2016 accused')
    outcome_dict = {int(ol[0]) : ol[1] for ol in cons.outcome_lists}

    # re-read in output file, prep for later resolve
    df = pd.read_csv(cons.output_remerge_file)
    cr_ids = set(df.cr_id)
    df = df[
        ['UID', 'cr_id', 'recommended_discipline', 'final_discipline',
        'recommended_finding', 'final_finding', 'complaint_category']]\
    .drop_duplicates()\
    .rename(columns={'recommended_finding' : 'recc_finding'})

    df['complaint_code'] = df['complaint_category'].str.split('-').str[0]
    df['complaint_category'] = df['complaint_category'].str.split("-").str[1:].str.join("-")
    
    df['recc_outcome'] = df['recommended_discipline'].fillna(-1).astype(int).replace(outcome_dict)
    df['final_outcome'] = df['final_discipline'].fillna(-1).astype(int).replace(outcome_dict)
    df.drop(['final_discipline', 'recommended_discipline'], axis=1, inplace=True)

    assert keep_duplicates(df, ['cr_id', 'UID']).empty
    assert set(df.cr_id) == cr_ids, "Missing cr_ids"
    assert df[df.cr_id.isnull()].empty, "Row without cr id"

    df.cr_id = df.cr_id.astype(str)
    df.to_csv(cons.output_remerge_file, **cons.csv_opts)
