#!usr/bin/env python3
#
# Author(s):    Roman Rivera / Ashwin Sharma (Invisible Institute)

'''merge script for complaints-accused_1967-1999_2016-12_'''

import pandas as pd
import __main__

from reference_data import ReferenceData
from merge_data import Merge
from default_merges import (
    base_merge, 
    null_appointed_date_reference_merge, 
    null_appointed_date_supplemental_merge,
    multirow_merge
)
from general_utils import  keep_duplicates, remove_duplicates
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
        'data_id': 'complaints-accused_1967-1999_2016-12_ID',
        'input_profiles_file' : 'input/complaints-accused_1967-1999_2016-12_profiles.csv.gz',
        'add_cols' : ["F4FN", "F4LN", "F1FN", "L3FN", "F1LN", "L3LN", "L4LN", "appointed_date"],
        'input_remerge_file' : 'input/complaints-accused_1967-1999_2016-12.csv.gz',
        'output_remerge_file' : 'output/complaints-accused_1967-1999_2016-12.csv.gz',
        'custom_merges' : [
                ['star', 'last_name_NS', 'appointed_date'], # all checked, first name mispellings/nicknames
                ['star', 'first_name_NS', 'last_name_NS'], # all checked, a number had wrong appointed dates 
                # ['star', 'first_name_NS', 'appointed_date'] 
            ],
        'base_OD': [
            ('star', ['star', '']),
            ('cr_id', ['cr_id', '']),
            ('first_name', ['first_name_NS', 'F4FN']),
            ('last_name', ['last_name_NS', 'F4LN']),
            ('appointed_date', ['appointed_date']),
            ('birth_year', ['birth_year', 'current_age', '']),
            ('middle_initial', ['middle_initial', '']),
            ('middle_initial2', ['middle_initial2', '']),
            ('gender', ['gender', '']),
            ('race', ['race', '']),
            ('suffix_name', ['suffix_name', '']),
            ('current_unit', ['current_unit', ''])],
        'reshape_cols': {'index': 'cr_id'},
        'one_to_one' : False,
        'keep_sup_um' : False
        }

    assert args['input_reference_file'] == 'input/officer-reference.csv.gz',\
    'Input reference file is not correct.'

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()

    ref_df = pd.read_csv(cons.input_reference_file)
    sup_df = pd.read_csv(cons.input_profiles_file)

    base_merge.filter_reference_merges_flag = False
    base_merge.check_duplicates = False

    # base_merge.merge_dict['first_name'] = ['first_name_NS']
    base_merge.merge_dict['last_name'].append("L4LN")

    custom_merge = Merge(custom_merges=cons.custom_merges, filter_reference_merges_flag=False, check_duplicates=False)

    rd = ReferenceData(ref_df, id=cons.universal_id, add_cols=cons.add_cols, 
                       null_flag_cols=['appointed_date'], log=log) \
            .add_sup_data(sup_df, data_id=cons.data_id, add_cols=cons.add_cols, one_to_one=False) \
            .loop_merge(merges=[base_merge, null_appointed_date_reference_merge, multirow_merge, custom_merge]) \
            .append_to_reference(keep_sup_um=False) \
            .remerge_to_file(cons.input_remerge_file,
                            cons.output_remerge_file,
                            cons.csv_opts) \
            .write_reference(cons.output_reference_file,  cons.csv_opts)
    
# some basic prep for resolve: reread output file, could probably be refactored
log.info("Preparing accused file for resolve.")
df = pd.read_csv(cons.output_remerge_file)
cr_ids = set(df.cr_id)

df = df.loc[df['UID'].notnull(), ['cr_id', 'UID', 'complaint_category', 'complaint_description', 'final_finding', 'final_discipline_desc']]\
    .rename(columns={
            'complaint_category' : 'complaint_code',
            'complaint_description': 'complaint_category',
            'final_discipline_desc' : 'final_outcome'}
           )\
    .drop_duplicates()

# accusations without UID
accused_drop = cr_ids - set(df.cr_id)
log.warning(f"Dropped {len(accused_drop)} cr_ids for not having UIDs associated")

cr_ids = set(df.cr_id)

# replace final finding with values temporarily to sort/prioritize on grouping
df['ff_val'] = df['final_finding'].replace({'SU' : 1, 'EX' : 2, 'UN' : 3, 'NS' : 4, 'NAF' : 5})
df = pd.concat([
        keep_duplicates(df, ['cr_id', 'UID'])\
            .sort_values(['cr_id', 'UID', 'ff_val'])\
            .groupby(['cr_id', 'UID'], as_index=False)\
            .first(),
        remove_duplicates(df, ['cr_id', 'UID'])])\
    .drop('ff_val', axis=1)

assert keep_duplicates(df, ['cr_id', 'UID']).empty, "Duplicate cr_id, UID found"
assert set(df.cr_id) == cr_ids, "Missing a cr_id after processing"
assert df[df.cr_id.isnull()].empty, "All accused must have a cr_id"

df.cr_id = df.cr_id.astype(str) 

df.to_csv(cons.output_remerge_file, **cons.csv_opts)
