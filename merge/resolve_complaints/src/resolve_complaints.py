#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''resolve_complaints generation script'''

import pandas as pd
import numpy as np
from general_utils import keep_duplicates, remove_duplicates

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
    'input_cmpl1_file' : "input/complaints-complaints_1967-1999_2016-12.csv.gz",
    'input_cmpl2_file' : "input/complaints-complaints_2000-2016_2016-11.csv.gz",
    'input_cmpl3_file' : "input/complaints-complaints_2000-2018_2018-03.csv.gz",
    'input_acc1_file' : "input/complaints-accused_1967-1999_2016-12.csv.gz",
    'input_acc2_file' : "input/complaints-accused_2000-2016_2016-11.csv.gz",
    'input_acc3_file' : "input/complaints-accused_2000-2018_2018-03.csv.gz",

    'output_accused_file' : "output/complaints-accused.csv.gz",
    'output_complaints_file' : "output/complaints-complaints.csv.gz",

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
        [-1, 'Unknown']]
        }

    return setup.do_setup(script_path, args)


cons, log = get_setup()

# helper functions
def fewest_nans(x):
    nulls = x.apply(lambda x: x.isnull().sum(), axis=1)
    return x[nulls == nulls.min()].iloc[0]

def resolve(d):
    nulls = d.apply(lambda x: x.isnull().sum(), axis=1)
    if nulls.sum() == 1 and d['closed_date'].isnull().any():
        d = d[nulls == 0]
    elif d['closed_date'].isnull().all():
        if nulls.sum()==2:
            d = d[d['cv']==2]
        elif nulls.sum()==3:
            d = d[nulls == 1]
    elif (nulls.sum() == 2 and
          d[d['closed_date'].notnull()]['cv'].tolist() == [3] and
          d[d['incident_date'].notnull()]['cv'].tolist() == [2]):
        d.loc[d['cv']==3, 'incident_date'] = d['incident_date'].dropna().values[0]
        d = d[d['cv']==3]
        assert not d.dropna(how='any').empty
    else:
        print(d)
        raise
    assert d.shape[0]==1, print('hm', d)
    return d


log.info('Assembling 1967 - 1999 complaints')
c1 = pd.read_csv(cons.input_cmpl1_file)
c1crs = set(c1.cr_id)
c1 = c1[['cr_id', 'incident_date', 'complaint_date', 'closed_date', 'incident_location']]\
    .rename(columns={'incident_location': 'address'})\
    .drop_duplicates()
c1[cons.date_cols] = c1[cons.date_cols].apply(pd.to_datetime)
c1['address'] = c1['address'].fillna('').astype(str)
c1 = \
keep_duplicates(c1, 'cr_id')\
    .groupby('cr_id', as_index=False)\
    .agg({
        'incident_date' : min, 'complaint_date' : min, 'closed_date' : min,
        'address' : lambda x: sorted(x, key=len)[-1]
    })\
    .append(remove_duplicates(c1, 'cr_id'))
assert c1.shape[0] == c1['cr_id'].nunique()
assert set(c1.cr_id) == c1crs
assert c1[c1.cr_id.isnull()].empty

log.info('Assembling 2000 - 2016 complaints')
c2 = pd.read_csv(cons.input_cmpl2_file)
c2crs = set(c2.cr_id)
c2_core = c2\
    .drop(['row_id'] + cons.date_cols, axis=1)\
    .drop_duplicates()

c2_core = keep_duplicates(c2_core, 'cr_id')\
    .replace({'----' : ''})\
    .replace({'' : np.nan})\
    .groupby('cr_id', as_index=False)\
    .apply(fewest_nans)\
    .append(remove_duplicates(c2_core, 'cr_id'))
keep_duplicates(c2, 'cr_id')

c2_dates = c2[['cr_id'] + cons.date_cols]\
    .drop_duplicates()
c2_dates[cons.date_cols] = c2_dates[cons.date_cols].apply(pd.to_datetime)
c2_dates = c2_dates.groupby('cr_id', as_index=False).min()
c2 = c2_core.merge(c2_dates, on='cr_id', how='inner')
assert c2.shape[0] == c2_core.shape[0] == c2_dates.shape[0] == c2['cr_id'].nunique()
assert set(c2.cr_id) == c2crs
assert c2[c2.cr_id.isnull()].empty

log.info('Assembling 2000 - 2018 complaints')
c3 = pd.read_csv(cons.input_cmpl3_file)
c3crs = set(c3.cr_id)
c3.rename(columns={'incident_start_date' : 'incident_date'}, inplace=True)
c3_core = c3\
    .drop(['row_id', 'incident_end_date'] + cons.date_cols, axis=1)\
    .drop_duplicates()\
    .rename(columns={'current_complaint_category' : 'complaint_category',
                     'current_complaint_category_code' : 'complaint_code'})

c3_core = keep_duplicates(c3_core, 'cr_id')\
    .replace({'?': np.nan, '0' : np.nan, 0.0 : np.nan})\
    .groupby('cr_id', as_index=False)\
    .apply(fewest_nans)\
    .append(remove_duplicates(c3_core, 'cr_id'))
c3_dates = c3[['cr_id'] + cons.date_cols]\
    .drop_duplicates()
c3_dates[cons.date_cols] = c3_dates[cons.date_cols].apply(pd.to_datetime)
c3_dates = c3_dates.groupby('cr_id', as_index=False).min()
c3 = c3_core.merge(c3_dates, on='cr_id', how='inner')
assert c3.shape[0] == c3_core.shape[0] == c3_dates.shape[0] == c3['cr_id'].nunique()
assert set(c3.cr_id) == c3crs
assert c3[c3.cr_id.isnull()].empty

log.info('Assembling 1967 - 1999 accused')
a1 = pd.read_csv(cons.input_acc1_file)
a1crs = set(a1.cr_id)
a1 = a1.loc[a1['UID'].notnull(), ['cr_id', 'UID', 'complaint_category', 'complaint_description', 'final_finding', 'final_discipline_desc']]\
    .rename(columns={
            'complaint_category' : 'complaint_code',
            'complaint_description': 'complaint_category',
            'final_discipline_desc' : 'final_outcome'}
           )\
    .drop_duplicates()
a1drop = a1crs - set(a1.cr_id)
a1crs = set(a1.cr_id)
a1['ff_val'] = a1['final_finding'].replace({'SU' : 1, 'EX' : 2, 'UN' : 3, 'NS' : 4, 'NAF' : 5}, value=np.nan)
a1 = keep_duplicates(a1, ['cr_id', 'UID'])\
    .sort_values(['cr_id', 'UID', 'ff_val'])\
    .groupby(['cr_id', 'UID'], as_index=False)\
    .first()\
    .append(remove_duplicates(a1, ['cr_id', 'UID']))\
    .drop('ff_val', axis=1)
assert keep_duplicates(a1, ['cr_id', 'UID']).empty
a1.insert(0, 'cv', 1)
assert set(a1.cr_id) == a1crs
assert a1[a1.cr_id.isnull()].empty

log.info('Assembling 2000 - 2016 accused')
outcome_dict = {int(ol[0]) : ol[1] for ol in cons.outcome_lists}
a2 = pd.read_csv(cons.input_acc2_file)
a2crs = set(a2.cr_id)
a2 = a2[
    ['UID', 'cr_id', 'recommended_discipline', 'final_discipline',
     'recommended_finding', 'final_finding', 'complaint_category']]\
.drop_duplicates()\
.rename(columns={'recommended_finding' : 'recc_finding'})

a2['complaint_code'], a2['complaint_category'] = a2['complaint_category'].str.split('-', 1).str
a2['recc_outcome'] = a2['recommended_discipline'].fillna(-1).astype(int).replace(outcome_dict)
a2['final_outcome'] = a2['final_discipline'].fillna(-1).astype(int).replace(outcome_dict)
a2.drop(['final_discipline', 'recommended_discipline'], axis=1, inplace=True)
assert keep_duplicates(a2, ['cr_id', 'UID']).empty
a2.insert(0, 'cv', 2)
assert set(a2.cr_id) == a2crs
assert a2[a2.cr_id.isnull()].empty

log.info('Assembling 2000 - 2018 accused')
a3 = pd.read_csv(cons.input_acc3_file)
a3crs = set(a3.cr_id)
a3 = a3.query('merge == 1')
a3drop = a3crs - set(a3.cr_id)
a3crs = set(a3.cr_id)
a3 = a3[
    ['cr_id', 'UID', 'accusation_id',
     'final_finding_USE', 'final_outcome_USE']]\
     .rename(columns={'final_finding_USE' : 'final_finding',
                 'final_outcome_USE' : 'final_outcome'})
a3['final_finding'] = a3['final_finding'].replace({
    'NOT SUSTAINED' : 'NS', 'SUSTAINED' : 'SU',
    'UNFOUNDED': 'UN', 'NO AFFIDAVIT' : 'NAF',
    'EXONERATED' : 'EX', 'ADDITIONAL INVESTIGATION REQUESTED' : np.nan})
a3['ff_val'] = a3['final_finding'].replace({'SU' : 1, 'EX' : 2, 'UN' : 3, 'NS' : 4, 'NAF' : 5}, value=np.nan)
a3 = \
    a3[a3['UID'].notnull()]\
    .sort_values(['cr_id', 'UID', 'ff_val'])\
    .groupby(['cr_id', 'UID'], as_index=False)\
    .first()\
    .append(a3[a3['UID'].isnull()])\
    .drop('ff_val', axis=1)
assert keep_duplicates(a3, ['cr_id', 'UID']).empty
assert set(a3.cr_id) == a3crs
assert a3[a3.cr_id.isnull()].empty

a3.drop('accusation_id', axis=1, inplace=True)
a3.insert(0, 'cv', 3)
a3 = a3.merge(c3[['cr_id', 'complaint_code', 'complaint_category']], how='left')
c3.drop(['complaint_code', 'complaint_category'], axis=1, inplace=True)

log.info('Combining accused')
all_aCRs = set(list(a1crs) + [str(int(x)) for x in list(a2crs)] + [str(int(x)) for x in list(a3crs)])
aa = pd.concat([a1, a2, a3])
aa['cr_id'] = aa['cr_id'].astype(str)

assert aa[aa.cr_id.isnull()].empty
assert set(aa.cr_id) == all_aCRs

log.info('Combining complaints')
c1.insert(0, 'cv', 1)
c2.insert(0, 'cv', 2)
c3.insert(0, 'cv', 3)

icl = ['cr_id', 'cv'] + cons.date_cols
c1.cr_id.astype(str)
cc = c1[icl]\
    .append(c2[icl])\
    .append(c3.loc[~c3['cr_id'].isin(c2[icl].dropna(how='any')['cr_id']), icl])
cc['cr_id'] = cc['cr_id'].astype(str)

all_cCRs = set(list(c1crs) + [str(int(x)) for x in list(c2crs)] + [str(int(x)) for x in list(c3crs)])
ccrs = set(pd.concat([c1.cr_id.astype(str), c2.cr_id.astype(str), c3.cr_id.astype(str)]))
assert ccrs == {str(x) for x in (c1crs | c2crs | c3crs)}
assert ccrs == set(cc.cr_id)
assert all_cCRs == set(cc.cr_id)

rcc = remove_duplicates(cc, ['cr_id'])
kcc = keep_duplicates(cc, ['cr_id'])

cc = kcc.groupby('cr_id', as_index=False)\
    .apply(resolve)\
    .append(rcc)\
    .reset_index(drop=True)

assert keep_duplicates(cc, 'cr_id').empty
assert ccrs == set(cc.cr_id)
assert all_cCRs == set(cc.cr_id)

log.info('Combining complaints addresses')
c1a = c1[['cr_id', 'cv', 'address']].rename(columns={'address' : 'full_address'})
c2 = c2.rename(columns={'location_code' : 'location', 'address_number' : 'add1', 'city_state' : 'city'})\
        .replace('-----', '')
c2['add2'] = c2['street'].fillna('') + ' ' + c2['apartment_number'].fillna('')
c2a = c2[['cr_id', 'cv', 'location', 'add1', 'add2', 'city', 'beat']]
c3a = c3[['cr_id', 'cv', 'address_number', 'street_direction',
    'incident_location', 'street_name', 'apartment_no',
    'city', 'state', 'zip', 'incident_beat']]

c3a = c3a.rename(columns={
    'incident_location' : 'location',
    'incident_beat' : 'beat'})

c3a['add1'] = c3a['address_number'].fillna(0).astype(int).map(lambda x: str(x) if x else '')
c3a['add2'] = c3a['street_direction'].fillna('') + ' ' + c3a['street_name'].fillna('') + ' ' + c3a['apartment_no'].fillna('')
c3a['city'] = c3a['city'].fillna('') + ' ' + c3a['state'].fillna('') + ' ' + c3a['zip'].fillna('').astype(str)
c3a = c3a[['cv', 'cr_id', 'add1', 'add2', 'city', 'location', 'beat']]

log.info('Combining complaints dates and addresses')
ca = pd.concat([c1a, c2a, c3a])
ca['cr_id'] = ca['cr_id'].astype(str)
rows = cc.shape[0]
cc = cc.merge(ca, on=['cr_id', 'cv'])
assert rows == cc.shape[0]
assert ccrs == set(cc.cr_id)
assert all_cCRs == set(cc.cr_id)

log.info('Filtering accused using combined complaints')
aa2 = aa.merge(cc[['cv', 'cr_id']].drop_duplicates(), on=['cv', 'cr_id'])

log.info('Finding and readding any missing accused')
mcrs = set(aa.cr_id) - set(aa2.cr_id)
assert a1[a1.cr_id.isin(mcrs)].empty
mcrs = {int(x) for x in mcrs}
a2mcrs = set(a2.cr_id) & mcrs
a3mcrs = set(a3.cr_id) & mcrs - a2mcrs
assert len(a2mcrs) + len(a3mcrs) == len(mcrs)

a2m = a2[a2['cr_id'].isin(a2mcrs)]
assert a2m.cr_id.nunique() == len(a2mcrs)
c2m = c2[c2['cr_id'].isin(a2mcrs)].merge(c2a[c2a['cr_id'].isin(a2mcrs)], on=['cr_id', 'cv'])
assert c2m.cr_id.nunique() == len(a2mcrs)
assert c2m.shape[0] == len(a2mcrs)

a3m = a3[a3['cr_id'].isin(a3mcrs)]
assert a3m.cr_id.nunique() == len(a3mcrs)
c3m = c3[c3['cr_id'].isin(a3mcrs)].merge(c3a[c3a['cr_id'].isin(a3mcrs)], on=['cr_id', 'cv'])
assert c3m.cr_id.nunique() == len(a3mcrs)
assert c3m.shape[0] == len(a3mcrs)

aa2 = aa2.append(a2m).append(a3m)
aa2['cr_id'] = aa2['cr_id'].astype(str)
assert set(aa.cr_id) == set(aa2.cr_id)

assert set(aa2.cr_id) == all_aCRs
assert set(cc.cr_id) == all_cCRs

log.info('Writing complaints (%d CRs) and accused (%d CRs)' % (len(all_cCRs), len(all_aCRs)))
aa2.to_csv(cons.output_accused_file, **cons.csv_opts)
cc.to_csv(cons.output_complaints_file, **cons.csv_opts)
