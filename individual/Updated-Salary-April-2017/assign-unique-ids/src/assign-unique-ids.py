import pandas as pd
import __main__

from assign_unique_ids_functions import assign_unique_ids, aggregate_data
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
        'input_file': 'input/salary.csv.gz',
        'output_file': 'output/salary.csv.gz',
        'output_demo_file': 'output/salary_demographics.csv.gz',
        'id_cols': [
            'First.Name', 'Last.Name', 'Middle.Initial', 'Suffix.Name',
            'Gender', 'Age.at.Hire'
            ],
        'sub_id_cols': [
            'First.Name', 'Last.Name', 'Middle.Initial', 'Suffix.Name',
            'Gender', 'Age.at.Hire', 'Salary', 'Year'
            ],
        'sub_conflict_cols': [
            'Start.Date', 'Org.Hire.Date'
            ],
        'id': 'salary_ID',
        'sub_id': 'salary_year_ID'
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

out_df = pd.DataFrame()
agg_full_df = pd.DataFrame()

for year in df['Year'].unique():
    sub_df = df[df['Year'] == year]
    sub_df = assign_unique_ids(sub_df, cons.sub_id,
                               cons.sub_id_cols,
                               cons.sub_conflict_cols)
    sub_df[cons.sub_id] = sub_df[cons.sub_id] + year * 100000
    out_df = out_df.append(sub_df)
    tagg_df = aggregate_data(sub_df, cons.sub_id,
                             cons.sub_id_cols,
                             max_cols=cons.sub_conflict_cols)
    agg_full_df = agg_full_df.append(tagg_df)

uid_df = assign_unique_ids(agg_full_df, cons.id,
                           id_cols=cons.id_cols + cons.sub_conflict_cols)
out_df = out_df.merge(uid_df[[cons.id, cons.sub_id]],
                      on=cons.sub_id, how='outer')
assert out_df.shape[0] == df.shape[0],\
        print('Remerged data does not match input dataset')

out_df.to_csv(cons.output_file, **cons.csv_opts)

agg_df = aggregate_data(out_df, cons.id,
                        id_cols=cons.id_cols,
                        max_cols=cons.sub_conflict_cols)
agg_df.to_csv(cons.output_demo_file, **cons.csv_opts)
