#!usr/bin/env python3
#
# Author(s):    Roman Rivera (Invisible Institute)

'''export script for salary_2002-2017_2017-09_'''

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
        'input_file': 'input/salary_2002-2017_2017-09.csv.gz',
        'output_file': 'output/salary_2002-2017_2017-09.csv.gz',
        'input_profiles_file': 'input/salary_2002-2017_2017-09_profiles.csv.gz',
        'output_profiles_file': 'output/salary_2002-2017_2017-09_profiles.csv.gz',
        'export_cols': ['row_id', 'pay_grade','rank', 'salary', 'employee_status',
                        'org_hire_date', 'spp_date','start_date', 'year', 'age_at_hire',
                        'salary-2002_2002-2017_2017-09_ID',
                        'salary-2003_2002-2017_2017-09_ID',
                        'salary-2004_2002-2017_2017-09_ID',
                        'salary-2005_2002-2017_2017-09_ID',
                        'salary-2006_2002-2017_2017-09_ID',
                        'salary-2007_2002-2017_2017-09_ID',
                        'salary-2008_2002-2017_2017-09_ID',
                        'salary-2009_2002-2017_2017-09_ID',
                        'salary-2010_2002-2017_2017-09_ID',
                        'salary-2011_2002-2017_2017-09_ID',
                        'salary-2012_2002-2017_2017-09_ID',
                        'salary-2013_2002-2017_2017-09_ID',
                        'salary-2014_2002-2017_2017-09_ID',
                        'salary-2015_2002-2017_2017-09_ID',
                        'salary-2016_2002-2017_2017-09_ID',
                        'salary-2017_2002-2017_2017-09_ID',
                        'salary_2002-2017_2017-09_ID'],
        'id': 'salary_2002-2017_2017-09_ID'
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
df[[cons.id] + cons.export_cols].to_csv(cons.output_file, **cons.csv_opts)
profiles_df = pd.read_csv(cons.input_profiles_file)
profiles_df.to_csv(cons.output_profiles_file, **cons.csv_opts)
