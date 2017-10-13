import pandas as pd
import __main__

from clean_functions import clean_data, clean_int
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
        'input_file': 'input/complaints.csv.gz',
        'output_file': 'output/complaints.csv.gz'
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
df = clean_data(df)
na_beats = df[df['beat'].isnull()].shape[0]
spaced_beats = df[(df['beat'].notnull()) &
                  (df['beat'].str.contains(' '))].shape[0]
df['beat'] = (df['beat']
              .map(lambda x: x.replace(' ', '') if isinstance(x, str) else x)
              .map(lambda x: abs(clean_int(x))))
log.info(('beat column converted to positive integers.\n'
          ' Assumed that spaces between numbers in beats, e.x. 31 0'
          ' should be replaced with no space. "31 0" -> "310" in {0} places.\n'
          '{1} originall NA. Now {2} now NA values.'
          '').format(spaced_beats, na_beats, df[df['beat'].isnull()].shape[0]))
df.to_csv(cons.output_file, **cons.csv_opts)
