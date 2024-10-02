#!usr/bin/env python3
#
# Author(s):   Ashwin Sharma (Invisible Institute)

'''export script for TRR-actions-responses_2017-2021_2021-06_p660692'''

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
        'input_file': 'input/TRR-actions-responses_2017-2021_2021-06_p660692.csv.gz',
        'output_file': 'output/TRR-actions-responses_2017-2021_2021-06_p660692.csv.gz',
        }
    

    assert (args['input_file'].startswith('input/') and
            args['input_file'].endswith('.csv.gz')),        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),        "output_file is malformed: {}".format(args['output_file'])

    return setup.do_setup(script_path, args)

if __name__ == "__main__":
    cons, log = get_setup()
     
    df = pd.read_csv(cons.input_file)

    def explode_action(df, col, description, person):
        """Helper function to explode out each instance from now different types of actions to match old data format

        Parameters
        ----------
        df : DataFrame
            full new action response dataframe
        col : str
            column name to be exploded
        description : str
            other description in database, just additional info on type of action response
        person : str
            Member or Subject Action

        Returns
        -------
        DataFrame
            exploded out dataframe
        """        
        return df \
            .assign(action=df[col].str.split(", "),
                    other_description=description,
                    person=person) \
            .explode('action') \
            [['row_id', 'trr_id', 'person', 'action', 'other_description']]
    
    force_mitigation = explode_action(df, 'force_mitigation_efforts', 'Force Mitigation', 'Member Action')
    control_tactics = explode_action(df, 'control_tactics', 'Control Tactic', 'Member Action')
    response_without_weapons = explode_action(df, 'response_without_weapons', 'Response Without Weapons', 'Member Action')
    response_with_weapons = explode_action(df, 'response_with_weapon', 'Response With Weapon', 'Member Action')
    subject_actions = explode_action(df, 'subject_actions', '', "Subject Action")

    df = pd.concat([force_mitigation, control_tactics, response_with_weapons, response_without_weapons, subject_actions])
               
    df = df.drop_duplicates(subset=['trr_id', 'person', 'action', 'other_description'])
    df.to_csv(cons.output_file, **cons.csv_opts)
     