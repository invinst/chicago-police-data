#! usr/bin/env python3
#
# Author:   Roman Rivera (Invisible Institute)

'''pytest functions in clean_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
from clean_name_utils import NameCleaners


def test_clean():
    ''' tests NameCleaners.clean()'''

    input_list = [ {'first_name' : 'DE ANDRE', 'last_name' : 'DE LA O'},
                   {'full_name' : 'JONES V, BOB D'},
                   {'first_name' : 'A RICHARD  A', 'last_name' : 'LUQUE-ROSALES'},
                   {'full_name': 'MC CARTHY, KIM-TOY'},
                   {'full_name' : 'O BRIEN, JO ANN'},
                   {'full_name' : ' SADOWSKY, JR , J T'},
                   {'first_name': 'ABDUL-AZIZ', 'last_name' : 'NORWOOD II'},
                   {'first_name': 'MARCELLUS. H', 'middle_initial' : '', 'last_name' : 'BURKE JR J'},
                   {'first_name': 'ERIN  E', 'middle_initial' : 'M', 'last_name' : 'VON KONDRAT'},
                   {'human_name' : 'J EDGAR HOOVER'},
                   {'first_name' : 'GEORGE H', 'middle_name' : 'WALKER', 'last_name' : 'BUSH'} ]

    results = pd.DataFrame([NameCleaners(**name_dict).clean() for name_dict in input_list])

    output_df = pd.DataFrame(
        {'first_name': ['DE ANDRE', 'BOB', 'A RICHARD', 'KIM-TOY',
                        'JO ANN', 'J T', 'ABDUL-AZIZ', 'MARCELLUS',
                        'ERIN', 'J EDGAR', 'GEORGE'],
         'middle_initial': ['', 'D', 'A', '', '', '', '', 'H', 'E', '', 'H'],
         'middle_name': ['', '', '', '', '', '', '', '', '', '', 'WALKER'],
         'middle_initial2': ['', '', '', '', '', '', '', 'J', 'M', '', 'W'],
         'suffix_name': ['', 'V', '', '', '', 'JR', 'II', 'JR', '', '', ''],
         'last_name': ['DE LA O', 'JONES', 'LUQUE-ROSALES', 'MC CARTHY',
                       'O BRIEN', 'SADOWSKY', 'NORWOOD', 'BURKE',
                       'VON KONDRAT', 'HOOVER', 'BUSH'],
         'first_name_NS': ['DEANDRE', 'BOB', 'ARICHARD', 'KIMTOY',
                           'JOANN', 'JT', 'ABDULAZIZ', 'MARCELLUS',
                           'ERIN', 'JEDGAR', 'GEORGE'],
         'last_name_NS': ['DELAO', 'JONES', 'LUQUEROSALES', 'MCCARTHY',
                          'OBRIEN', 'SADOWSKY', 'NORWOOD', 'BURKE',
                          'VONKONDRAT', 'HOOVER', 'BUSH']})

    results = results[output_df.columns]
    print(results)
    print(output_df)
    assert sorted(results.columns) == sorted(output_df.columns)
    for col in results.columns:
        assert results[col].equals(output_df[col])

def test_clean_human_names():
    ''' tests NameCleaners.clean() for human names'''

    input_list = [ {'human_name' : 'DE ANDRE DE LA O'},
                   {'human_name' : 'BOB D JONES V'},
                   {'human_name' : 'A RICHARD  A LUQUE-ROSALES',},
                   {'human_name': 'KIM-TOY MC CARTHY'},
                   {'human_name' : 'JO ANN O BRIEN'},
                   {'human_name' : 'J T SADOWSKY JR'},
                   {'human_name': 'ABDUL-AZIZ NORWOOD II'},
                   {'human_name': 'MARCELLUS. H BURKE JR J'},
                   {'human_name': 'ERIN  E M VON KONDRAT'},
                   {'human_name' : 'J EDGAR HOOVER'},
                   {'human_name' : 'GEORGE H WALKER BUSH'} ]

    results = pd.DataFrame([NameCleaners(**name_dict).clean() for name_dict in input_list])

    output_df = pd.DataFrame(
        {'first_name': ['DE ANDRE', 'BOB', 'A RICHARD', 'KIM-TOY',
                        'JO ANN', 'J T', 'ABDUL-AZIZ', 'MARCELLUS',
                        'ERIN', 'J EDGAR', 'GEORGE'],
         'middle_initial': ['', 'D', 'A', '', '', '', '', 'H', 'E', '', 'H'],
         'middle_name': ['', '', '', '', '', '', '', '', '', '', 'WALKER'],
         'middle_initial2': ['', '', '', '', '', '', '', 'J', 'M', '', 'W'],
         'suffix_name': ['', 'V', '', '', '', 'JR', 'II', 'JR', '', '', ''],
         'last_name': ['DE LA O', 'JONES', 'LUQUE-ROSALES', 'MC CARTHY',
                       'O BRIEN', 'SADOWSKY', 'NORWOOD', 'BURKE',
                       'VON KONDRAT', 'HOOVER', 'BUSH'],
         'first_name_NS': ['DEANDRE', 'BOB', 'ARICHARD', 'KIMTOY',
                           'JOANN', 'JT', 'ABDULAZIZ', 'MARCELLUS',
                           'ERIN', 'JEDGAR', 'GEORGE'],
         'last_name_NS': ['DELAO', 'JONES', 'LUQUEROSALES', 'MCCARTHY',
                          'OBRIEN', 'SADOWSKY', 'NORWOOD', 'BURKE',
                          'VONKONDRAT', 'HOOVER', 'BUSH']})

    results = results[output_df.columns]
    print(results)
    print(output_df)
    assert sorted(results.columns) == sorted(output_df.columns)
    for col in results.columns:
        assert results[col].equals(output_df[col])
