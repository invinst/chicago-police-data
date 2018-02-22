#!usr/bin/env python3
#
# Author(s):   Roman Rivera (Invisible Institute)

'''script containing utility functions used for clean name step'''

import re

from general_utils import string_strip, list_unique

from nameparser import HumanName
from nameparser.config import CONSTANTS
CONSTANTS.titles.remove(*CONSTANTS.titles)


class NameCleaners:
    def __init__(self, full_name='', human_name='',
                 first_name='', last_name='',
                 middle_initial='', middle_name=''):
        """Takes in name or name parts and performs standardization/cleaning

        Creates object for single name/name parts and performs standardization
        and cleaning operations on name, regardless of initial name format.

        Parameters
        ----------
        full_name : str
            Name in 'Last, First' format
        human_name : str
            Name in 'First Last' format
        first_name : str
        last_name : str
        middle_initial : str
        middle_name : str
        """
        self.full_name = full_name
        self.human_name = human_name

        self.first_name = first_name
        self.first_name_NS = ''

        self.last_name = last_name
        self.last_name_NS = ''

        self.middle_name = middle_name
        self.middle_initial = middle_initial
        self.middle_initial2 = ''

        self.suffix_name = ''
        self.second_name = ''

        if full_name:
            if not re.search('[a-zA-Z]', self.full_name):
                self.full_name = ','
            self.last_name, self.first_name = self.full_name.rsplit(',', 1)

        elif human_name:
            # Not perfect, needs work, see tests
            self.human_name = re.sub(r'MC\s', 'MC',
                                     self.human_name).replace('.', ' ')
            name_dict = HumanName(self.human_name).as_dict()
            if name_dict['middle']:
                if name_dict['middle'] in ['O', 'MC']:
                    # Change later, over counts number of O' ____ names
                    name_dict['last'] = ' '.join([name_dict['middle'],
                                                  name_dict['last']])
                else:
                    name_dict['first'] = ' '.join([name_dict['first'],
                                                   name_dict['middle']])
            if name_dict['suffix']:
                name_dict['last'] = ' '.join([name_dict['last'],
                                              name_dict['suffix']])
            self.last_name = name_dict['last']
            self.first_name = name_dict['first']

        self.first_name = string_strip(self.first_name, no_sep=False)
        self.last_name = string_strip(self.last_name, no_sep=False)

    def extract_suffix_name(self, name, suffixes):
        """Separate name and suffix
        Parameters
        ----------
        name : str
        suffixes : list
            list of strings specifying name suffixes to extract

        Returns
        -------
        name_tuple : tuple, length 2
            [0] name without suffix : str
            [1] suffix : str
        """
        suffix = [w for w in name.split(" ") if w in suffixes]
        assert len(suffix) < 2, 'Too many suffixes found {}'.format(suffix)
        if suffix and (len(suffix) != 1 or suffix != name.split(" ")[0]):
            name = re.sub(r'(^{0}\s)|(\s{0}$)|(\s{0}\s)'.format(suffix[0]),
                          ' ', name)
            name = string_strip(name, no_sep=False)
            suffix = suffix[0].replace(' ', '')
        else:
            suffix = ''

        if not name:
            name = suffix
            suffix = ''
        name_tuple = (string_strip(name, no_sep=False), suffix)
        return name_tuple

    def extract_middle_initial(self, name,
                               mi_pattern=r'(\s[A-Z]\s)|(\s[A-Z]$)',
                               not_pattern=r'^((DE LA)\s[A-Z]($|\s))',
                               avoid_suffixes=[]):
        """Separate name and suffix
        Parameters
        ----------
        name : str
        mi_pattern : str
            Regex pattern for identifying middle initial
        not_pattern : str
            Regex pattern to avoid identifying middle initial
        avoid_suffixes : list
            list of strings to avoid as middle initials

        Returns
        -------
        name_tuple : tuple, length 2
            [0] name without middle initial : str
            [1] middle initial : str
        """
        mi = re.search(mi_pattern, name)
        mi = mi.group() if mi and len(name) > 3 else ''
        if (mi and
            not ((not_pattern and re.search(not_pattern, name)) or
                 mi[-1] in avoid_suffixes)):
            name = re.sub(mi_pattern, ' ', name)
            mi = mi.replace(' ', '')
        else:
            mi = ''
        return string_strip(name, no_sep=False), mi

    def extract_parts(self):
        """Extracts middle initials and suffix names from first and last names
        """
        mi_parts = ['', self.middle_initial, self.middle_initial2,
                    '', '', '']
        sn_parts = [self.suffix_name, '', '']

        mi_pattern = r'(\s[A-Z]\s)|(\s[A-Z]$)'
        not_pattern = ''
        suffixes = ['II', 'III', 'IV', 'JR', 'SR']

        self.first_name, mi_parts[0] = \
            self.extract_middle_initial(self.first_name,
                                        mi_pattern, not_pattern, suffixes)
        self.first_name, mi_parts[3] = \
            self.extract_middle_initial(self.first_name,
                                        mi_pattern, not_pattern, suffixes)
        self.first_name, sn_parts[2] = \
            self.extract_suffix_name(self.first_name, suffixes)

        not_pattern = r'^((DE LA)\s[A-Z]($|\s))'
        suffixes.extend(['V', 'I'])

        self.last_name, sn_parts[1] = \
            self.extract_suffix_name(self.last_name, suffixes)
        self.last_name, mi_parts[4] = \
            self.extract_middle_initial(self.last_name,
                                        mi_pattern, not_pattern, suffixes)
        self.last_name, mi_parts[5] = \
            self.extract_middle_initial(self.last_name,
                                        mi_pattern, not_pattern, suffixes)
        mi_parts = list(filter(bool, list_unique(mi_parts)))
        if mi_parts:
            assert len(mi_parts) <= 2, print(mi_parts)
            mi_parts.extend([''] * (2 - len(mi_parts)))
            self.middle_initial = mi_parts[0]
            self.middle_initial2 = mi_parts[1]

        sn_parts = list(filter(bool, list_unique(sn_parts)))
        if sn_parts:
            assert len(sn_parts) <= 1, print(sn_parts)
            self.suffix_name = sn_parts[0]

    def clean(self):
        """Clean name

        Returns
        ----------
        name_dict : dict
            Dictionary of all name parts created in __init__ after cleaning
        """
        if self.middle_name != '' and self.middle_name[0] == '(':
            self.second_name = re.sub(r'\(|\)', '', self.middle_name)
        elif self.middle_name == '':
            pass
        else:
            self.middle_initial = self.middle_name[0]

        self.extract_parts()
        self.first_name = string_strip(self.first_name, no_sep=False)
        self.first_name_NS = string_strip(self.first_name, no_sep=True)

        self.last_name = string_strip(self.last_name, no_sep=False)
        self.last_name_NS = string_strip(self.last_name, no_sep=True)

        name_dict = self.__dict__
        return name_dict

#
# end
