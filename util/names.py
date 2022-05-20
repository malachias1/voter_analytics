import numpy as np
import re


class NameNormalizer:
    LEADING_PATTERN = re.compile(r'(SENATOR|REV|DR|MS|MRS|MR)')
    TRAILING_PATTERN = re.compile(r'(SR|JR|II|III|IV|MD|RN)$')
    HYPHEN_SPACE_PATTERN = re.compile(r'\s+-\s+')
    QUOTING_PATTERN = re.compile(r'\s*["(][^")].*[")]\s*')
    CONJUNCTION_PATTERN = re.compile(r'(\w+) (?:AND|&) (\w+)( (\w+(?: \w+)*))?')
    VAN_DER_PATTERN = re.compile(r'(\w+) (?:VAN[\s-]?DER)[\s-]?(\w+)$')
    VAN_PATTERN = re.compile(r'(\w+) VAN[\s-]?(\w+)$')
    WHITE_SPACE_PATTERN = re.compile(r'\s+')

    def is_compound_name(self, name):
        return self.CONJUNCTION_PATTERN.fullmatch(name) is not None

    def standarize_name_to_level_1(self, name):
        """
        Standarize a name by replacing commas and periods.
        Periods are replaced with an empty string because
        they are not generally used as separators. Commas
        are replaced with a space. Up case and strip.
        Remove quoted and parentheticals. Lastly, remove
        spaces around hypens (one case observed).
        :param name: a name, could be first, last, first and last, or more.
        :return: a standard level 1 name
        """
        name = name.replace('.', '').replace(',', ' ').upper().strip()
        name = self.HYPHEN_SPACE_PATTERN.sub('-', name)
        name = self.WHITE_SPACE_PATTERN.sub(' ', name)
        return self.QUOTING_PATTERN.sub(' ', name)

    def standarize_name_to_level_2(self, name):
        """
        Remove any titles or generations. Remove initials or single
        letters.
        :param name: a name, could be first, last, first and last, or more.
        :return: a standard level 2 name
        """
        name = self.standarize_name_to_level_1(name)

        leading_parts = []
        name_parts = []
        trailing_parts = []
        phase = 0
        for w in name.split():
            if len(w) < 2 and w != '&':
                continue
            if phase == 0:
                if self.LEADING_PATTERN.fullmatch(w):
                    leading_parts.append(w)
                else:
                    name_parts.append(w)
                    phase = 1
            elif phase == 1:
                if not self.TRAILING_PATTERN.fullmatch(w):
                    name_parts.append(w)
                else:
                    trailing_parts.append(w)
                    phase = 2
            else:
                if self.TRAILING_PATTERN.fullmatch(w):
                    trailing_parts.append(w)
                else:
                    print(f'Unrecognized trailing part, {w}!')
        return ' '.join(name_parts)

    def standarize_name_to_level_3(self, name):
        """
        Look for concatenated names (e.g., Bob and Sally)
        :param name: a name, could be first, last, first and last, or more.
        :return: one or more standard level 2 names
        """
        name = self.standarize_name_to_level_2(name)
        m = self.CONJUNCTION_PATTERN.fullmatch(name)
        if m is None:
            return name,
        if m.group(3) is not None:
            name1 = ' '.join([m.group(1), m.group(3).strip()])
            name2 = ' '.join([m.group(2), m.group(3).strip()])
        else:
            name1 = m.group(1)
            name2 = m.group(2)
        return name1, name2

    def normalize(self, name):
        return self.standarize_name_to_level_3(name)
