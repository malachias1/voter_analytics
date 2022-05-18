import json
from pathlib import Path
from collections import defaultdict
import re
import pandas as pd


class StreetNameNormalizer:
    STREET_ADDRESS_PATTERN = re.compile(r'^(\d+)\s+(.*)')
    DIRECTION_PATTERN = re.compile(r'(N|S|E|W|NW|NE|SW|SE)')

    def __init__(self):
        p = Path(Path(__file__).parent.parent, 'resources', 'street_suffix_abbreviations.json')
        with p.open('r') as f:
            abbrev = json.load(f)
            self.types = defaultdict(list)
            for k, v in abbrev.items():
                first_letter = k[0]
                patterns = self.types[first_letter]
                type_pattern = re.compile(rf'{"|".join(v)}')
                patterns.append({
                    'type_pattern': type_pattern,
                    'standard': k
                })

    @classmethod
    def clean_street_name(cls, value):
        if value is None:
            return None
        return value.replace('.', '').replace(',', ' ').upper().strip()

    def normalize(self, street_name):
        street_name = self.clean_street_name(street_name)
        if street_name == '':
            return ''
        parts = street_name.split()
        end = -1
        if self.DIRECTION_PATTERN.fullmatch(parts[end]):
            end -= 1
        if len(parts) + end >= 0:
            first_letter = parts[end][0]
            patterns = self.types.get(first_letter, None)
            if patterns is not None:
                for i in patterns:
                    m = i['type_pattern'].search(parts[end])
                    if m is None:
                        continue
                    parts[end] = i["standard"]
                    break
        return ' '.join(parts)

    def street_address_split(self, street_address):
        if pd.notna(street_address) and street_address is not None:
            street_address = self.clean_street_name(street_address)
            m = self.STREET_ADDRESS_PATTERN.match(street_address)
            if m is not None:
                house_number = m.group(1)
                street_name = self.normalize(m.group(2))
                return house_number, street_name
            return '', self.normalize(street_address)
        return None, None

    def is_street_address(self, street_address):
        house_number, street_name = self.street_address_split(street_address)
        return house_number is not None and street_name is not None and len(house_number) > 0 and len(street_name) > 0

