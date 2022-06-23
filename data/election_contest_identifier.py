import re
from pathlib import Path
import json


class ElectionContestIdentifer:
    def __init__(self):
        with Path('../resources/election_result_patterns.json').open('r') as f:
            self.patterns = json.load(f)
            for pattern in self.patterns:
                pattern['category_re'] = re.compile(pattern['pattern'])
                party_pattern = pattern.get('party', None)
                pattern['party_re'] = re.compile(party_pattern) if party_pattern is not None else None
                subcategory_pattern = pattern.get('subcategory', None)
                pattern['subcategory_re'] = re.compile(subcategory_pattern) if subcategory_pattern is not None else None
            self.freq = [{'idx': i, 'count': 0} for i in range(0, len(self.patterns))]
            self.count = 0

    def classify(self, election_date, contest):
        for entry in self.freq:
            idx = entry['idx']
            pattern = self.patterns[idx]
            category_re = pattern['category_re']
            if category_re.search(contest) is not None:
                entry['count'] += 1
                self.freq.sort(reverse=True, key=lambda x: x['count'])
                subcategory_re = pattern.get('subcategory_re', None)
                ambiguous = False
                if subcategory_re is None:
                    subcategory = None
                else:
                    m = subcategory_re.search(contest)
                    subcategory = m.group(0) if m is not None else None
                    if subcategory is None:
                        ambiguous = True
                party_re = pattern.get('party_re', None)
                if party_re is None:
                    party = None
                else:
                    m = party_re.search(contest)
                    party = m.group(0) if m is not None else None
                    if party is None:
                        print(f'Cannot find party in "{contest}"')
                return {
                    'election_date': election_date,
                    'contest': contest,
                    'category': pattern['category'],
                    'canonical_name': pattern['canonical_name'],
                    'type': pattern['type'],
                    'subcategory': subcategory,
                    'party': party,
                    'is_question': pattern.get('is_question', False),
                    'ambiguous': ambiguous or pattern.get('ambiguous', False)
                }
        print(contest)
        return None
