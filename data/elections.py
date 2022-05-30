from pathlib import Path
import xml.etree.ElementTree as Et
from collections.abc import MutableMapping
from csv import DictWriter
from datetime import datetime


class AsDict(MutableMapping):
    def __init__(self, data):
        self.data = data

    def __getitem__(self, key):
        return self.data.get(self._keytransform(key))

    def __setitem__(self, key, value):
        self.data.set(self._keytransform(key), value)

    def __delitem__(self, key):
        self.data.attrib.pop(self._keytransform(key))

    def __iter__(self):
        return iter(self.data.keys())

    def __len__(self):
        return len(self.data.keys())

    def __str__(self):
        entries = [f"'{k}': '{self[k]}'" for k in self]
        return f'{"{"} {",".join(entries)}{"}"}'

    def _keytransform(self, key):
        return key


class Election:
    def __init__(self, state, path):
        self.state = state
        self.path = Path(path)
        self.root = Et.parse(self.path).getroot()
        self.election_date_ = None
        self.precinct_irregularities_ = None
        self.precinct_turnout_ = None
        self.precinct_tally_ = None

    @property
    def county(self):
        return self.root.find('Region').text

    @property
    def election_date(self):
        if not self.election_date_:
            value = self.root.find('ElectionDate').text
            self.election_date_ = datetime.strptime(value, '%m/%d/%Y').strftime('%Y-%m-%d')
        return self.election_date_

    def normalize(self, element):
        element = AsDict(element)
        element['precinct'] = element['name']
        del element['name']
        element['state'] = self.state
        element['county'] = self.county
        element['electionDate'] = self.election_date
        return element

    @property
    def precinct_irregularities(self):
        if not self.precinct_irregularities_:
            precincts = {}
            for contest in self.root.findall("Contest"):
                name = contest.get('text')
                is_question = contest.get('isQuestion')
                for u in contest.findall("VoteType[@name='Undervotes']/Precinct"):
                    u = self.normalize(u)
                    u['contest'] = name
                    u['isQuestion'] = is_question
                    u['undervotes'] = u['votes']
                    del u['votes']
                    key = f"{u['precinct']}-{name}"
                    precincts[key] = u
                for o in contest.findall("VoteType[@name='Overvotes']/Precinct"):
                    key = f"{o.get('name')}-{name}"
                    precincts[key]['overvotes'] = o.get('votes')
            self.precinct_irregularities_ = list(precincts.values())
        return self.precinct_irregularities_

    @property
    def precinct_turnout(self):
        if not self.precinct_turnout_:
            precincts = []
            for p in self.root.findall('VoterTurnout/Precincts/Precinct'):
                p = self.normalize(p)
                precincts.append(p)
            self.precinct_turnout_ = precincts
        return self.precinct_turnout_

    @property
    def precinct_tally(self):
        if not self.precinct_tally_:
            precincts = []
            for contest in self.root.findall("Contest"):
                name = contest.get('text')
                is_question = contest.get('isQuestion')
                for choice in contest.findall("Choice"):
                    choice_name = choice.get('text')
                    for vote_type in choice.findall("VoteType"):
                        vote_type_name = vote_type.get('name')
                        for precinct in vote_type.findall('Precinct'):
                            precinct = self.normalize(precinct)
                            precinct['contest'] = name
                            precinct['choice'] = choice_name
                            precinct['voteType'] = vote_type_name
                            precinct['isQuestion'] = is_question
                            precincts.append(precinct)
            self.precinct_tally_ = precincts
        return self.precinct_tally_

    def save_something(self, dst_dir, stem, something):
        if len(something) > 0:
            with Path(dst_dir, f'{self.election_date}_{stem}').with_suffix('.csv').open('w') as f:
                w = DictWriter(f, something[0].keys())
                w.writeheader()
                w.writerows(something)

    def save(self, dst_dir: Path):
        dst_dir.mkdir(exist_ok=True, parents=True)
        self.save_something(dst_dir, 'turnout', self.precinct_turnout)
        self.save_something(dst_dir, 'irregularities', self.precinct_irregularities)
        self.save_something(dst_dir, 'tally', self.precinct_tally)


if __name__ == '__main__':
    for p in Path('../resources/election_data').iterdir():
        if p.is_file() and p.suffix == '.xml':
            print(p)
            election = Election('GA', 'Cobb', p)
            election.save(Path('../processed'))
