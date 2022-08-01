from django.db import models
import pandas as pd
from county_details.models import CountyDetails
from pathlib import Path
import xml.etree.ElementTree as Et
from dateutil import parser
import re
import json
import zipfile
import shutil
from datetime import datetime


class ChoiceManager(models.Manager):
    def create(self, choices):
        return pd.DataFrame.from_records([{'choice_id': x.id, 'choice': x.name}
                                          for x in self.bulk_create([Choice(name=n) for n in choices])])


class Choice(models.Model):
    name = models.TextField()

    objects = ChoiceManager()


class ContestManager(models.Manager):
    def create(self, contests):
        return pd.DataFrame.from_records([{'contest_id': x.id, 'contest': x.name}
                                          for x in self.bulk_create([Contest(name=n) for n in contests])])


class Contest(models.Model):
    name = models.TextField()

    objects = ContestManager()


class ElectionContestIdentifer:
    def __init__(self):
        self.patterns = None
        self.freq = None
        self.count = None
        self.categories = None

    def initialize(self):
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
            self.categories = {}

    def classify(self, detail):
        for entry in self.freq:
            idx = entry['idx']
            pattern = self.patterns[idx]
            category_re = pattern['category_re']
            name = detail.contest.name
            if category_re.search(name) is not None:
                entry['count'] += 1
                self.freq.sort(reverse=True, key=lambda x: x['count'])
                subcategory_re = pattern.get('subcategory_re', None)
                ambiguous = False
                if subcategory_re is None:
                    subcategory = '*'
                else:
                    m = subcategory_re.search(name)
                    subcategory = m.group(1) if m is not None else None
                    if subcategory is None:
                        ambiguous = True
                    subcategory = f'{int(subcategory):03d}' if subcategory is not None else '*'
                canonical_name = f'{pattern["category"]}-{subcategory}'
                if canonical_name in self.categories:
                    cc = self.categories[canonical_name]
                else:
                    cc = ContestCategory(category=pattern['category'],
                                         canonical_name=canonical_name,
                                         subcategory=subcategory,
                                         type=pattern['type'],
                                         is_question=pattern.get('is_question', False),
                                         ambiguous=ambiguous or pattern.get('ambiguous', False))
                    cc.save()
                    self.categories[canonical_name] = cc
                return detail, cc
        return detail, None


class ContestCategoryManager(models.Manager, ElectionContestIdentifer):
    def rebuild_contest_categories(self):
        self.initialize()
        ContestCategory.objects.all().delete()
        ContestCategoryMap.objects.all().delete()
        details = Detail.objects.all()
        contest_category_map = list(map(self.classify, details))
        for d, cc in contest_category_map:
            if cc is None:
                print(f'{d.contest.name} not classifiable')
        contest_category_map_objs = list(map(lambda k: ContestCategoryMap(detail=k[0], contest_category=k[1]),
                                             contest_category_map))

        ContestCategoryMap.objects.bulk_create(contest_category_map_objs)


class ContestCategory(models.Model):
    category = models.TextField()
    canonical_name = models.TextField()
    type = models.TextField()
    subcategory = models.TextField(blank=True, null=True)
    party = models.TextField(blank=True, null=True)
    is_question = models.BooleanField()
    ambiguous = models.BooleanField()

    objects = ContestCategoryManager()


class ContestCategoryMap(models.Model):
    detail = models.ForeignKey('Detail', on_delete=models.CASCADE)
    contest_category = models.ForeignKey('ContestCategory', on_delete=models.CASCADE)


class ElectionResultManagerBase:
    def ingest(self, path):
        cd = CountyDetails.objects.details
        for f in Path(path).expanduser().iterdir():
            if f.suffix == '.zip':
                extract_to = Path(f.parent, f'{f.stem}')
                move_to = Path(f.parent, f'{f.stem}.xml')
                if not move_to.exists():
                    with zipfile.ZipFile(f, 'r') as zip_ref:
                        zip_ref.extractall(extract_to)
                        shutil.move(Path(extract_to, 'detail.xml'), move_to)
                    extract_to.rmdir()

        for f in Path(path).expanduser().iterdir():
            if f.suffix == '.xml':
                print(f)
                er = ElectionResultReaderBase(f)
                self.ingest_county(er, cd)
                new_path = Path(f.parent, 'processed', f.name).with_suffix('.zip')
                f = f.with_suffix('.zip')
                if f.exists():
                    shutil.move(f, new_path)

    def ingest_county(self, er, cd):
        raise NotImplemented('ingest_county not implemented!')


class DetailManager(models.Manager, ElectionResultManagerBase):
    def get_results(self, category, district, date):
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d')
        mappings = ContestCategoryMap.objects.filter(contest_category__category=category,
                                                     contest_category__subcategory=district,
                                                     detail__election_date=date)
        return [x.detail for x in mappings]

    def party_filter(self, details, party):
        return list(filter(lambda d: d.party == party, details))

    # -------------------------------------------------------------------------
    # Ingest and Update Methods
    # -------------------------------------------------------------------------

    RAW_COL_NAMES = ['election_date', 'contest', 'is_question', 'choice',
                     'party', 'county_name', 'precinct_name', 'vote_type', 'votes'
                     ]
    COL_NAMES = ['election_date', 'county_code', 'contest_id', 'is_question',
                 'choice_id', 'party', 'precinct_name', 'vote_type', 'votes'
                 ]

    @property
    def con(self):
        return self.con

    def build_bulk_objs(self, row):
        contest = Contest.objects.get(id=row.contest_id)
        choice = Choice.objects.get(id=row.choice_id)
        obj = Detail(election_date=row.election_date, county_code=row.county_code,
                     contest=contest, is_question=row.is_question, choice=choice,
                     party=row.party, precinct_name=row.precinct_name, vote_type=row.vote_type,
                     votes=row.votes)
        if len(row.party) > 2:
            print(row)
        return obj

    def ingest_county(self, er, cd):
        df = pd.DataFrame.from_records(er.contests, columns=self.RAW_COL_NAMES)
        df = df.merge(cd[['county_code', 'county_name']], on='county_name', how='inner')
        choices = Choice.objects.create(df.choice.unique())
        contests = Contest.objects.create(df.contest.unique())
        df = df.merge(choices, on='choice', how='inner').drop(columns=['choice'])
        df = df.merge(contests, on='contest', how='inner').drop(columns=['contest'])
        df = df.assign(election_date=df.election_date.apply(lambda x: datetime.strptime(x, '%Y-%m-%d')))
        df = df[self.COL_NAMES]
        objs = df.apply(self.build_bulk_objs, axis=1).to_list()
        self.bulk_create(objs)


class Detail(models.Model):
    election_date = models.DateField()
    county_code = models.CharField(max_length=3)
    contest = models.ForeignKey('Contest', on_delete=models.CASCADE)
    choice = models.ForeignKey('Choice', on_delete=models.CASCADE)
    party = models.CharField(max_length=2, blank=True, null=True)
    is_question = models.BooleanField()
    precinct_name = models.CharField(max_length=64)
    vote_type = models.CharField(max_length=2)
    votes = models.IntegerField()

    objects = DetailManager()

    @property
    def as_record(self):
        return {
            'election_date': self.election_date,
            'county_code': self.county_code,
            'contest': self.contest,
            'choice': self.choice,
            'party': self.party,
            'is_question': self.is_question,
            'precinct_name': self.precinct_name,
            'vote_type': self.vote_type,
            'votes': self.votes,
        }

    class Meta:
        indexes = [
            models.Index(fields=('county_code', 'election_date', 'contest_id')),
        ]


class OverUnderVoteManager(models.Manager, ElectionResultManagerBase):
    OVER_COL_NAMES = ['election_date', 'contest', 'county_name', 'precinct_name', 'overvotes']
    UNDER_COL_NAMES = ['election_date', 'contest', 'county_name', 'precinct_name', 'undervotes']
    COL_NAMES = ['election_date', 'contest', 'county_code', 'precinct_name', 'overvotes', 'undervotes']

    def build_objs(self, row, contests):
        contest = contests[row.contest]
        if any([x is None or pd.isna(x) for x in row]):
            print(row)
        obj = OverUnderVote(election_date=row.election_date, county_code=row.county_code,
                            contest=contest, precinct_name=row.precinct_name,
                            overvotes=row.overvotes, undervotes=row.undervotes)
        return obj

    def get_contests(self, er, cd):
        election_date = er.election_date
        county_name = er.county
        county_code = cd[cd.county_name == county_name].county_code.iloc[0]
        election_results = Detail.objects.filter(election_date=election_date, county_code=county_code). \
            distinct('contest')
        return [x.contest for x in election_results]

    def ingest_county(self, er, cd):
        contests = {x.name: x for x in self.get_contests(er, cd)}
        df1 = pd.DataFrame.from_records(er.overvotes, columns=self.OVER_COL_NAMES)
        df2 = pd.DataFrame.from_records(er.undervotes, columns=self.UNDER_COL_NAMES)
        df = df1.merge(df2, on=self.OVER_COL_NAMES[:-1], how='inner')
        df = df.merge(CountyDetails.objects.details[['county_code', 'county_name']], on='county_name', how='inner')
        df = df[self.COL_NAMES]
        objs = df.apply(self.build_objs, axis=1, args=(contests,)).to_list()
        self.bulk_create(objs)


class OverUnderVote(models.Model):
    election_date = models.DateField()
    contest = models.ForeignKey('Contest', on_delete=models.CASCADE)
    county_code = models.TextField()
    precinct_name = models.TextField()
    overvotes = models.IntegerField()
    undervotes = models.IntegerField()

    objects = OverUnderVoteManager()

    @property
    def as_record(self):
        return {
            'election_date': self.election_date,
            'county_code': self.county_code,
            'contest': self.contest,
            'overvotes': self.overvotes,
            'undervotes': self.undervotes,
        }


class ElectionResults(models.Model):
    id = models.IntegerField(primary_key=True)
    election_date = models.TextField()
    county_code = models.TextField()
    contest = models.TextField()
    choice = models.TextField()
    party = models.TextField(blank=True, null=True)
    is_question = models.BooleanField()
    precinct_name = models.TextField()
    votes = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'election_results'


class ElectionResultReaderBase:
    def __init__(self, path):
        path = Path(path).expanduser()
        self.root = Et.parse(path).getroot()

    @property
    def contests(self):
        county = self.county
        election_date = self.election_date
        for contest in self.root.findall("Contest"):
            contest_name = self.get_contest_name(contest)
            is_question = self.is_question(contest)
            for choice_name, party, vote_type, precinct_name, votes in self.get_choices(contest):
                yield [election_date,
                       contest_name,
                       is_question,
                       choice_name,
                       party,
                       county,
                       precinct_name,
                       vote_type,
                       votes]

    @property
    def overvotes(self):
        county = self.county
        election_date = self.election_date
        for contest in self.root.findall("Contest"):
            contest_name = self.get_contest_name(contest)
            for precinct_name, votes in self.get_contest_overvote_details(contest):
                yield [election_date,
                       contest_name,
                       county,
                       precinct_name,
                       votes]

    @property
    def undervotes(self):
        county = self.county
        election_date = self.election_date
        for contest in self.root.findall("Contest"):
            contest_name = self.get_contest_name(contest)
            for precinct_name, votes in self.get_contest_undervote_details(contest):
                yield [election_date,
                       contest_name,
                       county,
                       precinct_name,
                       votes]

    @property
    def county(self):
        return self.root.find('Region').text.upper().strip()

    @property
    def county_voter_turnout(self):
        element = self.voter_turnout
        return {
            'total_votes': int(element.get('totalVoters').strip()),
            'ballots_cast': int(element.get('ballotsCast').strip()),
            'voter_turnout': float(element.get('voterTurnout').strip())
        }

    @property
    def election_date(self):
        value = self.root.find('ElectionDate').text.strip()
        return parser.parse(value).strftime('%Y-%m-%d')

    @property
    def election_name(self):
        return self.root.find('ElectionName').text.upper().strip()

    @property
    def precinct_voter_turnout(self):
        element = self.voter_turnout
        turnout = {}
        for p in element.findall('Precincts/Precinct'):
            name = p.get('name').upper().strip()
            turnout[p] = {
                'name': name,
                'total_votes': int(p.get('totalVoters').strip()),
                'ballots_cast': int(p.get('ballotsCast').strip()),
                'voter_turnout': float(p.get('voterTurnout').strip())
            }
        return turnout

    @property
    def timestamp(self):
        value = self.root.find('Timestamp').text.upper().strip()
        return parser.parse(value).strftime('%Y-%m-%d %H:%M:%S %z')

    @property
    def voter_turnout(self):
        return self.root.find('VoterTurnout')

    @property
    def voter_turnout_details(self):
        return self.root.findall('VoterTurnout/Precincts/Precinct')

    @classmethod
    def get_contest_name(cls, contest_element):
        return contest_element.get('text').upper().strip()

    @classmethod
    def get_contest_undervote_details(cls, contest_element):
        e = contest_element.find("VoteType[@name = 'Undervotes']")
        for p in e.findall('Precinct'):
            name = p.get('name').upper().strip()
            votes = int(p.get('votes').strip())
            yield [name, votes]

    @classmethod
    def get_contest_undervotes(cls, contest_element):
        e = contest_element.find("VoteType[@name = 'Undervotes']")
        return int(e.get('votes').strip())

    @classmethod
    def get_contest_overvote_details(cls, contest_element):
        e = contest_element.find("VoteType[@name = 'Overvotes']")
        for p in e.findall('Precinct'):
            name = p.get('name').upper().strip()
            votes = int(p.get('votes').strip())
            yield [name, votes]

    @classmethod
    def get_contest_overvotes(cls, contest_element):
        e = contest_element.find("VoteType[@name = 'Overvotes']")
        return int(e.get('votes').strip())

    @classmethod
    def get_choice_name(cls, choice_element):
        return choice_element.get('text').upper().strip()

    @classmethod
    def get_choice_party(cls, choice_element):
        party = choice_element.get('party')
        party = party.upper() if party is not None else 'NP'
        if party == 'REP':
            party = 'R'
        elif party == 'DEM':
            party = 'D'
        elif party == 'LIB':
            party = 'L'
        else:
            party = party[:2]
        return party.strip() if party is not None else None

    @classmethod
    def get_choice_total_votes(cls, choice_element):
        return int(choice_element.get('totalVotes').strip())

    @classmethod
    def get_choices(cls, contest_element):
        for c in contest_element.findall("Choice"):
            choice_name = cls.get_choice_name(c)
            party = cls.get_choice_party(c)
            for vote_type, precinct_name, votes in cls.get_vote_types(c):
                yield [choice_name, party, vote_type, precinct_name, votes]

    @classmethod
    def get_precinct_ballots_cast(cls, precinct_turnout_element):
        return int(precinct_turnout_element.get('ballotsCast').strip())

    @classmethod
    def get_precinct_total_votes(cls, precinct_turnout_element):
        return int(precinct_turnout_element.get('totalVoters').strip())

    @classmethod
    def get_precinct_turnout(cls, precinct_turnout_element):
        return int(precinct_turnout_element.get('totalVoters').strip())

    @classmethod
    def get_precinct_voter_turnout(cls, precinct_turnout_element):
        return float(precinct_turnout_element.get('voterTurnout').strip())

    @classmethod
    def get_precincts(cls, vote_type_element):
        return vote_type_element.findall('Precinct')

    @classmethod
    def get_vote_type_name(cls, vote_type_element):
        type_name = vote_type_element.get('name').upper().strip()
        if type_name.startswith('ELECTION'):
            return 'E'
        if type_name.startswith('PROVI'):
            return 'P'
        if type_name.startswith('ABSE'):
            return 'AB'
        if type_name.startswith('AD'):
            return 'AD'
        raise ValueError(f'Unknown vote type, {type_name}')

    @classmethod
    def get_vote_types(cls, choice_element):
        for v in choice_element.findall("VoteType"):
            vote_type = cls.get_vote_type_name(v)
            for p in cls.get_precincts(v):
                name, votes = cls.get_votes(p)
                yield [vote_type, name, votes]

    @classmethod
    def get_votes(cls, precinct_element):
        return [precinct_element.get('name').upper().strip(),
                int(precinct_element.get('votes'))
                ]

    @classmethod
    def is_question(cls, contest_element):
        return contest_element.get('isQuestion').upper().strip() == 'TRUE'


class ElectionResultReader(ElectionResultReaderBase):
    def __init__(self, path):
        super().__init__(path)
