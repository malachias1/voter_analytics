import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from pathlib import Path
import xml.etree.ElementTree as Et
from dateutil import parser
import time


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
        return party.upper().strip() if party is not None else None

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
        return vote_type_element.get('name').upper().strip()

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


class IngestElectionResults(Pathes):
    COL_NAMES = ['election_date',
                 'contest',
                 'is_question',
                 'choice',
                 'party',
                 'county',
                 'precinct_name',
                 'vote_type',
                 'votes'
                 ]

    OVER_COL_NAMES = ['election_date',
                      'contest',
                      'county',
                      'precinct_name',
                      'overvotes'
                      ]

    UNDER_COL_NAMES = ['election_date',
                       'contest',
                       'county',
                       'precinct_name',
                       'undervotes'
                       ]

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def con(self):
        return self.db.con

    def get_contest_results(self, er):
        return pd.DataFrame.from_records(er.contests,
                                         columns=self.COL_NAMES) \
            .assign(timestamp=time.time_ns())

    def get_contest_over_under(self, er):
        df1 = pd.DataFrame.from_records(er.overvotes,
                                        columns=self.OVER_COL_NAMES)
        df2 = pd.DataFrame.from_records(er.undervotes,
                                        columns=self.UNDER_COL_NAMES)
        return df1.merge(df2, on=['election_date', 'contest', 'county', 'precinct_name'], how='inner') \
            .assign(timestamp=time.time_ns())

    def ingest(self, path):
        er = ElectionResultReaderBase(path)
        df_contest = self.get_contest_results(er)
        df_over_under = self.get_contest_over_under(er)
        df1 = df_contest[['election_date', 'county']].drop_duplicates()
        df1.apply(lambda row: self.db.delete_election_result(row[0], row[1]), axis=1)
        df1.apply(lambda row: self.db.delete_election_result_over_under(row[0], row[1]), axis=1)

        stmt = f"""
        insert into election_results ('election_date', 'contest', 'is_question', 'choice', 'party',
               'county', 'precinct_name','vote_type','votes', 'timestamp') values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur = self.con.cursor()
        df_contest.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 10)]), axis=1)
        stmt = f"""insert into election_results_over_under ('election_date', 'contest',
               'county', 'precinct_name', 'overvotes', 'undervotes', 'timestamp')values (?, ?, ?, ?, ?, ?, ?)
        """
        cur = self.con.cursor()
        df_over_under.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 7)]), axis=1)
        self.con.commit()
