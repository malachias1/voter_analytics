import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from data.county_details import CountyDetails
from pathlib import Path
import xml.etree.ElementTree as Et
from datetime import datetime


class IngestElectionResults(Pathes):
    COL_NAMES = ['state',
                 'county',
                 'precinct_id',
                 'precinct_name',
                 'votes',
                 'election_date',
                 'contest',
                 'choice',
                 'vote_type',
                 'is_question',
                 'incumbent',
                 'party'
                 ]
    COL_TYPES = {'state': str,
                 'county': str,
                 'precinct_id': str,
                 'precinct_name': str,
                 'votes': int,
                 'election_date': str,
                 'contest': str,
                 'choice': str,
                 'vote_type': str,
                 'is_question': bool,
                 'incumbent': bool,
                 'party': str
                 }

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def con(self):
        return self.db.con

    @classmethod
    def county(cls, root):
        return root.find('Region').text

    @classmethod
    def get_election_date(cls, root):
        value = root.find('ElectionDate').text
        return datetime.strptime(value, '%m/%d/%Y').strftime('%Y-%m-%d')

    @classmethod
    def read_xml(cls, path):
        path = Path(path).expanduser()
        return Et.parse(path).getroot()

    def raw_tally(self, root):
        county = self.county(root)
        election_date = self.get_election_date(root)
        data = []
        for contest in root.findall("Contest"):
            contest_name = contest.get('text')
            is_question = contest.get('isQuestion')
            for choice in contest.findall("Choice"):
                choice_name = choice.get('text')
                for vote_type in choice.findall("VoteType"):
                    vote_type_name = vote_type.get('name')
                    for precinct in vote_type.findall('Precinct'):
                        precinct_name = precinct.get('name')
                        votes = int(precinct.get('votes'))
                        data.append([election_date, county, precinct_name, contest_name, choice_name,
                                     is_question, vote_type_name, votes])
        df = pd.DataFrame(data=data,
                          columns=['election_date', 'county', 'precinct_name', 'contest', 'choice', 'is_question',
                                   'vote_type', 'votes'])
        return df

    def read_csv(self, p):
        return pd.read_csv(p, dtype=self.COL_TYPES, true_values=['TRUE'], false_values=['FALSE'])

    # def ingest_precinct_details(self, df, year):
    #     cd = CountyDetails(self.root_dir)
    #     df = df.assign(county_code=map(lambda x: cd.name2code(x), df.county))
    #     new_details = df[['county_code', 'precinct_id', 'precinct_name']].drop_duplicates()
    #     old_details = self.db.get_precinct_details(year)
    #     details = new_details.merge(old_details, how='left', on=['county_code', 'precinct_id', 'precinct_name'])
    #     updates = details[details.id.isna()]
    #     next_id = self.db.get_precinct_id_max() + 1
    #     updates = updates.assign(id=range(next_id, next_id + len(updates)), year=year)
    #     stmt = f'replace into precinct_details (county_code, precinct_id, precinct_name, id, year) ' \
    #            f'values (?, ?, ?, ?, ?)'
    #     cur = self.con.cursor()
    #     df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 5)]), axis=1)
    #     self.con.commit()

    def ingest_election_results(self, p):
        cd = CountyDetails(self.root_dir_)
        df = self.read_csv(Path(p))
        df = df.assign(county_code=list(map(lambda x: cd.name2code(x), df.county)))
        df = df[['county_code', 'precinct_id', 'precinct_name',
                 'votes', 'election_date', 'contest', 'choice',
                 'vote_type', 'is_question', 'incumbent', 'party']]
        stmt = f'replace into election_results (county_code, precinct_id,precinct_name,votes,election_date,' \
               f'contest,choice,vote_type,is_question,incumbent,party)' \
               f'values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 11)]), axis=1)
        self.con.commit()
