import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb


class Elections(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    def get_results(self, election_date, contest_pattern=None, party=None, other_pattern=None, level=None):
        df = self.db.get_election_results(election_date)
        df = df[['contest', 'choice', 'party', 'county', 'precinct_name', 'votes']]
        if contest_pattern is not None:
            df = df[df.contest.str.contains(contest_pattern, regex=True)]
            df = df.drop(columns=['contest'])
        if party is not None:
            df = df[df.party == party]
            df = df.drop(columns=['party'])
        if other_pattern is not None:
            other_columns = list(df.columns)
            other_columns.remove('choice')
            df1 = df[~df.choice.str.contains(other_pattern, regex=True)]
            df2 = df[df.choice.str.contains(other_pattern, regex=True)].groupby(other_columns).sum().reset_index()
            df2 = df2.assign(choice='OTHER')
            df = pd.concat([df1, df2])
        return Elections.summarize(df, level)

    def get_over_under(self, election_date, contest_pattern=None, level=None):
        df = self.db.get_election_results_over_under(election_date)
        df = df[['contest', 'county', 'precinct_name', 'overvotes', 'undervotes']]
        if contest_pattern is not None:
            df = df[df.contest.str.contains(contest_pattern, regex=True)]
            df = df.drop(columns=['contest'])
        return Elections.summarize(df, level)

    @staticmethod
    def summarize(df, level):
        columns = list(df.columns)
        columns1 = [x for x in list(columns) if x not in ['votes', 'undervotes', 'overvotes']]
        if level is not None:
            columns.remove('precinct_name')
            columns1.remove('precinct_name')
        if level == 'state':
            columns.remove('county')
            columns1.remove('county')
        return df[columns].groupby(columns1).sum().reset_index()
