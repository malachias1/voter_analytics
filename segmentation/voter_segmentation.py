import re

import numpy as np
import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from pathlib import Path
from datetime import datetime, timedelta


class VoterSegmentation(Pathes):
    ELECTIONS = [
        {'date': '20201103', 'is_primary': False},
        {'date': '20181106', 'is_primary': False},
        {'date': '20161108', 'is_primary': False},
        {'date': '20141104', 'is_primary': False},
        {'date': '20220524', 'is_primary': True},
        {'date': '20200609', 'is_primary': True},
        {'date': '20180522', 'is_primary': True},
        {'date': '20160524', 'is_primary': True},
        {'date': '20140520', 'is_primary': True}
    ]

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    def history_for_election_date(self, election_date, is_primary):
        date = datetime.strptime(election_date, '%Y%m%d')
        df = self.db.voter_history_for_date(date.year, date.month, date.day)
        # Assume NaN means voter cast ballot in non-partisan ballot.
        # Party, for those that didn't vote is set to X.
        # Ensure the party, if NaN, is set to NP for primary and
        # G for general election.
        if is_primary:
            df = df.assign(party=df.loc[:, 'party'].fillna('NP'))
            # Select only general election primary. Sometimes there is
            # a special election entry as well as a general election
            # primary entry.
            df = df[df.type == '001']
            df = df.assign(type='p')
        else:
            # Select only general election. Sometimes there is
            # a special election entry as well as a general election
            # entry.
            df = df[df.type == '003']
            df = df.assign(type='g')
            # Set party in general election to G
            df = df.assign(party='G')

        df = df[['voter_id', 'date', 'type', 'party']]
        df = df.assign(date=date)
        return df

    def history(self):
        df = pd.DataFrame()
        for election in self.ELECTIONS:
            election_date = election['date']
            is_primary = election['is_primary']
            df = pd.concat([df, self.history_for_election_date(election_date, is_primary)])

        # We are going to create dummy records for
        # each election for each voter.
        election_date_types = df[['date', 'type']].drop_duplicates()
        election_date_types = election_date_types.assign(key=0)
        voters = df[['voter_id']].drop_duplicates()
        voters = voters.assign(key=0)
        df2 = voters.merge(election_date_types, on='key', how='outer').drop(columns=['key'])
        # When I merge, we end up with a record for every election
        # for every voter. If the voter did not cast a ballot in that
        # election then party will be NaN.  I set party to X (i.e.,
        # don't care) if it is NaN. This covers general election case
        # and voter that did not cast ballot.
        df = df.merge(df2, on=['voter_id', 'date', 'type'], how='right')
        df = df.assign(party=df.loc[:, 'party'].fillna('X'))

        vs = self.db.get_voter_status()[['voter_id', 'status', 'date_added']]
        vs = vs[vs.status == 'A'].drop(columns=['status'])
        # If date_added is blank assume is proceeds the earliest election.
        # Lots of examples of date_added = 19000101 in voter list.
        vs = vs.assign(date_added=vs.date_added.str.replace('^$', '19000101', regex=True))
        # Add 30 days to date added -- registration has to be far enough in
        # advance so person can vote. If the resulting date is
        # greater than election date then, voter can't cast ballot.
        thirty_days = timedelta(30)
        vs = vs.assign(date_added=vs.date_added.apply(lambda d: datetime.strptime(d, '%Y%m%d') + thirty_days))
        df = vs.merge(df, on=['voter_id'], how='inner')
        df = df[df.date_added <= df.date].drop(columns=['type'])
        df = df.pivot(index='voter_id', columns="date", values="party")
        # Convert index, which is voter_id, to column (in column 0).
        df = df.reset_index()
        # Convert date column names to strings
        columns = list(df.columns)
        columns = list(map(lambda x: x.strftime('%Y-%m-%d'), columns[1:]))
        columns.insert(0, 'voter_id')
        # set column names
        df.columns = columns
        # Add demographics to voter history
        vd = self.db.get_voter_demographics()
        df = vd.merge(df, on=['voter_id'], how='inner')
        # Get the current county code for voter
        av = self.db.get_residence_address_voter()
        ac = self.db.get_residence_addresses()[['address_id', 'county_code']]
        vc = av.merge(ac, on=['address_id'], how='inner')[['voter_id', 'county_code']]
        # Add county code to voter history
        return vc.merge(df, on=['voter_id'], how='inner')

    def rebuild(self):
        # Gather history
        df = self.history()

        # Get cursor
        cur = self.db.con.cursor()

        # Drop table
        drop_table_stmt = 'drop table if exists voter_history_summary'
        cur.execute(drop_table_stmt)

        # Construct table create statement -- election dates are unknown and then
        # create table
        election_dates = [f"'{x}'" + ' text' for x in list(df.columns)[5:]]
        create_table_stmt = f"""
            CREATE TABLE voter_history_summary
            (
                voter_id    text primary key,
                county_code text not null,
                race_id   text not null,
                gender  text not null,
                year_of_birth integer not null,
                {','.join(election_dates)}
            )
        """

        cur.execute(create_table_stmt)
        self.db.con.commit()

        # Construct insert statement then insert records
        column_count = len(election_dates) + 5
        insert_stmt = f"""
        insert into voter_history_summary values ({','.join(['?'] * column_count)})
        """
        df.apply(lambda row: cur.execute(insert_stmt, [row[i] for i in range(0, column_count)]), axis=1)
        self.db.con.commit()

        # Add a county code index
        create_index_stmt = 'CREATE INDEX voter_history_summary_county_code_idx ON voter_history_summary (county_code)'
        cur.execute(create_index_stmt)
        self.db.con.commit()

    def score_voters(self):
        vhs = self.db.get_voter_history_summary()
        tt = list(map(lambda x: 'p' if x.find('-11-') == -1 else 'g', vhs.columns[5:14]))

        def f(row):
            k = []
            for t, p in zip(tt, row[5:]):
                if p == 'X':
                    k.append((p + t).upper())
                elif p is not None:
                    k.append(p)
            return k

        pp = re.compile(r'R|D|XP')
        vhs = vhs.assign(ops=vhs.apply(lambda row: f(row), axis=1))
        vhs = vhs.assign(max_ballots_cast=vhs.ops.apply(lambda x: len(x)))
        vhs = vhs.assign(ballots_cast=vhs.ops.apply(lambda ops: sum([not x.startswith('X') for x in ops])))
        vhs = vhs.assign(gn_max=vhs.ops.apply(lambda ops: sum([x.endswith('G') for x in ops])))
        vhs = vhs.assign(pn_max=vhs.ops.apply(lambda ops: sum([pp.fullmatch(x) is not None for x in ops])))
        vhs = vhs.assign(gn=vhs.ops.apply(lambda ops: sum([x == 'G' for x in ops])))
        vhs = vhs.assign(rn=vhs.ops.apply(lambda ops: sum([x == 'R' for x in ops])))
        vhs = vhs.assign(dn=vhs.ops.apply(lambda ops: sum([x == 'D' for x in ops])))
        vhs = vhs.assign(gr=(vhs.gn / vhs.gn_max).fillna(0))
        vhs = vhs.assign(pr=((vhs.rn + vhs.dn) / vhs.pn_max).fillna(0))
        vhs = vhs.assign(ra=((vhs.rn / vhs.pn_max) - (vhs.dn / vhs.pn_max) + 1) / 2)
        vhs = vhs[['voter_id', 'county_code', 'max_ballots_cast', 'ballots_cast',
                   'gn_max', 'pn_max', 'gn', 'rn', 'dn', 'gr', 'pr', 'ra']]

        # Get cursor
        cur = self.db.con.cursor()

        # Drop table
        drop_voter_score_stmt = 'drop table if exists voter_score'
        cur.execute(drop_voter_score_stmt)

        # Construct table create statement and  create table
        create_voter_score_stmt = f"""
             CREATE TABLE voter_score
             (
                 voter_id    text primary key,
                 county_code text not null,
                 max_ballots_cast   integer not null,
                 ballots_cast  integer not null,
                 gn_max integer not null,
                 pn_max integer not null,
                 gn integer not null,
                 rn integer not null,
                 dn integer not null,
                 gr real,
                 pr real,
                 ra real
             )
         """

        cur.execute(create_voter_score_stmt)
        self.db.con.commit()

        # Construct insert statement then insert records
        insert_stmt = f"""
         insert into voter_score values (?,?,?,?,?,?,?,?,?,?,?,?)
         """
        vhs.apply(lambda row: cur.execute(insert_stmt, [row[i] for i in range(0, 12)]), axis=1)
        self.db.con.commit()

        # Add a county code index
        create_index_stmt = 'CREATE INDEX voter_score_county_code_idx ON voter_score (county_code)'
        cur.execute(create_index_stmt)
        self.db.con.commit()
