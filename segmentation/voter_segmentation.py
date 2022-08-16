import pandas as pd
from datetime import datetime, timedelta
import time
from voter_history.models import VoterHistory
from voter.models import Voter


class VoterSegmentation:
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

    THIRTY_DAYS = timedelta(30)
    MOST_RECENT_ELECTION_DATE = datetime.strptime('20220524', '%Y%m%d')

    def __init__(self, voters):
        self.voters = voters
        self.voter_ids = [v.voter_id for v in voters]  # works with voter ids for now
        self.edition = voters[0].edition

    def history_for_election_date(self, election_date, is_primary):
        date = datetime.strptime(election_date, '%Y%m%d')
        df = VoterHistory.objects.get_for(date, self.voter_ids)
        # Assume NaN means voter cast ballot in non-partisan ballot.
        # Party, for those that didn't vote is set to X.
        # Ensure the party, if NaN, is set to NP for primary and
        # G for general election.
        if is_primary:
            df = df.assign(party=df.loc[:, 'party'].fillna('NP'))
            # Select only general election primary. Sometimes there is
            # a special election entry as well as a general election
            # primary entry.
            df = df[df.type == '001'].reset_index(drop=True)
            df = df.assign(type='P')
        else:
            # Select only general election. Sometimes there is
            # a special election entry as well as a general election
            # entry.
            df = df[df.type == '003'].reset_index(drop=True)
            df = df.assign(type='G')
            # Set party in general election to G
            df = df.assign(party='G')

        df = df[['county_code', 'voter_id', 'type', 'party']]
        df = df.assign(date=date)
        return df

    def gather_history(self):
        elections = []
        for election in self.ELECTIONS:
            election_date = election['date']
            is_primary = election['is_primary']
            elections.append(self.history_for_election_date(election_date, is_primary))
        return pd.concat(elections)

    @staticmethod
    def get_county_info(df):
        """
        Given some voter history, return a data frame with the
        voter id and county if for the last election the voter
        participated in.
        :param df: voter history
        :return: voter id and county
        """
        return df[['voter_id', 'county_code', 'date']].sort_values('date') \
            .drop_duplicates(['voter_id'], keep='last').drop(columns=['date']).reset_index(drop=True)

    @staticmethod
    def get_first_last(df):
        """
        Given some voter history, return a data frame with the
        voter id and first and last dates of elections the voter
        has participated in.
        :param df: voter history
        :return: voter id, first election date, last election date
        """
        df_first = df[['voter_id', 'date']].sort_values('date') \
            .drop_duplicates(['voter_id'], keep='first')
        df_first.columns = ['voter_id', 'first_vote']
        df_last = df[['voter_id', 'date']].sort_values('date') \
            .drop_duplicates(['voter_id'], keep='last')
        df_last.columns = ['voter_id', 'last_vote']
        return df_first.merge(df_last, on='voter_id', how='inner').reset_index(drop=True)

    @staticmethod
    def add_missing_records(df):
        """
        We are going to create placeholder records for
        each election for each voter. First drop the stuff
        I no longer need. When I merge the placeholders
        with voter history, I end up with a record for every election
        for every voter. If the voter did not cast a ballot in that
        election then party will be NaN.  I set party to X (i.e.,
        don't care) if it is NaN. Next, I append the type
        to the party and drop the type. I do this, so I can distinguish
        between primary and general election don't care (Xs).
        Lastly, I replace NPP with NP where Non-partisan ballot
        was indicated with NP (the norm) versus N
        :param df: voter history
        :return: voter history for every voter for every election
        """
        # Drop all but voter_id, date, type, and party.
        df = df[['voter_id', 'date', 'type', 'party']]

        election_date_types = df[['date', 'type']].drop_duplicates()
        election_date_types = election_date_types.assign(key=0)
        voters = df[['voter_id']].drop_duplicates()
        voters = voters.assign(key=0)
        placeholders = voters.merge(election_date_types, on='key', how='outer').drop(columns=['key']) \
            .reset_index(drop=True)
        df = df.merge(placeholders, on=['voter_id', 'date', 'type'], how='right').reset_index(drop=True)
        df = df.assign(party=df.loc[:, 'party'].fillna('X'))
        df = df.assign(party=df.party + df.type).drop(columns=['type'])
        return df.assign(party=df.party.str.replace('NPP', 'NP'))

    def get_date_added(self):
        """
        Get the date added for voters from the voter list
        for active voters. Cleanup any bad dates. Add
        30 days to the date added to simplify comparison
        to election date and reflect voter has to be registered
        30 days prior to election.
        :return: voter id and date added
        """
        voters = Voter.objects.active().filter(voter_id__in=self.voter_ids).distinct('voter_id')
        records = [{'voter_id': v.voter_id, 'date_added': v.date_added} for v in voters]
        df = pd.DataFrame.from_records(records)
        # Add 30 days to date added -- registration has to be far enough in
        # advance so person can vote. If the resulting date is
        # greater than election date then, voter can't cast ballot.
        return df.assign(date_added=df.date_added + self.THIRTY_DAYS)

    @staticmethod
    def pivot(df, df_first_last, df_date_added):
        """
        I want to create a pivot table with voter id and election dates as
        columns. Any election date that is not an available voting opportunity
        for the voter should have a NaN as its value. Any election date
        that is an available voting opportunity, but in which the voter
        did not vote will have an XP (missed primary) or XG (missed general)
        value. I filter out the non-opportunities using date added, first
        and last. If there is no date added, then anything after the first
        observed vote is classified as an opportunity.
        I filter out all the non-opportunities and then pivot.
        Any election column with an NaN is a non-opportunity.
        Last I convert election date columns back to string.
        :param df: voter history so far
        :param df_first_last: first and last observed vote dates
        :param df_date_added: date voter added to the voter list
        :return: data frame with voter id and column for each date - date column
        have party as value
        """
        # merge everything together
        df = df_date_added.merge(df, on=['voter_id'], how='right')
        df = df.merge(df_first_last, on='voter_id', how='inner')

        # fill in party in the last observed voting date with '--', where
        # the voter is no longer in the voting list
        no_date_added_filler_mask = df.date_added.isna() & (df.last_vote < df.date)
        df = df.assign(party=df.party.where(~no_date_added_filler_mask, other='--'))

        # Select the records if election date is on or after date_added or
        # first_vote
        no_date_added_mask = df.date_added.isna() & (df.first_vote <= df.date)
        date_added_mask = df.date_added <= df.date
        df = df[no_date_added_mask | date_added_mask]

        # drop the unneeded columns
        df = df.drop(columns=['date_added', 'first_vote', 'last_vote'])
        df = df.pivot(index='voter_id', columns="date", values="party")
        # Convert index, which is voter_id, to column (in column 0).
        df = df.reset_index()
        # Convert date column names to str -- voter_id, date1, ..., dateN
        columns = list(map(lambda x: x.strftime('%Y-%m-%d'), list(df.columns)[1:]))
        columns.insert(0, "voter_id")

        # set column names
        df.columns = columns
        return df

    @staticmethod
    def add_county(df, df_county):
        columns = df.columns
        columns = columns.insert(1, "county_code")
        df = df.merge(df_county, on=['voter_id'], how='inner')
        return df[columns]

    def history_summary(self):
        """
        Return a record for each voter with the voter's voting history,
        where each election is represented by an election date string
        (i.e., YYYY-MM-DD). The value indicates the voter's behavior
         in each:
        * GG - voted in general election
        * XG - did not vote in general election
        * RP - pulled a republican party ballot in a primary
        * DP - pulled a democratic party ballot in a primary
        * NP - pulled a non-partisan ballot in a primary
        * XP - did not vote in primary
        * NaN - was not registered to vote

        The county in which the voter cast a ballot
        is also included.

        :return: voter history
        """
        start = time.perf_counter()
        df = self.gather_history()
        end = time.perf_counter()
        print(f'Gather time: {end - start:.1f}')
        start = end
        # save the county info -- I will need it for later. Use the most
        # recent county.
        df_county = self.get_county_info(df)
        end = time.perf_counter()
        print(f'County Info Time: {end - start:.1f}')
        start = end

        # Get the first and last vote date -- I will need this later.
        df_first_last = self.get_first_last(df)
        end = time.perf_counter()
        print(f'First/Last Time: {end - start:.1f}')
        start = end

        # Add missing records -- I have a record for casting a
        # ballot and not casting a ballot.
        df = self.add_missing_records(df)
        end = time.perf_counter()
        print(f'Add Missing Records Time: {end - start:.1f}')
        start = end

        # Create a voting record for each voter with a column for
        # each election
        df = self.pivot(df, df_first_last, self.get_date_added())
        end = time.perf_counter()
        print(f'Pivot Time: {end - start:.1f}')
        start = end

        # add county info
        df = self.add_county(df, df_county)
        end = time.perf_counter()
        print(f'Add County Time: {end - start:.1f}')

        return df

    @staticmethod
    def compute_ops(vhs):
        return vhs.assign(ops=vhs.apply(lambda row: [p for p in row[2:] if pd.notna(p)], axis=1))

    def score_voters(self, df):
        start = time.perf_counter()
        end_time = time.perf_counter()
        print(f'Load Voter History Summary Time: {end_time - start:.1f}')
        start = end_time
        df = self.compute_ops(df)
        end_time = time.perf_counter()
        print(f'Compute Ops Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(max_ballots_cast=df.ops.apply(lambda x: len(x)))
        end_time = time.perf_counter()
        print(f'Compute max_ballots_cast Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(ballots_cast=df.ops.apply(lambda ops: sum([not x.startswith('X') for x in ops])))
        end_time = time.perf_counter()
        print(f'Compute ballots_cast Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(gn_max=df.ops.apply(lambda ops: sum([x.endswith('G') for x in ops])))
        end_time = time.perf_counter()
        print(f'Compute gn_max Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(pn_max=df.ops.apply(lambda ops: sum([x.endswith('P') for x in ops])))
        end_time = time.perf_counter()
        print(f'Compute pn_max Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(gn=df.ops.apply(lambda ops: sum([x == 'GG' for x in ops])))
        end_time = time.perf_counter()
        print(f'Compute gn Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(rn=df.ops.apply(lambda ops: sum([x == 'RP' for x in ops])))
        end_time = time.perf_counter()
        print(f'Compute rn Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(dn=df.ops.apply(lambda ops: sum([x == 'DP' for x in ops])))
        end_time = time.perf_counter()
        print(f'Compute dn Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(gr=(df.gn / df.gn_max).fillna(0))
        end_time = time.perf_counter()
        print(f'Compute gr Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(pr=((df.rn + df.dn) / df.pn_max).fillna(0))
        end_time = time.perf_counter()
        print(f'Compute pr Time: {end_time - start:.1f}')
        start = end_time
        df = df.assign(ra=((df.rn / df.pn_max) - (df.dn / df.pn_max) + 1) / 2)
        end_time = time.perf_counter()
        print(f'Compute ra Time: {end_time - start:.1f}')
        start = end_time
        df = df[['voter_id', 'county_code', 'max_ballots_cast', 'ballots_cast',
                 'gn_max', 'pn_max', 'gn', 'rn', 'dn', 'gr', 'pr', 'ra']]
        end_time = time.perf_counter()
        print(f'Reorder Time: {end_time - start:.1f}')
        return df
