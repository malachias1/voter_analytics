from data.voterdb import VoterDb
from data.voter_list_ingest import IngestVoterList
from util.addresses import StreetNameNormalizer
from util.names import NameNormalizer
import pandas as pd
import numpy as np


class VoterMatch:
    COLUMNS = ['voter_id', 'last_name',
               'first_name', 'house_number',
               'street_name', 'apt_no', 'city', 'zipcode',
               'plus4', 'county_code', 'precinct_id', 'cng',
               'sen', 'hse']

    def __init__(self, root_dir):
        self.sn = StreetNameNormalizer()
        self.nn = NameNormalizer()
        self.db = VoterDb(root_dir)
        self.matched_rows = None
        self.unmatchable = None
        self.matchable_rows = None
        self.name_idx = None
        self.first_name_idx = None
        self.last_name_idx = None
        self.street_address_idx = None
        self.zipcode_idx = None

    def match(self, df, strategy):
        self.matched_rows = pd.DataFrame(columns=self.COLUMNS, dtype=str)
        # Remove clearly unmatchable records that are missing
        # essential information
        mask = (df.zipcode.isna() |
                df.street_address.apply(lambda x: not self.sn.is_street_address(x)))
        if strategy.startswith('name'):
            self.name_idx = df.columns.get_loc('name')
            mask = mask | df.name.isna()
        elif strategy.startswith('split name'):
            self.last_name_idx = df.columns.get_loc('last_name')
            self.first_name_idx = df.columns.get_loc('first_name')
            mask = mask | df.first_name.isna() | df.last_name.isna()
        self.unmatchable = df[mask]

        # Get indexes for match columns
        self.street_address_idx = df.columns.get_loc('street_address')
        self.zipcode_idx = df.columns.get_loc('zipcode')

        # Get a dataframe of potentially matchable columns
        self.matchable_rows = df[~mask].reset_index(drop=True)
        self.matchable_rows = self.matchable_rows.assign(reason=np.NaN)
        n = len(self.matchable_rows)
        for i in range(0, n):
            row = self.matchable_rows.iloc[i]
            self.match_row(i, row, strategy=strategy)
        match_count = len(self.matched_rows)
        self.matched_rows = self.matched_rows.drop_duplicates()
        duplicate_count = match_count - len(self.matched_rows)
        return (self.matched_rows, self.matchable_rows[self.matchable_rows.reason.notna()],
                self.unmatchable, duplicate_count)

    def update_reason(self, i, msg):
        priors = self.matchable_rows.at[i, 'reason']
        priors = priors.split(',') if pd.notna(priors) else []
        priors.append(msg)
        priors = ','.join(priors)
        self.matchable_rows.at[i, 'reason'] = priors

    def match_row(self, i, row, strategy='name'):
        """
        Try to match a row. Sometimes the given first name is really the middle
        name. If retry_middle is true try matching the middle and last name.
        :param i: index into matchable rows
        :param row:
        :param strategy:
        :return: number of matches
        """
        # Get first and last name from normalized name.
        # If the normalized name is one word, skip it.
        # This approach generally does not work
        # for names with leading "Van" or "Van Der". The voter
        # list does not have a standard.
        # normalize the name. normalize returns a tuple
        # of names because sometimes names are entered as "Bob and Kathy".
        split_names = []
        if strategy.startswith('name'):
            names = self.nn.normalize(row[self.name_idx])
            for n in names:
                parts = n.split()
                if len(parts) < 2:
                    self.update_reason(i, f'{n} is not a first and last name')
                    return 0
                split_names.append((parts[0], parts[-1]))
        elif strategy.startswith('split name'):
            first_names = self.nn.normalize(row[self.first_name_idx])
            last_name = self.nn.normalize(row[self.last_name_idx])
            for n in first_names:
                split_names.append((n, last_name[-1]))
        else:
            raise ValueError(f'Unknown strategy {strategy}')
        retry_middle = strategy.endswith('retry middle')
        count = 0
        for k, (first_name, last_name) in enumerate(split_names):
            success, msg = self.match_row_for_name(i, row, first_name, last_name, retry_middle=retry_middle)
            if not success:
                self.update_reason(i, msg)
            else:
                count += 1
        return count

    def match_row_for_name(self, i, row, first_name, last_name, retry_middle=False):
        """
        Try to match a row. Sometimes the given first name is really the middle
        name. If retry_middle is true try matching the middle and last name.
        :param i: index into matchable rows
        :param row:
        :param first_name: a voter first name
        :param last_name: a voter last name
        :param retry_middle:
        :return: success and a message if fail
        """
        # Match first, last name, zip code, and house number.
        # Name matching is complicated by the fact that
        # the first name is not always the one found
        # in the voter list (e.g., Rick versus Richard)
        zipcode = row[self.zipcode_idx]
        street_address = row[self.street_address_idx]
        if retry_middle:
            matches = self.db.get_voter_by_middle_name(first_name, last_name)
        else:
            matches = self.db.get_voter_by_name(first_name, last_name)

        if len(matches) > 0:
            for j in range(len(matches)):
                voter_id = matches.iloc[j, 0]
                address = self.db.get_residence_address_for_voter(voter_id)
                zipcode, _ = IngestVoterList.zip_plus4(zipcode)
                if zipcode != address.zipcode.iloc[0]:
                    continue
                house_number, street_name = self.sn.street_address_split(street_address)
                if house_number != address.house_number.iloc[0]:
                    continue
                record = {
                    'voter_id': voter_id,
                    'last_name': last_name,
                    'first_name': first_name,
                    'house_number': house_number,
                    'street_name': address.street_name.iloc[0],
                    'apt_no': address.apt_no.iloc[0],
                    'city': address.city.iloc[0], 'zipcode': address.zipcode.iloc[0],
                    'plus4': address.plus4.iloc[0],
                    'county_code': address.county_code.iloc[0],
                    'precinct_id': self.db.get_voter_precinct_id(voter_id),
                    'cng': self.db.get_voter_cng(voter_id),
                    'sen': self.db.get_voter_sen(voter_id),
                    'hse': self.db.get_voter_hse(voter_id)
                }
                self.matched_rows = pd.concat([self.matched_rows, pd.Series(record).to_frame().T])
                return True, None
            return False, f'No matching zip code or house number for {first_name} {last_name}'
        else:
            return False, f'No voters matching {first_name} {last_name}'
