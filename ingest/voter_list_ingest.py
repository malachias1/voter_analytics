import re

import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from util.addresses import StreetNameNormalizer


class IngestVoterList(Pathes):
    ZIPCODE = re.compile(r'(\d\d\d\d\d)-?(\d\d\d\d)?')

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb()
        self.sn = StreetNameNormalizer()

    @property
    def voter_names(self):
        return self.db.voter_names

    @property
    def voter_status(self):
        return self.db.voter_status

    @property
    def voter_demographics(self):
        return self.db.voter_demographics

    def read_csv(self, county_code, edition='latest'):
        p = self.voter_list_path(county_code, edition)
        df = pd.read_csv(p, dtype=str).fillna('')
        df = df.assign(residence_street_name=df.residence_street_name.apply(self.sn.normalize),
                       mail_street_name=df.mail_street_name.apply(self.sn.normalize))
        return df

    @classmethod
    def zip_plus4(cls, zipcode):
        if zipcode == '':
            return '', ''
        m = cls.ZIPCODE.match(zipcode)
        if m is None:
            return '', ''
        return m.group(1), m.group(2) if m.group(2) is not None else ''

    # noinspection PyUnresolvedReferences
    def as_mailing_address(self, df):
        """
        Mailing address are different from residence addresses.
        Residence addresses are in Georgia whereas mailing addresses
        could be international. Only voters with a mailing address
        different from the residence address will have a mailing
        address. It is the responsibility of the caller to remove
        addresses that are blank.
        :param df: a voter list
        :return: dataframe of mailing address of voter
        """
        df = df[['mail_house_nbr',
                 'mail_street_name',
                 'mail_apt_unit_nbr',
                 'mail_city',
                 'mail_state',
                 'mail_zipcode',
                 'mail_address_2',
                 'mail_address_3',
                 'mail_country'
                 ]]
        df.columns = ['house_number',
                      'street_name',
                      'apt_no',
                      'city',
                      'state',
                      'zipcode',
                      'address_line2',
                      'address_line3',
                      'country'
                      ]
        zip_plus4 = pd.DataFrame.from_records(df['zipcode'].apply(self.zip_plus4))
        # Columns need to be in a particular order. Zip code is
        # in the right place, but I need to insert plus4 after
        # zip code.
        df = df.assign(zipcode=zip_plus4[0])
        idx = df.columns.get_loc('zipcode')
        df.insert(idx + 1, 'plus4', zip_plus4[1])

        return df.assign(key=df.apply(self.as_mailing_address_key, axis=1))

    def ingest(self, edition='latest'):
        for c in self.voter_list_dir.iterdir():
            if re.fullmatch(r'\d+', c.name):
                print(c.name, end=' ... ')
                self.ingest_county_voter_list(c.name, edition)
                print('Done')

    def ingest_county_voter_list(self, county_code, edition='latest'):
        df = self.read_csv(county_code, edition)
        self.ingest_voter_name(df)
        self.ingest_voter_status(df)
        self.replace_multiple_voter_demographics(df)
        # self.ingest_residence_address(county_code, df)
        # self.ingest_mailing_address(df)
        # self.ingest_address_voter_relationship(county_code, df)
        # self.ingest_mailing_address_relationship(df)
        # self.replace_multiple_voter_cng(df)
        self.replace_multiple_voter_hse(df)
        self.replace_multiple_voter_sen(df)
        self.ingest_precinct_details(county_code, df)

    def ingest_precinct_details(self, county_code, df):
        """
        Precinct details are encompassed by a single table,
        precinct_details. Precincts are updated at the county
        level. All county precincts are purged and then new
        precinct details are inserted.

        :param county_code: the county in which precincts
        are to be updated
        :param df: a voter list with both address and precinct details
        :return:
        """
        # Run a brief integrity check
        check = df.county_code.unique()
        if len(check) > 1:
            raise ValueError('More than one county code in voter list!')
        if check[0] != county_code:
            raise ValueError('County code in voter list different than that given!')

        # Construct a dataframe consisting of unique county_code/precinct_id
        # combinations. Assign an id to each combination.
        # Replace county precinct details
        precinct_details = df[['county_code', 'precinct_id']].drop_duplicates()
        next_id = self.db.next_precinct_id
        precinct_details = precinct_details.assign(id=range(next_id, next_id + len(precinct_details.index)))
        self.replace_multiple_precinct_details(precinct_details)

        # Next, I create voter_id/id combinations, where id
        # is the same as id above. Need to work on names at some point.
        # First, create a dataframe with voter_id, county_code, and precinct_id.
        # Then I merge it with precinct details to get the id.
        # Finally, replace voter/precinct pairs
        voter_precinct = df[['voter_id', 'county_code', 'precinct_id']]
        voter_precinct = voter_precinct.merge(precinct_details, on=['county_code', 'precinct_id'], how='inner')
        voter_precinct = voter_precinct[['voter_id', 'id']].rename(columns={'id': 'precinct_id'})
        self.replace_multiple_voter_precinct(voter_precinct)
        return len(voter_precinct)

    def ingest_voter_name(self, df):
        df = df.rename(columns={'middle_maiden_name': 'middle_name'})
        self.replace_multiple_voter_name(df)

    def ingest_voter_status(self, df):
        df = df.rename(columns={'voter_status': 'status'})
        self.replace_multiple_voter_status(df)

    def ingest_voter_demographics(self, df):
        df1 = df.copy()
        df1 = df1[['voter_id', 'race_id', 'gender', 'year_of_birth']]
        df2 = self.voter_demographics
        df2 = df2.loc[df2['voter_id'].isin(df1['voter_id'])]

        df = pd.concat([df1, df2])
        updates = df.drop_duplicates(['voter_id'], keep='first')
        self.replace_multiple_voter_demographics(updates)

    def replace_multiple_precinct_details(self, df):
        self.db.replace_multiple_precinct_details(df)

    def replace_multiple_voter_demographics(self, df):
        self.db.replace_multiple_voter_demographics(df)

    def replace_multiple_voter_hse(self, df):
        self.db.replace_multiple_voter_hse(df)

    def replace_multiple_voter_name(self, df):
        self.db.replace_multiple_voter_name(df)

    def replace_multiple_voter_precinct(self, df):
        self.db.replace_multiple_voter_precinct(df)

    def replace_multiple_voter_sen(self, df):
        self.db.replace_multiple_voter_sen(df)

    def replace_multiple_voter_status(self, df):
        self.db.replace_multiple_voter_status(df)
