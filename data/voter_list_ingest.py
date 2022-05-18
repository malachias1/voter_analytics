import re

import numpy as np
import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb
from util.addresses import StreetNameNormalizer
import sqlite3 as sql

class IngestVoterList(Pathes):
    ZIPCODE = re.compile(r'(\d\d\d\d\d)(?:-)?(\d\d\d\d)?')

    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)
        self.sn = StreetNameNormalizer()

    @property
    def con(self):
        return self.db.con

    def as_residence_address_key(self, row):
        return self.db.as_residence_address_key(row)

    def as_mailing_address_key(self, row):
        return self.db.as_mailing_address_key(row)

    def get_mailing_addresses(self):
        return self.db.get_mailing_addresses()

    def get_residence_addresses(self, county_code):
        return self.db.get_residence_addresses(county_code)

    def get_voter_names(self):
        return self.db.get_voter_names()

    def get_voter_status(self):
        return self.db.get_voter_status()

    def get_voter_demographics(self):
        return self.db.get_voter_demographics()

    def get_address_voter(self):
        return self.db.get_residence_address_voter()

    def get_mailing_address_voter(self):
        return self.db.get_mailing_address_voter()

    def read_csv(self, county_code, edition='latest'):
        p = self.voter_list_path(county_code, edition)
        df = pd.read_csv(p, dtype=str).fillna('')
        df = df.assign(residence_street_name=df.residence_street_name.apply(self.sn.normalize),
                       mail_street_name=df.mail_street_name.apply(self.sn.normalize))
        return df

    @classmethod
    def zip_plus4(cls, zipcode):
        m = cls.ZIPCODE.match(zipcode)
        if m is None:
            return '', ''
        return m.group(1), m.group(2) if m.group(2) is not None else ''

    def as_residence_address(self, df):
        df = df[['county_code',
                 'residence_house_number',
                 'residence_street_name',
                 'residence_apt_unit_nbr',
                 'residence_city',
                 'residence_zipcode',
                 ]]
        df.columns = ['county_code',
                      'house_number',
                      'street_name',
                      'apt_no',
                      'city',
                      'zipcode',
                      ]
        zip_plus4 = pd.DataFrame.from_records(df['zipcode'].apply(self.zip_plus4))
        idx = df.columns.get_loc('zipcode')
        df.insert(idx, 'state', self.state.upper())
        df = df.assign(zipcode=zip_plus4[0], plus4=zip_plus4[1], lat=None, lon=None)
        return df.assign(key=df.apply(self.as_residence_address_key, axis=1))

    # noinspection PyUnresolvedReferences
    def as_mailing_address(self, df):
        """
        Mailing address are different than residence addresses.
        Residence addresses are in Georgia whereas mailing addresses
        could be international. Only voters with a mailing address
        different from the residence address will have a mailing
        address.

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
        # Drop any row without a city -- no mailing address
        # Reset the index. If I don't the from_records won't work
        # because it expects indexes to range from 0 to len(df).
        # Lastly, drop the index; otherwise, it gets inserted into
        # column 0.
        df = df[df['city'] != ''].reset_index(drop=True)

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
            df = self.read_csv(c.name, edition)
            self.ingest_voter_name(df)

    def ingest_county_voter_list(self, county_code, edition='latest'):
        df = self.read_csv(county_code, edition)
        self.ingest_voter_name(df)
        self.ingest_voter_status(df)
        self.ingest_voter_demographics(df)
        self.ingest_residence_address(county_code, df)
        self.ingest_mailing_address(df)
        self.ingest_address_voter_relationship(county_code, df)
        self.ingest_mailing_address_relationship(df)
        self.ingest_address_district_relationship(county_code, df)

    def ingest_voter_name(self, df):
        df1 = df.copy()
        df1 = df1[['voter_id', 'last_name', 'first_name',
                   'middle_maiden_name', 'name_suffix',
                   'name_title']]
        df1.columns = ['voter_id', 'last_name', 'first_name',
                       'middle_name', 'name_suffix',
                       'name_title']
        df2 = self.get_voter_names()
        df2 = df2.loc[df2['voter_id'].isin(df1['voter_id'])]

        df = pd.concat([df1, df2])
        updates = df.drop_duplicates(['voter_id', 'last_name', 'first_name',
                                      'middle_name', 'name_suffix',
                                      'name_title'], keep='first')
        self.replace_voter_name(updates)
        self.con.commit()

    def ingest_voter_status(self, df):
        df1 = df.copy()
        df1 = df1[['voter_id', 'voter_status', 'status_reason',
                   'date_added', 'date_changed',
                   'registration_date', 'last_contact_date']]
        df1.columns = ['voter_id', 'status', 'status_reason',
                       'date_added', 'date_changed',
                       'registration_date', 'last_contact_date']
        df2 = self.get_voter_status()
        df2 = df2.loc[df2['voter_id'].isin(df1['voter_id'])]

        df = pd.concat([df1, df2])
        updates = df.drop_duplicates(['voter_id'], keep='first')
        self.replace_voter_status(updates)
        self.con.commit()

    def ingest_voter_demographics(self, df):
        df1 = df.copy()
        df1 = df1[['voter_id', 'race_id', 'gender', 'year_of_birth']]
        df2 = self.get_voter_demographics()
        df2 = df2.loc[df2['voter_id'].isin(df1['voter_id'])]

        df = pd.concat([df1, df2])
        updates = df.drop_duplicates(['voter_id'], keep='first')
        self.replace_voter_demographics(updates)
        self.con.commit()

    def ingest_address_relationship(self, county_code, column, df, table_name=None):
        """
        Insert or update rows linking column to residence addresses.

        An address is the defining characteristic that results in a voter's
        inclusion or exclusion from a district. Thus, I link addresses
        to districts and link voters to addresses. When determining linkages,
        I include only those addresses that are in a given county, which
        greatly reduces the number of addresses that need to be
        considered at one time.

        :param county_code: Georgia county identifier, 3 digits, zero filled
        :param column: the name of the column with which to form a relationsip with address
        :param df: a voter list data frame
        :param table_name: optional table name if table name cannot be constructed from column (e.g., voter_id)
        :return: the number of inserted or updated rows
        """

        # Get known addresses from the database.
        # Construct an old address dataframe consisting
        # of just the address_id and key
        old_addresses = self.get_residence_addresses(county_code)
        #old_addresses = old_addresses[['address_id', 'key']]

        # Construct a dataframe consisting of the residence address portion
        # of the voter list. Insert the specified district type.
        # Construct a new address dataframe consisting
        # of just the column and key
        new_addresses = self.as_residence_address(df)
        new_addresses.insert(0, column, df[column])
        new_addresses = new_addresses[[column, 'key']]
        new_addresses = new_addresses[new_addresses[column] != '']
        if len(new_addresses) > 0:
            # join new addresses and old addresses on key and save the
            # result
            updates = new_addresses.merge(old_addresses, how='inner', on='key')
            # Construct a statement from the district type
            # Replace
            if table_name is None:
                table_name = f'address_{column}'
            stmt = f'replace into {table_name} values (?,?)'
            cur = self.con.cursor()
            errors = updates[updates.address_id.isna()]
            if len(errors) > 0:
                print(errors)
            updates[['address_id', column]].apply(lambda row: cur.execute(stmt, (row[0], row[1])), axis=1)
            self.con.commit()
            return len(updates)
        return 0

    def ingest_address_voter_relationship(self, county_code, df):
        self.ingest_address_relationship(county_code, 'voter_id', df, table_name='address_voter')

    def ingest_address_district_relationship(self, county_code, df):
        district_types = [
            'land_district',
            'land_lot',
            'precinct_id',
            'city_precinct_id',
            'cng',
            'sen',
            'hse',
            'jud',
            'com',
            'sch',
            'county_districta_name',
            'county_districta_value',
            'county_districtb_name',
            'county_districtb_value',
            'municipal_name',
            'municipal_code',
            'ward_city_council_name',
            'ward_city_council_code',
            'city_school_district_name',
            'city_school_district_value',
            'city_dista_name',
            'city_dista_value',
            'city_distb_name',
            'city_distb_value',
            'city_distc_name',
            'city_distc_value',
            'city_distd_name',
            'city_distd_value'
        ]
        """
        Insert or update rows linking column to residence addresses.

        An address is the defining characteristic that results in a voter's
        inclusion or exclusion from a district. Thus, I link addresses
        to districts and link voters to addresses. When determining linkages,
        I include only those addresses that are in a given county, which
        greatly reduces the number of addresses that need to be
        considered at one time.

        :param county_code: Georgia county identifier, 3 digits, zero filled
        :param column: the name of the column with which to form a relationsip with address
        :param df: a voter list data frame
        :return: None
        """

        for dt in district_types:
            self.ingest_address_relationship(county_code, dt, df)

    def ingest_mailing_address_relationship(self, df):
        """
        Insert or update rows linking voter_id to mailing addresses.

        :param df: a voter list data frame
        :return: the number of inserted or updated rows
        """

        # Get known addresses from the database.
        # Construct an old address dataframe consisting
        # of just the address_id and key
        old_addresses = self.get_mailing_addresses()
        old_addresses = old_addresses[['address_id', 'key']]

        # Construct a dataframe consisting of the residence address portion
        # of the voter list. Insert the specified column.
        # Construct a new address dataframe consisting
        # of just the column and key
        new_addresses = self.as_mailing_address(df)
        new_addresses.insert(0, 'voter_id', df['voter_id'])
        new_addresses = new_addresses[['voter_id', 'key']]
        # join new addresses and old addresses on key and save the
        # result
        updates = old_addresses.merge(new_addresses, how='inner', on='key')
        self.replace_voter_mailing_address(updates[['address_id', 'voter_id']])

        return len(updates)

    def ingest_mailing_address(self, df):
        df = df.copy()
        df = self.as_mailing_address(df)
        df.insert(0, 'address_id', np.NaN)
        df.drop_duplicates(['house_number', 'city', 'zipcode', 'street_name', 'apt_no', 'plus4'],
                           inplace=True)
        dfdb = self.get_mailing_addresses()
        df = pd.concat([dfdb, df])
        df.drop_duplicates(['house_number', 'city', 'zipcode', 'street_name', 'apt_no', 'plus4'], keep='first',
                           inplace=True)
        df = df[df['city'] != '']
        if len(df) > 0:
            df = df[pd.isna(df['address_id'])]
            next_address_id = self.db.get_mailing_address_id_max() + 1
            df = df.assign(address_id=range(next_address_id, next_address_id + len(df)))

            self.insert_mailing_address(df)

    def ingest_residence_address(self, county_code, df):
        df = df.copy()
        check = df.county_code.unique()
        if len(check) > 1:
            raise ValueError('More than one county code in voter list!')
        if check[0] != county_code:
            raise ValueError('County code in voter list different than that given!')

        df = self.as_residence_address(df)
        df.insert(0, 'address_id', np.NaN)
        df.drop_duplicates(['house_number', 'city', 'zipcode', 'street_name', 'apt_no', 'plus4'],
                           inplace=True)
        dfdb = self.get_residence_addresses(county_code)
        df = pd.concat([dfdb, df])
        df.drop_duplicates(['house_number', 'city', 'zipcode', 'street_name', 'apt_no', 'plus4'], keep='first',
                           inplace=True)
        if len(df) > 0:
            df = df[pd.isna(df['address_id'])]
            next_address_id = self.db.get_residence_address_id_max() + 1
            df = df.assign(address_id=range(next_address_id, next_address_id + len(df)))

            self.insert_residence_address(df)

    def replace_voter_residence_address(self, df):
        stmt = f'replace into address_voter values (?,?)'
        cur = self.con.cursor()
        df[['address_id', 'voter_id']].apply(lambda row: cur.execute(stmt, (row[0], row[1])), axis=1)
        self.con.commit()

    @classmethod
    def replace_voter_mailing_address_row(cls, cur, row):
        stmt = f'replace into mailing_address_voter values (?,?)'
        try:
            cur.execute(stmt, (row[0], row[1]))
        except sql.IntegrityError as e:
            print(row[0], row[1])
            raise e

    def replace_voter_mailing_address(self, df):
        cur = self.con.cursor()
        df[['address_id', 'voter_id']].apply(lambda row: self.replace_voter_mailing_address_row(cur, row), axis=1)
        self.con.commit()

    def replace_voter_name(self, df):
        stmt = f'replace into voter_name values (?, ?, ?, ?, ?, ?)'
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 6)]), axis=1)
        self.con.commit()

    def replace_voter_status(self, df):
        stmt = f'replace into voter_status values (?, ?, ?, ?, ?, ?, ?)'
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 7)]), axis=1)
        self.con.commit()

    def replace_voter_demographics(self, df):
        stmt = f'replace into voter_demographics values (?, ?, ?, ?)'
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 4)]), axis=1)
        self.con.commit()

    def insert_residence_address(self, df):
        stmt = f"""
        insert into residence_address ('address_id',
                       'county_code',
                       'house_number',
                       'street_name',
                       'apt_no',
                       'city',
                       'state',
                       'zipcode',
                       'plus4') 
                       values (?,?,?,?,?,?,?,?,?)
        """
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 9)]), axis=1)
        self.con.commit()

    def insert_mailing_address(self, df):
        stmt = f"""
        insert into mailing_address values (?,?,?,?,?,?,?,?,?,?,?)
        """
        cur = self.con.cursor()
        df.apply(lambda row: cur.execute(stmt, [row[i] for i in range(0, 11)]), axis=1)
        self.con.commit()
