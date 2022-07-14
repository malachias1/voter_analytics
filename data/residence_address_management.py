from psycopg2.extras import execute_values
import pandas as pd
from data.address_management_base import AddressManagementBase
from data.voter_list import VoterList
import re
import numpy as np


class ResidenceAddressManagement(AddressManagementBase):
    def __init__(self):
        super().__init__('residence_address', 'address_voter')

    @property
    def addresses(self):
        results = self.fetchall(f"""
                    select address_id, county_code, house_number, street_name, apt_no, city, state,
                        zipcode, plus4, lat, lon
                        from residence_address
                """)
        return self.from_records(results)

    @property
    def columns(self):
        return [
            'address_id', 'county_code', 'house_number', 'street_name',
            'apt_no', 'city', 'state',
            'zipcode', 'plus4', 'lat', 'lon'
        ]

    @classmethod
    def as_address_key(cls, row, pos=0):
        """
        Construct a key for this address.
        * 0 -- county code (unused)
        * 1 -- street number
        * 2 -- street name
        * 3 -- apartment no. (optional)
        * 4 -- city
        * 5 -- state
        * 6 -- zipcode
        * 7 -- plus4 (optional)
        :param row: the parts of the address
        :param pos: starting position, pos is one if there is an
        address_id otherwise it is 0
        :return: a unique key for the address
        """
        address = f'{row[pos + 1]} {row[pos + 2]}'
        if len(row[pos + 3]) > 0:
            address += f' #{row[pos + 3]}'
        address += f'; {row[pos + 4]}'
        address += f' {row[pos + 5]}'
        address += f' {row[pos + 6]}'
        if len(row[pos + 7]) > 0:
            address += f'-{row[pos + 7]}'
        return address

    def get_address(self, address_id):
        results = self.fetchall(f"""
            select address_id, county_code, house_number, street_name, apt_no, city, state,
                zipcode, plus4, lat, lon
                from residence_address
                where address_id={address_id}
        """)
        return self.from_records(results)

    def get_addresses_for_county(self, county_code):
        results = self.fetchall(f"""
            select address_id, county_code, house_number, street_name, apt_no, city, state,
                zipcode, plus4, lat, lon
                from residence_address
                where county_code = '{county_code}'
        """)
        return self.from_records(results)

    def get_voter_address(self, voter_id):
        result = self.fetchone(f"""
            select address_id from address_voter where voter_id = '{voter_id}'
        """)
        address_id = result[0]
        return self.get_address(address_id)

    # -------------------------------------------------------------------------
    # Ingest and Update Methods
    # -------------------------------------------------------------------------
    def ingest(self, root_dir, edition='latest'):
        self.truncate()
        vl = VoterList(root_dir)
        county_names = list(filter(lambda d: re.fullmatch(r'\d+', d.name), vl.voter_list_dir.iterdir()))
        old_addresses = self.addresses
        for c in county_names:
            print(c.name, end=' details ... ')
            df = vl.read_csv(c, edition)
            updates = self.build_address_details(c, df)
            self.insert_details(updates)
            print('Done')
        old_addresses = self.addresses[['address_id', 'key']]
        for c in county_names:
            print(c.name, end=' links ... ')
            df = vl.read_csv(c, edition)
            updates = self.rebuild_voter_address_links(old_addresses, df)
            self.replace_links(updates)
            print('Done')

    def build_address_details(self, county_code, df):
        check = df.county_code.unique()
        if len(check) > 1:
            raise ValueError('More than one county code in voter list!')
        if check[0] != county_code:
            raise ValueError('County code in voter list different than that given!')

        df = VoterList.as_residence_address(df)
        df.insert(0, 'address_id', np.NaN)
        df = df.drop_duplicates(['house_number', 'city', 'zipcode', 'street_name', 'apt_no', 'plus4'])
        old_addresses = self.get_addresses_for_county(county_code)
        df = pd.concat([old_addresses, df])
        df = df.drop_duplicates(['house_number', 'city', 'zipcode', 'street_name', 'apt_no', 'plus4'], keep='first')
        updates = pd.DataFrame(columns=old_addresses.columns)
        if len(df.index) > 0:
            updates = df[pd.isna(df['address_id'])]
            next_address_id = self.next_address_id
            updates = updates.assign(address_id=range(next_address_id, next_address_id + len(updates.index)))
        return updates

    def insert_details(self, details):
        with self.con:
            with self.con.cursor() as cur:
                records = details[['address_id', 'county_code', 'house_number',
                                   'street_name', 'apt_no', 'city',
                                   'state', 'zipcode', 'plus4']].to_records(index=False)
                execute_values(cur, f"""
                        insert into residence_address (address_id, county_code, house_number, street_name,
                                         apt_no, city, state, zipcode, plus4)
                            values %s
                """, records)

    def rebuild_voter_address_links(self, county_code, df):
        """
        Link addresses to voter. When determining linkages,
        I include only those addresses that are in a given county, which
        greatly reduces the number of addresses that need to be
        considered at one time.

        :param county_code: Georgia county identifier, 3 digits, zero filled
        :param df: a voter list data frame
        :return: the number of inserted or updated rows
        """

        # Get known addresses from the database.
        # Construct an old address dataframe consisting
        # of just the address_id and key
        old_addresses = self.get_addresses_for_county(county_code)
        old_addresses = old_addresses[['voter_id', 'key']]

        # Construct a dataframe consisting of the mailing
        # address portion of the voter list. Add voter_id
        # and address key, then reduce the dataframe to just
        # voter_id and key.
        new_addresses = self.add_address_key(VoterList.as_residence_address(df))
        new_addresses.insert(0, 'voter_id', df['voter_id'])
        new_addresses = new_addresses[['voter_id', 'key']]
        new_addresses = new_addresses[new_addresses['voter_id'] != '']
        updates = pd.DataFrame(columns=old_addresses.columns)
        if len(new_addresses) > 0:
            # join new addresses and old addresses on key and save the
            # result
            updates = new_addresses.merge(old_addresses, how='inner', on='key')
        return updates

    def replace_links(self, new_addresses):
        """
        Replace (or insert) the residence addresses for
        voters. Delete any old addresses first (in the case
        of replace).
        :param new_addresses:
        :return: None
        """
        if 'voter_id' not in new_addresses.columns:
            raise ValueError('voter_id missing from addresses!')
        self.delete_voters_from_table(new_addresses, 'address_voter')
        records = new_addresses[['voter_id', 'address_id']].to_records(index=False)
        with self.con:
            with self.con.cursor() as cur:
                execute_values(cur, f"""
                    insert into address_voter (voter_id, address_id) values %s
                """, records)

