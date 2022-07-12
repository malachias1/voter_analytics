from psycopg2.extras import execute_values
import pandas as pd
from data.address_management_base import AddressManagementBase
from data.voter_list import VoterList
import re
import numpy as np


class MailingAddressManagement(AddressManagementBase):
    def __init__(self):
        super().__init__('mailing_address', 'mailing_address_voter')

    @property
    def addresses(self):
        results = self.fetchall(f"""
                    select address_id, house_number, street_name, apt_no, city, state,
                        zipcode, plus4, address_line2, address_line3, country
                        from mailing_address
                """)
        return self.from_records(results)

    @property
    def columns(self):
        return [
            'address_id', 'house_number', 'street_name',
            'apt_no', 'city', 'state',
            'zipcode', 'plus4', 'address_line2',
            'address_line3', 'country'
        ]

    @classmethod
    def as_address_key(cls, row, pos=0):
        """
        Construct a key for this address.
        * 0 -- street number
        * 1 -- street name
        * 2 -- apartment no. (optional)
        * 3 -- city
        * 4 -- state
        * 5 -- zipcode
        * 6 -- plus4 (optional)
        * 7 -- address line2 (optional)
        * 8 -- address line3 (optional)
        * 9 -- country (optional)
        :param row: the parts of the address
        :param pos: starting position, sometimes there is an address_id
        :return: a unique key for the address
        """
        # If the street name is missing, just
        # return an em
        if row[pos + 1] == '':
            return ''
        if len(row[pos + 4]) == 0:
            return ''
        address = f'{row[pos + 0]} {row[pos + 1]}'
        if len(row[pos + 2]) > 0:
            address += f' #{row[pos + 2]}'
        address += f'; {row[pos + 3]}'
        address += f' {row[pos + 4]}'
        address += f' {row[pos + 5]}'
        if len(row[pos + 6]) > 0:
            address += f'-{row[pos + 6]}'
        if len(row[pos + 7]) > 0:
            address += f'; {row[pos + 7]}'
        if len(row[pos + 8]) > 0:
            address += f'; {row[pos + 8]}'
        if len(row[pos + 9]) > 0:
            address += f'; {row[pos + 9]}'
        return address

    def get_address(self, address_id):
        results = self.fetchall(f"""
            select address_id, house_number, street_name,
                apt_no, city, state, zipcode, plus4,
                address_line2, address_line3, country
                from mailing_address
                where address_id={address_id}
        """)
        return self.from_records(results)

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
            updates, old_addresses = self.build_address_details(old_addresses, df)
            self.insert_details(updates)
            print('Done')
        old_addresses = self.addresses[['address_id', 'key']]
        for c in county_names:
            print(c.name, end=' links ... ')
            df = vl.read_csv(c, edition)
            updates = self.rebuild_voter_address_links(old_addresses, df)
            self.replace_links(updates)
            print('Done')

    def build_address_details(self, old_addresses, df):
        """
        Find anything new in df. Assign new addresses an
        address_id. Add the new addresses to the old addresses.
        :param old_addresses:
        :param df:
        :return:
        """
        df = VoterList.as_mailing_address(df)
        df.insert(0, 'address_id', np.NaN)
        df = df.drop_duplicates(['house_number', 'city', 'zipcode', 'street_name', 'apt_no', 'plus4'])
        df = pd.concat([old_addresses, df])
        df = df.drop_duplicates(['house_number', 'city', 'zipcode', 'street_name', 'apt_no', 'plus4'],
                                keep='first')
        df = df[df['city'] != '']
        updates = pd.DataFrame(columns=old_addresses.columns)
        if len(df.index) > 0:
            updates = df[pd.isna(df['address_id'])]
            next_address_id = len(old_addresses.index)
            updates = updates.assign(address_id=range(next_address_id, next_address_id + len(updates)))
            old_addresses = pd.concat([old_addresses, updates])
        return updates, old_addresses

    def rebuild_voter_address_links(self, old_addresses, df):
        """
        Insert or update rows linking voter_id to mailing addresses.

        :param old_addresses:
        :param df: a voter list data frame
        :return: old addresses merged with new addresses
        """

        # Construct a dataframe consisting of the mailing
        # address portion of the voter list. Add voter_id
        # and address key, then reduce the dataframe to just
        # voter_id and key.
        new_addresses = self.add_address_key(VoterList.as_mailing_address(df))
        new_addresses.insert(0, 'voter_id', df['voter_id'])
        new_addresses = new_addresses[['voter_id', 'key']]
        new_addresses = new_addresses[new_addresses.key != '']
        # join new addresses and old addresses on key and save the
        # result
        return old_addresses.merge(new_addresses, how='inner', on='key')

    def insert_details(self, details):
        if len(details.index) > 0:
            with self.con:
                with self.con.cursor() as cur:
                    records = details[['address_id', 'house_number',
                                       'street_name', 'apt_no', 'city',
                                       'state', 'zipcode', 'plus4',
                                       'address_line2', 'address_line3', 'country']].to_records(index=False)
                    execute_values(cur, f"""
                            insert into mailing_address (address_id, house_number, street_name,
                                             apt_no, city, state, zipcode, plus4,
                                             address_line2, address_line3, country)
                                values %s
                    """, records)

    def replace_links(self, new_addresses):
        """
        Replace (or insert) the mailing addresses for
        voters. Delete any old addresses first (in the case
        of replace).
        :param new_addresses:
        :return: None
        """
        if 'voter_id' not in new_addresses.columns:
            raise ValueError('voter_id missing from addresses!')
        self.delete_voters_from_table(new_addresses, 'mailing_address_voter')
        records = new_addresses[['voter_id', 'address_id']].to_records(index=False)
        with self.con:
            with self.con.cursor() as cur:
                execute_values(cur, f"""
                    insert into mailing_address_voter (voter_id, address_id) values %s
                """, records)
