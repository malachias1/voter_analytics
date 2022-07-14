import json

from data.utils import zip_plus4
from data.voter_details import VoterDetails
from util.addresses import StreetNameNormalizer
from util.names import NameNormalizer
import pandas as pd
from pathlib import Path
import sys


class VoterMatch:
    def __init__(self, column_map_path=None):
        self.sn = StreetNameNormalizer()
        self.nn = NameNormalizer()
        self.vd = VoterDetails()
        self.df = None
        if column_map_path is not None:
            self.column_map_path = Path(column_map_path)
            try:
                with self.column_map_path.open('r') as f:
                    self.column_map = json.load(f)
            except FileNotFoundError as _:
                print(f'ERROR: Column mapping file, "{column_map_path}", not found',
                      file=sys.stderr)
                return
            except json.JSONDecodeError as _:
                print(f'ERROR: Column mapping file is not a valid JSON file.',
                      file=sys.stderr)
                return
        else:
            self.column_map = {}

    def get_column_name_mapping(self, column_name):
        mapping = self.column_map.get(column_name, None)
        return mapping if mapping is not None else column_name

    def get_street_address(self, i):
        column_name = self.get_column_name_mapping("street_address")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_house_number(self, i):
        street_address = self.get_street_address(i)
        house_number, _ = self.sn.street_address_split(street_address)
        return house_number

    def get_apt_no(self, i):
        column_name = self.get_column_name_mapping("apt_no")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_city(self, i):
        column_name = self.get_column_name_mapping("city")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_state(self, i):
        column_name = self.get_column_name_mapping("state")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_zipcode(self, i):
        column_name = self.get_column_name_mapping("zipcode")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_zipcode_plus4(self, i):
        return zip_plus4(self.get_zipcode(i))

    def get_country(self, i):
        column_name = self.get_column_name_mapping("country")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_contact_name(self, i):
        column_name = self.get_column_name_mapping("contact_name")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_first_name(self, i):
        column_name = self.get_column_name_mapping("first_name")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_last_name(self, i):
        column_name = self.get_column_name_mapping("last_name")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_precinct_id(self, i):
        column_name = self.get_column_name_mapping("precinct_id")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_gender(self, i):
        column_name = self.get_column_name_mapping("gender")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_hse(self, i):
        column_name = self.get_column_name_mapping("hse")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_sen(self, i):
        column_name = self.get_column_name_mapping("sen")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_voter_id(self, i):
        column_name = self.get_column_name_mapping("voter_id")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def get_cng(self, i):
        column_name = self.get_column_name_mapping("cng")
        if column_name is not None:
            return self.df.at[i, column_name]
        else:
            return None

    def set_street_address(self, i, house_number, street_name):
        street_address = f'{house_number} {street_name.title()}'
        column_name = self.get_column_name_mapping("street_address")
        if column_name is not None:
            self.df.at[i, column_name] = street_address

    def set_apt_no(self, i, value):
        column_name = self.get_column_name_mapping("apt_no")
        if column_name is not None:
            self.df.at[i, column_name] = value

    def set_city(self, i, value):
        column_name = self.get_column_name_mapping("city")
        if column_name is not None:
            self.df.at[i, column_name] = value.title()

    def set_state(self, i, value):
        column_name = self.get_column_name_mapping("state")
        if column_name is not None:
            self.df.at[i, column_name] = value.title()

    def set_zipcode(self, i, zipcode, plus4):
        zipcode = f'{zipcode}-{plus4}' if len(plus4) > 0 else zipcode
        column_name = self.get_column_name_mapping("zipcode")
        if column_name is not None:
            self.df.at[i, column_name] = zipcode

    def set_country(self, i, value):
        column_name = self.get_column_name_mapping("country")
        if column_name is not None:
            self.df.at[i, column_name] = value.title()

    def set_name(self, i, first_name, last_name):
        self.set_first_name(i, first_name)
        self.set_last_name(i, last_name)
        self.set_contact_name(i, first_name, last_name)

    def set_contact_name(self, i, first_name, last_name):
        column_name = self.get_column_name_mapping("contact_name")
        if column_name is not None:
            self.df.at[i, column_name] = f'{first_name} {last_name}'.title()

    def set_first_name(self, i, value):
        column_name = self.get_column_name_mapping("first_name")
        if column_name is not None:
            self.df.at[i, column_name] = value.title()

    def set_last_name(self, i, value):
        column_name = self.get_column_name_mapping("last_name")
        if column_name is not None:
            self.df.at[i, column_name] = value.title()

    def set_precinct_id(self, i, value):
        column_name = self.get_column_name_mapping("precinct_id")
        if column_name is not None:
            self.df.at[i, column_name] = value

    def set_gender(self, i, value):
        column_name = self.get_column_name_mapping("gender")
        if column_name is not None:
            self.df.at[i, column_name] = value

    def set_hse(self, i, value):
        column_name = self.get_column_name_mapping("hse")
        if column_name is not None:
            self.df.at[i, column_name] = value

    def set_sen(self, i, value):
        column_name = self.get_column_name_mapping("sen")
        if column_name is not None:
            self.df.at[i, column_name] = value

    def set_voter_id(self, i, value):
        column_name = self.get_column_name_mapping("voter_id")
        if column_name is not None:
            self.df.at[i, column_name] = value

    def set_cng(self, i, value):
        column_name = self.get_column_name_mapping("cng")
        if column_name is not None:
            self.df.at[i, column_name] = value

    def is_matchable(self, i):
        return (pd.notna(self.get_street_address(i)) &
                pd.notna(self.get_zipcode(i)) &
                pd.notna(self.get_contact_name(i)) &
                pd.notna(self.get_first_name(i)) &
                pd.notna(self.get_last_name(i)))

    def is_compound_name(self, i):
        return (self.nn.is_compound_name(self.get_contact_name(i)) |
                self.nn.is_compound_name(self.get_first_name(i)))

    def is_street_address(self, i):
        return self.sn.is_street_address(self.get_street_address(i))

    def match(self, p, log_path=None):
        try:
            df = pd.read_csv(p, dtype=str)
            return self.match_df(df, log_path)
        except FileNotFoundError as _:
            print(f'ERROR: Input file, "{str(p)}", not found')

    def match_df(self, df, log_path):
        self.df = df
        if log_path is None:
            self.match_with_log(sys.stderr)
        else:
            with log_path.open('w') as fo:
                self.match_with_log(fo)
        return self.df

    def match_with_log(self, fo):
        matched = 0
        for i in range(0, len(self.df)):
            # Skip clearly unmatchable rows that are missing
            # essential information
            if not self.is_matchable(i):
                print(f'Row[{i + 1}]-ERROR: Skipped, missing essential information!',
                      file=fo)
                continue
            # Skip rows that contain names like "Bob and Kathy" or
            # "Bob & Kathy"
            if i == 51:
                print(i)
            if self.is_compound_name(i):
                print(f'Row[{i + 1}]-ERROR: Name, "{self.get_contact_name(i)}" or "{self.get_first_name(i)}", '
                      f'refers to more than one person!',
                      file=fo)
                continue
            # Skip rows that have street address with no house number
            house_number = self.get_house_number(i)
            if house_number is None:
                print(f'Row[{i + 1}]-ERROR: Street address, "{self.get_street_address(i)}", has no house number!',
                      file=fo)
                continue
            # Skip rows that have a bad zipcode (not 12345, 12345-1234, 123451234)
            zipcode, _ = self.get_zipcode_plus4(i)
            if zipcode is None:
                print(f'Row[{i + 1}]-ERROR: Invalid zip code format, "{self.get_zipcode(i)}"!',
                      file=fo)
                continue

            # Try first and last name fields first
            first_name = self.nn.normalize(self.get_first_name(i))[0]
            last_name = self.nn.normalize(self.get_last_name(i))[-1]
            matches = self.vd.voter_search(first_name, last_name, house_number, zipcode)

            if len(matches) == 0:
                # well that didn't work
                # try contact name.
                # There should be one entry
                names = self.nn.normalize(self.get_contact_name(i))
                name = names[0].split()
                first_name = name[0]
                last_name = name[-1]
                matches = self.vd.voter_search(first_name, last_name, house_number, zipcode)

                if len(matches) == 0:
                    # Still no joy, skip it.
                    print(
                        f'Row[{i + 1}]-ERROR: Unable to find match for {first_name} {last_name} with zip code '
                        f'{zipcode} and house number {house_number}!',
                        file=fo)
                    continue
            # Check for multiple matches
            if len(matches) > 1:
                print(
                    f'Row[{i + 1}]-ERROR: Multiple matches for {first_name} {last_name} with zip code '
                    f'{zipcode} and house number {house_number}!',
                    file=fo)
                continue
            # One match. Yea! Update the record
            voter_id = matches.voter_id[0]
            last_name = matches.last_name[0]
            first_name = matches.first_name[0]

            # get various address stuff
            address = self.vd.get_residence_address(voter_id)
            street_name = address.street_name[0]
            apt_no = address.apt_no[0]
            city = address.city[0]
            zipcode = address.zipcode[0]
            plus4 = address.plus4[0]

            # get demographics
            demographics = self.vd.get_demographics(voter_id)
            gender = demographics.gender[0]

            # get various political districts
            precinct_id = self.vd.get_precinct_id(voter_id)
            cng = self.vd.get_cng(voter_id)
            sen = self.vd.get_sen(voter_id)
            hse = self.vd.get_hse(voter_id)

            # update row
            self.set_contact_name(i, first_name, last_name)
            self.set_voter_id(i, voter_id)
            self.set_first_name(i, first_name)
            self.set_last_name(i, last_name)
            self.set_gender(i, gender)

            self.set_street_address(i, house_number, street_name)
            self.set_apt_no(i, apt_no)
            self.set_zipcode(i, zipcode, plus4)
            self.set_city(i, city)
            # Since this the residence address in Georgia
            # set state and country to Georgia
            self.set_state(i, 'Georgia')
            self.set_country(i, 'United States')

            self.set_precinct_id(i, precinct_id)
            self.set_hse(i, hse)
            self.set_sen(i, sen)
            self.set_cng(i, cng)
            matched += 1
            print(
                f'Row[{i + 1}]-: Successful match for {first_name} {last_name} with zip code '
                f'{zipcode} and house number {house_number}!',
                file=fo)
        print(
            f'\nRow Count: {len(self.df)}, Matched Row Count: {matched}, Match %: {matched / len(self.df):.2f}',
            file=fo)
        return self.df
