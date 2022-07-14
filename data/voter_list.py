import pandas as pd
from data.pathes import Pathes
from util.addresses import StreetNameNormalizer
from data.utils import zip_plus4


class VoterList(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)

    @classmethod
    def as_mailing_address(cls, df):
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
        z4 = pd.DataFrame.from_records(df['zipcode'].apply(zip_plus4))
        # Columns need to be in a particular order. Zip code is
        # in the right place, but I need to insert plus4 after
        # zip code.
        df = df.assign(zipcode=z4[0])
        idx = df.columns.get_loc('zipcode')
        df.insert(idx + 1, 'plus4', z4[1])

        return df

    @classmethod
    def as_residence_address(cls, df):
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
        z4 = pd.DataFrame.from_records(df['zipcode'].apply(zip_plus4))
        idx = df.columns.get_loc('zipcode')
        df.insert(idx, 'state', 'GA')
        return df.assign(zipcode=z4[0], plus4=z4[1], lat=None, lon=None)

    def read_csv(self, county_code, edition='latest'):
        sn = StreetNameNormalizer()
        p = self.voter_list_path(county_code, edition)
        df = pd.read_csv(p, dtype=str).fillna('')
        df = df.assign(residence_street_name=df.residence_street_name.apply(sn.normalize),
                       mail_street_name=df.mail_street_name.apply(sn.normalize))
        return df
