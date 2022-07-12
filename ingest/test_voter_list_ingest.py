import unittest
from pathlib import Path
import pandas as pd
from ingest.voter_list_ingest import IngestVoterList
from data.voterdb import VoterDb
from random import seed, randint


class TestIngestVoterList(unittest.TestCase):
    def setUp(self):
        self.root_dir = '../test_resources'
        self.voter_db_path = Path('../test_resources/ga/voter.db').expanduser()
        self.voter_db_path.unlink(missing_ok=True)
        self.db = VoterDb(self.root_dir)
        self.db.initialize()
        self.sut = IngestVoterList(self.root_dir)
        seed(10)

    def test_zip_plus4_without_plus4(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        zip_plus4 = pd.DataFrame.from_records(df['residence_zipcode'].apply(self.sut.zip_plus4))
        self.assertEqual('30152', zip_plus4.loc[0, 0])
        self.assertEqual('', zip_plus4.loc[0, 1])

    def test_zip_plus4_with_plus4(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        zip_plus4 = pd.DataFrame.from_records(df['residence_zipcode'].apply(self.sut.zip_plus4))
        self.assertEqual('30106', zip_plus4.loc[6, 0])
        self.assertEqual('1078', zip_plus4.loc[6, 1])

    def test_as_residence_address_key_pos_0(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_residence_address(df)
        key = df['key']
        self.assertEqual('3629 HOLLYHOCK WAY NW; KENNESAW GA 30152', key.loc[0])

    def test_as_mailing_address_key_pos_0(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_mailing_address(df)
        key = df['key']
        self.assertEqual('5756 STARBOARD CT; BUFORD GA 30518-2020', key.loc[0])

    def test_as_residence_address_house_number(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_residence_address(df)
        x = df['house_number']
        self.assertEqual('3629', x.loc[0])

    def test_as_residence_address_street_name(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_residence_address(df)
        x = df['street_name']
        self.assertEqual('HOLLYHOCK WAY NW', x.loc[0])

    def test_as_residence_address_city(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_residence_address(df)
        x = df['city']
        self.assertEqual('KENNESAW', x.loc[0])

    def test_as_residence_address_state(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_residence_address(df)
        x = df['state']
        self.assertEqual('GA', x.loc[0])

    def test_as_residence_zipcode(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_residence_address(df)
        x = df['zipcode']
        self.assertEqual('30152', x.loc[0])

    def test_as_mailing_address_house_number(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_mailing_address(df)
        x = df['house_number']
        self.assertEqual('5756', x.loc[0])

    def test_as_mailing_address_street_name(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_mailing_address(df)
        x = df['street_name']
        self.assertEqual('STARBOARD CT', x.loc[0])

    def test_as_mailing_address_city(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_mailing_address(df)
        x = df['city']
        self.assertEqual('BUFORD', x.loc[0])

    def test_as_mailing_address_state(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_mailing_address(df)
        x = df['state']
        self.assertEqual('GA', x.loc[0])

    def test_as_mailing_zipcode(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_mailing_address(df)
        x = df['zipcode']
        self.assertEqual('30518', x.loc[0])

    def test_as_mailing_plus4(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df = self.sut.as_mailing_address(df)
        x = df['plus4']
        self.assertEqual('2020', x.loc[0])

    def test_ingest_voter_name(self):
        """
        Ingest voter ids. Make sure the stored ids
        match the ingested ids. Make sure that the
        operation is idempotent.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_voter_name(df)
        dfdb = self.sut.voter_names
        self.assertEqual(len(df), len(dfdb))
        self.assertTrue(df['voter_id'].equals(dfdb['voter_id']))
        self.sut.ingest_voter_name(df)
        # test idempotent
        dfdb = self.sut.voter_names
        self.assertEqual(len(df), len(dfdb))
        self.assertTrue(df['voter_id'].equals(dfdb['voter_id']))

    def test_ingest_voter_name_update(self):
        """
        The two sets of voter ids are overlapping.
        Make sure there are no duplicate ids.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df1 = df.iloc[range(0, 50)]
        self.sut.ingest_voter_name(df1)
        df2 = df.iloc[range(30, len(df))]
        self.sut.ingest_voter_name(df2)
        dfdb = self.sut.voter_names
        self.assertEqual(len(df), len(dfdb))

    def test_ingest_voter_name_check_values(self):
        """
        Check the values of the ingested voter name data.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_voter_name(df)
        con = self.db.con
        df2 = pd.read_sql_query(f"select * from voter_name where voter_id='00014397'", con)
        self.assertEqual(1, len(df2))
        self.assertEqual('SMITH', df2.iloc[0, 1])
        self.assertEqual('HUGH', df2.iloc[0, 2])
        self.assertEqual('LEAVELL', df2.iloc[0, 3])
        self.assertEqual('JR', df2.iloc[0, 4])
        self.assertEqual('', df2.iloc[0, 5])

    def test_ingest_voter_status(self):
        """
        Ingest voter status. Make sure the stored ids
        match the ingested ids. Make sure that the
        operation is idempotent.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_voter_status(df)
        dfdb = self.sut.voter_status
        self.assertEqual(len(df), len(dfdb))
        self.assertTrue(df['voter_id'].equals(dfdb['voter_id']))
        self.sut.ingest_voter_status(df)
        # test idempotent
        dfdb = self.sut.voter_status
        self.assertEqual(len(df), len(dfdb))
        self.assertTrue(df['voter_id'].equals(dfdb['voter_id']))

    def test_ingest_voter_status_check_values(self):
        """
        Check the values of the ingested voter status data.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_voter_demographics(df)
        con = self.db.con
        df2 = pd.read_sql_query(f"select * from voter_demographics where voter_id='00014397'", con)
        self.assertEqual(1, len(df2))
        self.assertEqual('WH', df2.iloc[0, 1])
        self.assertEqual('M', df2.iloc[0, 2])
        self.assertEqual(1944, df2.iloc[0, 3])

    def test_ingest_voter_demographics(self):
        """
        Ingest voter demographics. Make sure the stored ids
        match the ingested ids. Make sure that the
        operation is idempotent.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_voter_demographics(df)
        dfdb = self.sut.voter_demographics
        self.assertEqual(len(df), len(dfdb))
        self.assertTrue(df['voter_id'].equals(dfdb['voter_id']))
        self.sut.ingest_voter_demographics(df)
        # test idempotent
        dfdb = self.sut.voter_demographics
        self.assertEqual(len(df), len(dfdb))
        self.assertTrue(df['voter_id'].equals(dfdb['voter_id']))

    def test_ingest_voter_demographics_check_values(self):
        """
        Check the values of the ingested demographic data.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_voter_demographics(df)
        con = self.db.con
        df2 = pd.read_sql_query(f"select * from voter_demographics where voter_id='00014397'", con)
        self.assertEqual(1, len(df2))
        self.assertEqual('WH', df2.iloc[0, 1])
        self.assertEqual('M', df2.iloc[0, 2])
        self.assertEqual(1944, df2.iloc[0, 3])

    def test_ingest_residence_address(self):
        """
        The two sets of addresses have duplicate address.
        Make sure there are no duplicate ids.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        df1 = df.sample(frac=0.8, ignore_index=True)
        self.sut.ingest_residence_address('033', df1)
        df2 = df.sample(frac=0.8, ignore_index=True)
        self.sut.ingest_residence_address('033', df2)
        self.sut.ingest_residence_address('033', df)
        dfdb = self.sut.get_residence_addresses('033')
        self.assertEqual(90, len(dfdb))

    def test_ingest_residence_address_max_id(self):
        """
        Check max id before and after load.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.assertEqual(0, self.sut.db.next_residence_address_id)
        self.sut.ingest_residence_address('033', df)
        self.assertEqual(90, self.sut.db.next_residence_address_id)

    def test_ingest_residence_address_check_values(self):
        """
        Check the values of the ingested data.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_residence_address('033', df)
        con = self.db.con
        df2 = pd.read_sql_query(f"select * from residence_address where zipcode='30144' and "
                                f"plus4='3073' and house_number='3736' and street_name='JESICA TRCE NE' ", con)
        self.assertEqual(1, len(df2))
        self.assertEqual('KENNESAW', df2.iloc[0, 5])

    def test_ingest_address_relationship(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_voter_name(df)
        self.sut.ingest_residence_address('033', df)
        self.sut.ingest_address_voter_relationship('033', df)
        dfdb = self.sut.address_voter
        self.assertEqual(len(df), len(dfdb))
        n = randint(0, len(df))
        voter_id = df.voter_id.iloc[n]
        addresses = self.db.get_residence_addresses_for_county('033')
        x = dfdb[dfdb.voter_id == voter_id]
        self.assertEqual(1, len(x.index))
        address_id = x.address_id.iloc[0]
        address = addresses[addresses.address_id == address_id]
        self.assertEqual(1, len(address.index))
        self.assertEqual(address.house_number.iloc[0], df.residence_house_number.iloc[n])


    def test_ingest_address_voter_relationship(self):
        p = Path('../test_resources/ga/politics/voter_list/033/latest.csv').expanduser()
        df = pd.read_csv(p, dtype=str).fillna('')
        self.sut.ingest_voter_name(df)
        self.sut.ingest_residence_address('033', df)
        self.sut.ingest_address_voter_relationship('033', df)
        dfdb = self.sut.address_voter
        self.assertEqual(len(df), len(dfdb))

    def test_ingest_mailing_address(self):
        """
        There are only two mailing addresses in the dataset.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_mailing_address(df)
        dfdb = self.sut.mailing_addresses
        self.assertEqual(2, len(dfdb))

    def test_ingest_mailing_address_max_id(self):
        """
        Check max id before and after load.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.assertEqual(0, self.sut.db.next_mailing_address_id)
        self.sut.ingest_mailing_address(df)
        self.assertEqual(2, self.sut.db.next_mailing_address_id)

    def test_ingest_mailing_address_check_values(self):
        """
        Check the values of the ingested data.
        """
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_mailing_address(df)
        con = self.db.con
        df2 = pd.read_sql_query(f"select * from mailing_address where zipcode='30518' and "
                                f"plus4='2020' and house_number='5756' and street_name='STARBOARD CT' ", con)
        self.assertEqual(1, len(df2))
        self.assertEqual('BUFORD', df2.iloc[0, 4])

    def test_ingest_mailing_address_relationship(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_voter_name(df)
        self.sut.ingest_mailing_address(df)
        self.sut.ingest_mailing_address_relationship(df)
        dfdb = self.sut.mailing_address_voter
        self.assertEqual(2, len(dfdb))

    def test_ingest_precinct_details(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/033/latest.csv', dtype=str).fillna('')
        self.sut.ingest_residence_address('033', df)
        x = self.sut.ingest_precinct_details('033', df)
        self.assertEqual(100, x)

    def test_ingest_precinct_details_132(self):
        df = pd.read_csv('../test_resources/ga/politics/voter_list/132/latest.csv', dtype=str).fillna('')
        self.sut.ingest_residence_address('132', df)
        x = self.sut.ingest_precinct_details('132', df)
        self.assertEqual(12500, x)


if __name__ == '__main__':
    unittest.main()
