import unittest
from pathlib import Path
import pandas as pd
import sqlite3 as sql
from data.voterdb import VoterDb


class TestVoterDb(unittest.TestCase):
    def setUp(self):
        self.root_dir = '../test_resources'
        self.voter_db_path = Path('../test_resources/ga/voter.db').expanduser()
        self.voter_db_path.unlink(missing_ok=True)
        self.sut = VoterDb(self.root_dir)
        self.sut.initialize()

    def test_as_residence_address_key_no_apt_no_plus4(self):
        row = pd.Series(data=('033', '1234', 'HAPPY ST', '', 'EVANS', 'GA', '12345', ''))
        key = self.sut.as_residence_address_key(row)
        self.assertEqual('1234 HAPPY ST; EVANS GA 12345', key)

    def test_as_residence_address_key_apt_no_plus4(self):
        row = pd.Series(data=('033', '1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', ''))
        key = self.sut.as_residence_address_key(row)
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345', key)

    def test_as_residence_address_key_apt_plus4(self):
        row = pd.Series(data=('033', '1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', '2020'))
        key = self.sut.as_residence_address_key(row)
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020', key)

    def test_as_residence_address_key_apt_plus4_pos_1(self):
        row = pd.Series(data=('0', '033', '1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', '2020'))
        key = self.sut.as_residence_address_key(row, pos=1)
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020', key)

    def test_as_mailing_address_key_apt_plus4(self):
        row = pd.Series(data=('1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', '2020', 'A', 'B', 'USA'))
        key = self.sut.as_mailing_address_key(row)
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020; A; B; USA', key)

    def test_get_residence_addresses_empty(self):
        """
        Make sure get_residence_addresses works with empty
        table. Make sure key column is there.
        """

        dfdb = self.sut.get_residence_addresses('033')
        self.assertEqual(0, len(dfdb))
        self.assertTrue('key' in dfdb.columns)

    def test_get_mailing_addresses_empty(self):
        """
        Make sure get_mailing_addresses works with empty
        table. Make sure key column is there.
        """

        dfdb = self.sut.get_mailing_addresses()
        self.assertEqual(0, len(dfdb))
        self.assertTrue('key' in dfdb.columns)

    def test_get_voter_names_empty(self):
        """
        Make sure get_voter_names works with empty
        table. Make sure key column is there.
        """

        dfdb = self.sut.get_voter_names()
        self.assertEqual(0, len(dfdb))

    def test_get_voter_status_empty(self):
        """
        Make sure get_voter_status works with empty
        table. Make sure key column is there.
        """

        dfdb = self.sut.get_voter_status()
        self.assertEqual(0, len(dfdb))

    def test_get_voter_demographics_empty(self):
        """
        Make sure get_voter_demographics works with empty
        table. Make sure key column is there.
        """

        dfdb = self.sut.get_voter_demographics()
        self.assertEqual(0, len(dfdb))

    def test_get_address_voter_empty(self):
        """
        Make sure get_address_voter works with empty
        table. Make sure key column is there.
        """

        dfdb = self.sut.get_residence_address_voter()
        self.assertEqual(0, len(dfdb))

    def test_get_mailing_address_voter_empty(self):
        """
        Make sure get_mailing_address_voter works with empty
        table. Make sure key column is there.
        """

        dfdb = self.sut.get_mailing_address_voter()
        self.assertEqual(0, len(dfdb))

    def test_get_residence_addresses_not_empty(self):
        """
        Make sure get_residence_addresses returns correct values.
        """

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
        cur = self.sut.con.cursor()
        cur.execute(stmt, (0, '033', '1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', '2020'))
        self.sut.con.commit()

        df = self.sut.get_residence_addresses('033')
        self.assertEqual(1, len(df))
        self.assertEqual(0, df.iloc[0, 0])
        self.assertEqual('033', df.iloc[0, 1])
        self.assertEqual('1234', df.iloc[0, 2])
        self.assertEqual('HAPPY ST', df.iloc[0, 3])
        self.assertEqual('12', df.iloc[0, 4])
        self.assertEqual('EVANS', df.iloc[0, 5])
        self.assertEqual('GA', df.iloc[0, 6])
        self.assertEqual('12345', df.iloc[0, 7])
        self.assertEqual('2020', df.iloc[0, 8])
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020', df.iloc[0, len(df.columns)-1])

    def test_get_mailing_addresses_not_empty(self):
        """
        Make sure get_mailing_addresses returns correct values.
        """

        stmt = f"""
        insert into mailing_address ('address_id',
                       'house_number',
                       'street_name',
                       'apt_no',
                       'city',
                       'state',
                       'zipcode',
                       'plus4',
                       'address_line2',
                       'address_line3',
                       'country') 
                       values (?,?,?,?,?,?,?,?,?,?,?)
        """
        cur = self.sut.con.cursor()
        cur.execute(stmt, (0, '1234', 'HAPPY ST', '12', 'EVANS', 'GA', '12345', '2020', 'A', 'B', 'USA'))
        self.sut.con.commit()

        df = self.sut.get_mailing_addresses()
        self.assertEqual(1, len(df))
        self.assertTrue('key' in df.columns)
        self.assertEqual(0, df.iloc[0, 0])
        self.assertEqual('1234', df.iloc[0, 1])
        self.assertEqual('HAPPY ST', df.iloc[0, 2])
        self.assertEqual('12', df.iloc[0, 3])
        self.assertEqual('EVANS', df.iloc[0, 4])
        self.assertEqual('GA', df.iloc[0, 5])
        self.assertEqual('12345', df.iloc[0, 6])
        self.assertEqual('2020', df.iloc[0, 7])
        self.assertEqual('A', df.iloc[0, 8])
        self.assertEqual('B', df.iloc[0, 9])
        self.assertEqual('USA', df.iloc[0, 10])
        self.assertEqual('1234 HAPPY ST #12; EVANS GA 12345-2020; A; B; USA', df.iloc[0, 11])

    def test_get_voter_names_not_empty(self):
        """
        Make sure get_voter_names returns correct values.
        """

        stmt = f"insert into voter_name values (?,?,?,?,?,?)"
        cur = self.sut.con.cursor()
        cur.execute(stmt, ('0', 'BOGGS', 'WILLIAM', 'BENTLEY', 'IV', 'DR'))
        self.sut.con.commit()

        df = self.sut.get_voter_names()
        self.assertEqual(1, len(df))
        self.assertEqual('0', df.iloc[0, 0])
        self.assertEqual('BOGGS', df.iloc[0, 1])
        self.assertEqual('WILLIAM', df.iloc[0, 2])
        self.assertEqual('BENTLEY', df.iloc[0, 3])
        self.assertEqual('IV', df.iloc[0, 4])
        self.assertEqual('DR', df.iloc[0, 5])

    def test_get_voter_status_not_empty(self):
        """
        Make sure get_voter_status returns correct values.
        """

        stmt = f"insert into voter_status values (?,?,?,?,?,?,?)"
        cur = self.sut.con.cursor()
        cur.execute(stmt, ('0', 'I', 'NCOA', '20000101', '20210101', '20210102', '20100102'))
        self.sut.con.commit()

        df = self.sut.get_voter_status()
        self.assertEqual(1, len(df))
        self.assertEqual('0', df.iloc[0, 0])
        self.assertEqual('I', df.iloc[0, 1])
        self.assertEqual('NCOA', df.iloc[0, 2])
        self.assertEqual('20000101', df.iloc[0, 3])
        self.assertEqual('20210101', df.iloc[0, 4])
        self.assertEqual('20210102', df.iloc[0, 5])
        self.assertEqual('20100102', df.iloc[0, 6])

    def test_get_voter_demographics_not_empty(self):
        """
        Make sure get_voter_demographics returns correct values.
        """

        stmt = f"insert into voter_demographics values (?,?,?,?)"
        cur = self.sut.con.cursor()
        cur.execute(stmt, ('0', 'WH', 'F', 2000))
        self.sut.con.commit()

        df = self.sut.get_voter_demographics()
        self.assertEqual(1, len(df))
        self.assertEqual('0', df.iloc[0, 0])
        self.assertEqual('WH', df.iloc[0, 1])
        self.assertEqual('F', df.iloc[0, 2])
        self.assertEqual(2000, df.iloc[0, 3])

    def test_get_voter_demographics_not_empty(self):
        """
        Make sure get_voter_demographics returns correct values.
        """

        stmt = f"insert into voter_demographics values (?,?,?,?)"
        cur = self.sut.con.cursor()
        cur.execute(stmt, ('0', 'WH', 'F', 2000))
        self.sut.con.commit()

        df = self.sut.get_voter_demographics()
        self.assertEqual(1, len(df))
        self.assertEqual('0', df.iloc[0, 0])
        self.assertEqual('WH', df.iloc[0, 1])
        self.assertEqual('F', df.iloc[0, 2])
        self.assertEqual(2000, df.iloc[0, 3])

    def test_get_address_voter_not_empty(self):
        """
        Make sure get_address_voter returns correct values.
        """

        stmt = f"insert into address_voter values (?,?)"
        cur = self.sut.con.cursor()
        cur.execute(stmt, (0, '1235'))
        self.sut.con.commit()

        df = self.sut.get_residence_address_voter()
        self.assertEqual(1, len(df))
        self.assertEqual(0, df.iloc[0, 0])
        self.assertEqual('1235', df.iloc[0, 1])

    def test_get_mailing_address_voter_not_empty(self):
        """
        Make sure get_mailing_address_voter returns correct values.
        """

        stmt = f"insert into mailing_address_voter values (?,?)"
        cur = self.sut.con.cursor()
        cur.execute(stmt, (0, '1235'))
        self.sut.con.commit()

        df = self.sut.get_mailing_address_voter()
        self.assertEqual(1, len(df))
        self.assertEqual(0, df.iloc[0, 0])
        self.assertEqual('1235', df.iloc[0, 1])


if __name__ == '__main__':
    unittest.main()
