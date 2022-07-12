import unittest
from ingest.voter_list_ingest import IngestVoterList
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()


class MailingAddressBugTestCase(unittest.TestCase):

    def test_diagnose(self):
        root_dir = '~/Documents/data'
        ivl = IngestVoterList(root_dir)
        df = ivl.read_csv('036')
        ivl.ingest_mailing_address_relationship(df)


if __name__ == '__main__':
    unittest.main()
