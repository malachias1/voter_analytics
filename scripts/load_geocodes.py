#! ../venv/bin/python
import sys
from pathlib import Path

sys.path.append('..')

import os
import argparse
import django

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

from geocode.loader import AddressLoader

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load geocoded addresses.')
    parser.add_argument('root_dir', metavar='ROOT_DIR', type=str,
                        help='a directory containing geocoded csv files')

    args = parser.parse_args()

    al = AddressLoader(args.root_dir)
    al.load()

