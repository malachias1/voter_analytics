#! ../venv/bin/python
import argparse
from util.search import VoterMatch
from pathlib import Path
import django
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clean and update a voter contact list.')
    parser.add_argument('column_map', metavar='COLUMN_MAP', type=str,
                        help='a json object that maps standard names to column names in the input file.')
    parser.add_argument('input_file', metavar='INPUT_FILE', type=str,
                        help='a csv input file')

    parser.add_argument('output_file', metavar='OUTPUT_FILE', type=str,
                        help='a csv output file')
    args = parser.parse_args()

    vm = VoterMatch(column_map_path=args.column_map)
    logp = Path(args.input_file).with_suffix('.log')
    df = vm.match(args.input_file, log_path=logp)
    df.to_csv(args.output_file, index=False)


