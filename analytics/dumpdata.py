import django
from django.core.management import call_command
import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'dashboard.settings'
django.setup()

MODELS = [
    # 'AddressVoter',
    # 'BlockGroupGeometry',
    # 'Children',
    # 'CngMap',
    # 'ContestClass',
    # 'CountyDetails',
    # 'CountyMap',
    'EducationalAttainment',
    # 'ElectionResultDetails',
    # 'ElectionResults',
    # 'ElectionResultsOverUnder',
    # 'HseMap',
    # 'MailingAddress',
    # 'MailingAddressVoter',
    # 'MedianHouseHoldIncome',
    # 'PrecinctDetails',
    # 'PrecinctSummary',
    # 'ResidenceAddress',
    # 'SenMap',
    # 'VoterCng',
    # 'VoterDemographics',
    # 'VoterHistory',
    # 'VoterHistorySummary',
    # 'VoterHse',
    # 'VoterName',
    # 'VoterPrecinct',
    # 'VoterScore',
    # 'VoterSearch',
    # 'VoterSen',
    # 'VoterStatus',
    # 'VtdMap',
    # 'WorkTravelTime'
]


def dumptable(model_name):
    print(f'Dumping {model_name} ...', end='')
    call_command('dumpdata', '--database', 'sqlite', f'analytics.{model_name}',
                 '--indent', '2', '-o', f'../data/{model_name}.json'.lower())
    print(f' done.')


if __name__ == '__main__':
    for t in MODELS:
        dumptable(t)
