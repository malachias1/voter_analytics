import pandas as pd

from data.pathes import Pathes
from data.voterdb import VoterDb
from geopandas import GeoSeries, GeoDataFrame
from maps.district_maps import CngDistrictMap, HseDistrictMap, SenDistrictMap


class AbstractElectionResult(Pathes):
    def __init__(self, root_dir, election_date, district, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)
        if election_date is None:
            raise ValueError('election_date cannot be None!')
        self.election_date = election_date
        self.district = district

        self.district_maps_ = None
        self.results_ = None

        self.district_map_ = None
        self.result_ = None
        self.precincts_ = None
        self.voter_history_ = None
        self.voter_precincts_ = None
        self.voters_ = None
        self.vtd_maps_ = None

    @property
    def county_codes(self):
        return self.precincts.county_code.unique()

    @property
    def district_map(self):
        if self.district_map_ is None:
            self.district_map_ = self.district_maps_.get_map(self.district)
        return self.district_map_

    @property
    def precincts(self):
        if self.precincts_ is None:
            self.precincts_ = self.district_maps_.get_precincts(self.district)
        return self.precincts_

    @property
    def results(self):
        return self.results_

    @property
    def voter_history(self):
        if self.voter_history_ is None:
            self.voter_history_ = self.district_maps_.get_voter_history(self.district, self.election_date)
        return self.voter_history_

    @property
    def voter_precincts(self):
        if self.voter_precincts_ is None:
            self.voter_precincts_ = self.district_maps_.get_voter_precincts(self.district)
        return self.voter_precincts_

    @property
    def voter_query(self):
        return self.district_maps_.get_voter_query(self.district)

    @property
    def voters(self):
        if self.voters_ is None:
            self.voters_ = self.district_maps_.get_voters(self.district)
        return self.voters_

    @property
    def vtd_maps(self):
        if self.vtd_maps_ is None:
            self.vtd_maps_ = self.district_maps_.get_vtd_maps(self.district)
        return self.vtd_maps_


class USHouseElectionResult(AbstractElectionResult):
    def __init__(self, root_dir, election_date, district, state='ga'):
        super().__init__(root_dir, election_date, district, state)
        self.results_ = self.db.get_election_results_for_category(self.election_date, 'us_house',
                                                                  self.district)
        self.district_maps_ = CngDistrictMap(root_dir, state)
