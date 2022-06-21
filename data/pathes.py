from pathlib import Path


class Pathes:
    def __init__(self, root_dir, state='ga'):
        self.root_dir_ = Path(root_dir).expanduser()
        self.state_ = state

    @property
    def census_dir(self):
        return Path(self.root_dir, 'census')

    @property
    def maps_dir(self):
        return Path(self.politics_dir, 'maps')

    @property
    def politics_dir(self):
        return Path(self.root_dir, 'politics')

    @property
    def root_dir(self):
        return Path(self.root_dir_, self.state_)

    @property
    def state(self):
        return self.state_

    @property
    def voter_history_dir(self):
        return Path(self.politics_dir, 'voter_history')

    def voter_history_path(self, year):
        return Path(self.voter_history_dir, f'{year}.txt')

    @property
    def voter_list_dir(self):
        return Path(self.politics_dir, 'voter_list')

    def voter_list_path(self, county_code, edition='latest'):
        return Path(self.voter_list_dir, county_code, f'{edition}.csv')
