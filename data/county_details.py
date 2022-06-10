import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb


class CountyDetails(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.name2code_ = {}
        self.name2fp_ = {}
        self.fp2code_ = {}
        self.fp2name_ = {}
        self.code2name_ = {}
        self.code2fp_ = {}
        db = VoterDb(root_dir, state)
        df = pd.read_sql_query(f"select * from county_details", db.con)
        for i in range(0, len(df)):
            name = df.loc[i].county_name.upper()
            code = df.loc[i].county_code
            fp = df.loc[i].county_fp
            self.name2code_[name] = code
            self.name2fp_[name] = fp
            self.fp2code_[fp] = code
            self.fp2name_[fp] = name
            self.code2name_[code] = name
            self.code2fp_[code] = fp

    def name2code(self, k):
        return self.name2code_.get(k.upper(), None)

    def name2fp(self, k):
        return self.name2fp_.get(k.upper(), None)

    def fp2code(self, k):
        return self.fp2code_.get(k, None)

    def fp2name(self, k):
        return self.fp2name_.get(k, None)

    def code2name(self, k):
        return self.code2name_.get(k, None)

    def code2fp(self, k):
        return self.code2fp_.get(k, None)
