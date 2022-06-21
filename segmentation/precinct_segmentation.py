import pandas as pd
from data.pathes import Pathes
from data.voterdb import VoterDb


class PrecinctSegmentation(Pathes):
    def __init__(self, root_dir, state='ga'):
        super().__init__(root_dir, state)
        self.db = VoterDb(root_dir, state)

    @property
    def demographics_df(self):
        df = self.db.voter_precinct.merge(self.db.voter_demographics, on='voter_id', how='inner')
        df = df.drop(columns=['voter_id'])
        df = df.rename(columns={'id': 'precinct_id'})
        dummy = pd.DataFrame()
        dummy_race = dummy.assign(race_id=['WH', 'BH', 'U', 'OT', 'HP', 'AI', 'AP'], key=0)
        dummy_gender = dummy.assign(gender=['F', 'M'], key=0)
        dummy_year_of_birth = dummy.assign(year_of_birth=[1922, 1946, 1965, 1981, 1997], key=0)
        dummy = dummy_race.merge(dummy_gender, on='key')
        dummy = dummy.merge(dummy_year_of_birth, on='key')
        dummy = dummy.assign(precinct_id=-1).drop(columns=['key'])
        #
        # dummy_seg = dummy.assign(segment=['WH_F_S', 'WH_F_B', 'WH_F_GX', 'WH_F_F', 'WH_F_M', 'WH_M_S', 'WH_M_B',
        #                                   'WH_M_GX', 'WH_M_F', 'WH_M_M', 'BH_F_S', 'BH_F_B', 'BH_F_GX', 'BH_F_F',
        #                                   'BH_F_M', 'BH_M_S', 'BH_M_B', 'BH_M_GX', 'BH_M_F', 'BH_M_M', 'U_F_S',
        #                                   'U_F_B', 'U_F_GX', 'U_F_F', 'U_F_M', 'U_M_S', 'U_M_B', 'U_M_GX', 'U_M_F',
        #                                   'U_M_M', 'OT_F_S', 'OT_F_B', 'OT_F_GX', 'OT_F_F', 'OT_F_M', 'OT_M_S',
        #                                   'OT_M_B', 'OT_M_GX', 'OT_M_F', 'OT_M_M', 'HP_F_S', 'HP_F_B', 'HP_F_GX',
        #                                   'HP_F_F', 'HP_F_M', 'HP_M_S', 'HP_M_B', 'HP_M_GX', 'HP_M_F', 'HP_M_M',
        #                                   'AI_F_S', 'AI_F_B', 'AI_F_GX', 'AI_F_F', 'AI_F_M', 'AI_M_S', 'AI_M_B',
        #                                   'AI_M_GX', 'AI_M_F', 'AI_M_M', 'AP_F_S', 'AP_F_B', 'AP_F_GX', 'AP_F_F',
        #                                   'AP_F_M', 'AP_M_S', 'AP_M_B', 'AP_M_GX', 'AP_M_F', 'AP_M_M'])
        df = pd.concat([df, dummy], axis=0, ignore_index=True)
        df = self.add_generation(df)
        return self.add_segment(df)

    @property
    def precinct_summary_df(self):
        df = self.demographics_df
        df_total = self.total(df)
        df_race = self.summarize_race(df)
        df_gen = self.summarize_gen(df)
        df_seg = self.summarize_segment(df)
        df1 = df_total.merge(df_race, on='precinct_id', how='inner')
        df1 = df1.merge(df_gen, on='precinct_id', how='inner')
        return df1.merge(df_seg, on='precinct_id', how='inner').fillna(0)

    @classmethod
    def add_generation(cls, df):
        return df.assign(gen=pd.cut(df.year_of_birth, [0, 1946, 1965, 1981, 1997, 2030],
                                    right=False,
                                    labels=['S', 'B', 'GX', 'M', 'GZ']))

    @classmethod
    def add_segment(cls, df):
        return df.assign(segment=df.race_id + '_' + df.gender + '_' + df.gen.astype(str))

    @classmethod
    def total(cls, df):
        p_total = df.groupby(['precinct_id']).size().reset_index()
        p_total.columns.name = None
        p_total = p_total.reset_index()
        p_total = p_total.rename(columns={0: 'total'})
        return p_total[p_total.precinct_id >= 0]

    @classmethod
    def summarize_race(cls, df):
        p_race = df.groupby(['precinct_id', 'race_id']).size().reset_index()
        p_race = p_race.pivot('precinct_id', 'race_id', 0).fillna(0)
        p_race.columns.name = None
        p_race = p_race.reset_index()
        return p_race[p_race.precinct_id >= 0]

    @classmethod
    def summarize_gen(cls, df):
        p_gen = df.groupby(['precinct_id', 'gen']).size().reset_index()
        p_gen = p_gen.pivot('precinct_id', 'gen', 0).fillna(0)
        p_gen.columns.name = None
        p_gen = p_gen.reset_index()
        return p_gen[p_gen.precinct_id >= 0]

    @classmethod
    def summarize_segment(cls, df):
        p_seg = df.groupby(['precinct_id', 'segment']).size().reset_index()
        p_seg = p_seg.pivot('precinct_id', 'segment', 0).fillna(0)
        p_seg.columns.name = None
        p_seg = p_seg.reset_index()
        return p_seg[p_seg.precinct_id >= 0]

    def rebuild_precinct_summary(self):
        self.db.replace_precinct_summary(self.precinct_summary_df)
