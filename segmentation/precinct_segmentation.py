import pandas as pd
from segmentation.utils import categorize_age


class PrecinctSegmentation:
    def __init__(self, key='precinct_short_name'):
        self.key = key

    @classmethod
    def add_generation(cls, df):
        return df.assign(gen=categorize_age(df.year_of_birth))

    @classmethod
    def add_segment(cls, df):
        return df.assign(segment=df.race_id + '_' + df.gender + '_' + df.gen.astype(str))

    def prepare(self, df):
        df = df.drop(columns=['voter_id'])
        df = df.rename(columns={'id': self.key})
        dummy = pd.DataFrame()
        dummy_race = dummy.assign(race_id=['WH', 'BH', 'U', 'OT', 'HP', 'AI', 'AP'], key=0)
        dummy_gender = dummy.assign(gender=['F', 'M'], key=0)
        dummy_year_of_birth = dummy.assign(year_of_birth=[1922, 1946, 1965, 1981, 1997], key=0)
        dummy = dummy_race.merge(dummy_gender, on='key')
        dummy = dummy.merge(dummy_year_of_birth, on='key')
        dummy = dummy.assign(precinct_short_name='xxx-xxx').drop(columns=['key'])
        df = pd.concat([df, dummy], axis=0, ignore_index=True)
        df = self.add_generation(df)
        return self.add_segment(df)

    def summarize(self, df, year):
        df = self.prepare(df)
        df_total = self.total(df)
        df_race = self.summarize_race(df)
        df_gen = self.summarize_gen(df)
        df_seg = self.summarize_segment(df)
        df_age = df.assign(age=(year - df.year_of_birth))
        df_age = df_age[[self.key, 'age']].groupby([self.key], as_index=False).median()

        df1 = df_total.merge(df_age, on=self.key, how='inner')
        df1 = df1.merge(df_race, on=self.key, how='inner')
        df1 = df1.merge(df_gen, on=self.key, how='inner')
        df1 = df1.merge(df_seg, on=self.key, how='inner').fillna(0)
        df1 = df1.assign(s_b_gx=df1.S + df1.B + df1.GX)
        df1 = df1.assign(m_gz=df1.M + df1.GZ)
        df1 = df1.assign(hp_ai_ap_o=df1.HP + df1.AI + df1.AP + df1.OT)
        return df1

    def summarize_race(self, df):
        p_race = df.groupby([self.key, 'race_id'], as_index=False).size()
        p_race = p_race.pivot(index=self.key, columns='race_id', values='size').fillna(0)
        p_race.columns.name = None
        p_race = p_race.reset_index()
        return p_race[p_race.precinct_short_name != 'xxx-xxx']

    def summarize_gen(self, df):
        p_gen = df.groupby([self.key, 'gen'], as_index=False).size()
        p_gen = p_gen.pivot(index=self.key, columns='gen', values='size').fillna(0)
        p_gen.columns.name = None
        p_gen = p_gen.reset_index()
        return p_gen[p_gen.precinct_short_name != 'xxx-xxx']

    def summarize_segment(self, df):
        p_seg = df.groupby([self.key, 'segment'], as_index=False).size()
        p_seg = p_seg.pivot(index=self.key, columns='segment', values='size').fillna(0)
        p_seg.columns.name = None
        p_seg = p_seg.reset_index()
        return p_seg[p_seg.precinct_short_name != 'xxx-xxx']

    def total(self, df):
        p_total = df.groupby([self.key], as_index=False).size()
        p_total.columns.name = None
        p_total = p_total.rename(columns={'size': 'total'})
        return p_total[p_total[self.key] != 'xxx-xxx']
