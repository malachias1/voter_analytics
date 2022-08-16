import pandas as pd
from segmentation.utils import categorize_age


class PrecinctSegmentation:
    @classmethod
    def add_generation(cls, df):
        return df.assign(gen=categorize_age(df.year_of_birth))

    @classmethod
    def add_segment(cls, df):
        return df.assign(segment=df.race_id + '_' + df.gender + '_' + df.gen.astype(str))

    @classmethod
    def prepare(cls, df):
        df = df.drop(columns=['voter_id'])
        df = df.rename(columns={'id': 'precinct_short_name'})
        dummy = pd.DataFrame()
        dummy_race = dummy.assign(race_id=['WH', 'BH', 'U', 'OT', 'HP', 'AI', 'AP'], key=0)
        dummy_gender = dummy.assign(gender=['F', 'M'], key=0)
        dummy_year_of_birth = dummy.assign(year_of_birth=[1922, 1946, 1965, 1981, 1997], key=0)
        dummy = dummy_race.merge(dummy_gender, on='key')
        dummy = dummy.merge(dummy_year_of_birth, on='key')
        dummy = dummy.assign(precinct_short_name='xxx-xxx').drop(columns=['key'])
        df = pd.concat([df, dummy], axis=0, ignore_index=True)
        df = cls.add_generation(df)
        return cls.add_segment(df)

    @classmethod
    def summarize(cls, df, year):
        df = cls.prepare(df)
        df_total = cls.total(df)
        df_race = cls.summarize_race(df)
        df_gen = cls.summarize_gen(df)
        df_seg = cls.summarize_segment(df)
        df_age = df.assign(age=(year - df.year_of_birth))
        df_age = df_age[['precinct_short_name', 'age']].groupby(['precinct_short_name'], as_index=False).median()

        df1 = df_total.merge(df_age, on='precinct_short_name', how='inner')
        df1 = df1.merge(df_race, on='precinct_short_name', how='inner')
        df1 = df1.merge(df_gen, on='precinct_short_name', how='inner')
        df1 = df1.merge(df_seg, on='precinct_short_name', how='inner').fillna(0)
        df1 = df1.assign(s_b_gx=df1.S + df1.B + df1.GX)
        df1 = df1.assign(m_gz=df1.M + df1.GZ)
        df1 = df1.assign(hp_ai_ap_o=df1.HP + df1.AI + df1.AP + df1.OT)
        return df1

    @classmethod
    def summarize_race(cls, df):
        p_race = df.groupby(['precinct_short_name', 'race_id'], as_index=False).size()
        p_race = p_race.pivot(index='precinct_short_name', columns='race_id', values='size').fillna(0)
        p_race.columns.name = None
        p_race = p_race.reset_index()
        return p_race[p_race.precinct_short_name != 'xxx-xxx']

    @classmethod
    def summarize_gen(cls, df):
        p_gen = df.groupby(['precinct_short_name', 'gen'], as_index=False).size()
        p_gen = p_gen.pivot(index='precinct_short_name', columns='gen', values='size').fillna(0)
        p_gen.columns.name = None
        p_gen = p_gen.reset_index()
        return p_gen[p_gen.precinct_short_name != 'xxx-xxx']

    @classmethod
    def summarize_segment(cls, df):
        p_seg = df.groupby(['precinct_short_name', 'segment'], as_index=False).size()
        p_seg = p_seg.pivot(index='precinct_short_name', columns='segment', values='size').fillna(0)
        p_seg.columns.name = None
        p_seg = p_seg.reset_index()
        return p_seg[p_seg.precinct_short_name != 'xxx-xxx']

    @classmethod
    def total(cls, df):
        p_total = df.groupby(['precinct_short_name'], as_index=False).size()
        p_total.columns.name = None
        p_total = p_total.rename(columns={'size': 'total'})
        return p_total[p_total.precinct_short_name != 'xxx-xxx']
