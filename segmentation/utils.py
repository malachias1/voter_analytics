import pandas as pd


def categorize_age(year_of_birth):
    return pd.cut(year_of_birth.astype(int), [0, 1946, 1965, 1981, 1997, 2030],
                  right=False,
                  labels=['S', 'B', 'GX', 'M', 'GZ'])


def remap_race(race):
    return race.apply(lambda x: 'WH' if x == 'WH' else 'BH' if x == 'BH' else 'NWB')
