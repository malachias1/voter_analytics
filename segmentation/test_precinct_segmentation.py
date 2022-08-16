import unittest
from pathlib import Path
from segmentation.precinct_segmentation import PrecinctSegmentation
from ingest.voter_list_ingest import IngestVoterList
from data.voterdb import VoterDb


class TestPrecinctSegmentation(unittest.TestCase):
    def setUp(self):
        self.root_dir = '../test_resources'
        self.voter_db_path = Path('../test_resources/ga/voter.db').expanduser()
        self.voter_db_path.unlink(missing_ok=True)
        self.db = VoterDb(self.root_dir)
        self.db.initialize()
        ivl = IngestVoterList(self.root_dir)
        ivl.ingest_county_voter_list('033')
        self.sut = PrecinctSegmentation(self.root_dir)

    def test_build_demographics_df(self):
        df = self.sut.demographics_df
        self.assertEqual(170, len(df.index))

    def test_build_demographics_df_gen(self):
        df = self.sut.demographics_df
        self.assertEqual('GX', df.loc[0, 'gen'])

    def test_build_demographics_df_segment(self):
        df = self.sut.demographics_df
        self.assertEqual('WH_F_GX', df.loc[0, 'segment'])

    def test_summarize_race(self):
        df = self.sut.demographics_df
        df = df[df.precinct_short_name >= 0]
        df1 = self.sut.summarize_race(df)
        n_wh = df[(df.race_id == 'WH') & (df.precinct_short_name == 0)]
        n_b = df[(df.race_id == 'B') & (df.precinct_short_name == 0)]
        self.assertEqual(len(n_wh.index), df1.WH[0])

    def test_summarize_segment(self):
        df = self.sut.demographics_df
        df = df[df.precinct_short_name >= 0]
        df1 = self.sut.summarize_segment(df)
        n_wh_f_gx = df[(df.segment == 'WH_F_GX') & (df.precinct_short_name == 0)]
        self.assertEqual(len(n_wh_f_gx.index), df1.WH_F_GX[0])
        self.assertEqual(2, df1.WH_F_GX[0])

    def test_segment_df(self):
        df = self.sut.demographics_df
        df = df[df.precinct_short_name >= 0]
        df_seg = self.sut.precinct_summary_df
        n_wh_f_gx = df[(df.segment == 'WH_F_GX') & (df.precinct_short_name == 0)]
        self.assertEqual(len(df.precinct_short_name.unique()), len(df_seg.index))
        self.assertEqual(len(n_wh_f_gx.index), df_seg.WH_F_GX[0])
        self.assertEqual(2, df_seg.WH_F_GX[0])
        n_wh = df[(df.race_id == 'WH') & (df.precinct_short_name == 0)]
        self.assertEqual(len(n_wh.index), df_seg.WH[0])
        self.assertEqual(0, df.isna().sum().sum())

    def test_total(self):
        df = self.sut.demographics_df
        df = df[df.precinct_short_name >= 0]
        df1 = self.sut.total(df)
        n_p_o = df[(df.precinct_short_name == 0)]
        self.assertEqual(len(n_p_o.index), df1.total[0])
        self.assertEqual(2, df1.total[0])

    def test_replace_precinct_summary(self):
        self.sut.rebuild_precinct_summary()


if __name__ == '__main__':
    unittest.main()
