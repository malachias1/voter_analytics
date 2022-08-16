from pathlib import Path
from psycopg2.extras import execute_values
import pandas as pd
from data.pathes import Pathes
from django.db import connections
from django.db.utils import OperationalError
from psycopg2.extensions import register_adapter, AsIs
import numpy


def adapt_numpy_bool(numpy_bool):
    return AsIs(numpy_bool)


def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.bool_, adapt_numpy_bool)
register_adapter(numpy.float64, adapt_numpy_float64)
register_adapter(numpy.int64, adapt_numpy_int64)


class VoterDb:
    def __init__(self, conn_name='default'):
        """
        Check to see if the connection works.
        Get a cursor first because connection
        is a lazy initialization.
        :param conn_name: the name of the connection to use in settings.
        """
        con = connections[conn_name]
        try:
            c = con.cursor()
            del c
            self.con = con.connection
        except OperationalError as e:
            raise RuntimeError(f'Unable to connect to db: {str(e)}')

    @property
    def contests(self):
        cur = self.cursor()
        cur.execute(f"select id, election_date, contest from election_results")
        results = cur.fetchall()
        return pd.DataFrame.from_records(results, columns=['id', 'election_date', 'contest'])

    @property
    def county_maps(self):
        return pd.read_sql_query(f"select * from county_map", self.con)

    @property
    def election_results(self):
        return pd.read_sql_query(f"select * from election_results", self.con)

    @property
    def election_result_details(self):
        return pd.read_sql_query(f"select * from election_result_details", self.con)

    @property
    def hse_maps(self):
        return pd.read_sql_query(f"select * from hse_map", self.con)

    @property
    def next_contest_class_id(self):
        df = pd.read_sql_query(f"select max(id) from contest_class", self.con)
        if len(df) > 0 and df.iloc[0, 0] is not None:
            return df.iloc[0, 0] + 1
        return 0

    @property
    def next_precinct_id(self):
        df = pd.read_sql_query(f"select max(id) from precinct_details", self.con)
        if len(df) > 0 and df.iloc[0, 0] is not None:
            return df.iloc[0, 0] + 1
        return 0

    @property
    def next_vtd_id(self):
        if self.vtd_maps_exists:
            df = pd.read_sql_query(f"select max(id) from vtd_map", self.con)
            if len(df) > 0 and df.iloc[0, 0] is not None:
                return df.iloc[0, 0] + 1
        return 0

    @property
    def over_under_votes(self):
        return pd.read_sql_query(f"select * from election_results_over_under", self.con)

    @property
    def precinct_details(self):
        cur = self.cursor()
        cur.execute(f"""
            select id, county_code, precinct_id, precinct_name 
                from precinct_details
        """)
        results = cur.fetchall()
        return pd.DataFrame.from_records(results, columns=['id', 'county_code', 'precinct_id', 'precinct_name'])

    @property
    def precinct_summary(self):
        cur = self.cursor()
        # Because I have mixed case or upper case column names
        # I need to quote the column names.
        cur.execute(f"""
            select precinct_id, total, "AP", "AI", "HP", "BH", "OT", "U", "WH", "S", "B", "GX", "M", "GZ", "WH_F_S", 
            "WH_F_B",
                    "WH_F_GX", "WH_F_M", "WH_F_GZ", "WH_M_S", "WH_M_B", "WH_M_GX", "WH_M_M", "WH_M_GZ", "BH_F_S",
                    "BH_F_B", "BH_F_GX", "BH_F_M", "BH_F_GZ", "BH_M_S", "BH_M_B", "BH_M_GX", "BH_M_M", "BH_M_GZ",
                    "U_F_S", "U_F_B", "U_F_GX", "U_F_M", "U_F_GZ", "U_M_S", "U_M_B", "U_M_GX", "U_M_M", "U_M_GZ", 
                    "OT_F_S",
                    "OT_F_B", "OT_F_GX", "OT_F_M", "OT_F_GZ", "OT_M_S", "OT_M_B", "OT_M_GX", "OT_M_M", "OT_M_GZ", 
                    "HP_F_S",
                    "HP_F_B", "HP_F_GX", "HP_F_M", "HP_F_GZ", "HP_M_S", "HP_M_B", "HP_M_GX", "HP_M_M", "HP_M_GZ", 
                    "AI_F_S",
                    "AI_F_B", "AI_F_GX", "AI_F_M", "AI_F_GZ", "AI_M_S", "AI_M_B", "AI_M_GX", "AI_M_M", "AI_M_GZ", 
                    "AP_F_S",
                    "AP_F_B", "AP_F_GX", "AP_F_M", "AP_F_GZ", "AP_M_S", "AP_M_B", "AP_M_GX", "AP_M_M", "AP_M_GZ" 
                from precinct_summary
        """)
        results = cur.fetchall()
        return pd.DataFrame.from_records(results, columns=['precinct_id', 'total', 'AP', 'AI', 'HP', 'BH', 'OT', 'U',
                                                           'WH', 'S', 'B', 'GX', 'M', 'GZ', 'WH_F_S', 'WH_F_B',
                                                           'WH_F_GX', 'WH_F_M', 'WH_F_GZ', 'WH_M_S', 'WH_M_B',
                                                           'WH_M_GX', 'WH_M_M', 'WH_M_GZ', 'BH_F_S', 'BH_F_B',
                                                           'BH_F_GX', 'BH_F_M', 'BH_F_GZ', 'BH_M_S', 'BH_M_B',
                                                           'BH_M_GX', 'BH_M_M', 'BH_M_GZ', 'U_F_S', 'U_F_B',
                                                           'U_F_GX', 'U_F_M', 'U_F_GZ', 'U_M_S', 'U_M_B', 'U_M_GX',
                                                           'U_M_M', 'U_M_GZ', 'OT_F_S', 'OT_F_B', 'OT_F_GX', 'OT_F_M',
                                                           'OT_F_GZ', 'OT_M_S', 'OT_M_B', 'OT_M_GX', 'OT_M_M',
                                                           'OT_M_GZ', 'HP_F_S', 'HP_F_B', 'HP_F_GX', 'HP_F_M',
                                                           'HP_F_GZ', 'HP_M_S', 'HP_M_B', 'HP_M_GX', 'HP_M_M',
                                                           'HP_M_GZ', 'AI_F_S', 'AI_F_B', 'AI_F_GX', 'AI_F_M',
                                                           'AI_F_GZ', 'AI_M_S', 'AI_M_B', 'AI_M_GX', 'AI_M_M',
                                                           'AI_M_GZ', 'AP_F_S', 'AP_F_B', 'AP_F_GX', 'AP_F_M',
                                                           'AP_F_GZ', 'AP_M_S', 'AP_M_B', 'AP_M_GX', 'AP_M_M',
                                                           'AP_M_GZ'])


    def initialize(self):
        """
        Execute schema DDL. The method is idempotent.

        :return: None
        """
        schema_path = Path(Path(__file__).parent, 'voterdb.sql')
        with schema_path.open('r') as f:
            schema_ddl = f.read()
            self.run_script(schema_ddl)
        county_details = pd.read_csv(Path(self.root_dir, 'county_details.csv'))
        county_details.to_sql('county_details', self.con)

    def cursor(self):
        return self.con.cursor()

    def fetchall(self, q):
        cur = self.cursor()
        cur.execute(q)
        return cur.fetchall()

    def fetchone(self, q):
        cur = self.cursor()
        cur.execute(q)
        return cur.fetchone()

    def get_county_map(self, county_code):
        return pd.read_sql_query(f"select * from county_map where county_code='{county_code}'", self.con)

    def get_election_results_for_category(self, election_date, category, subcategory):
        election_results = pd.read_sql_query(f"""
        select * from election_results where election_date='{election_date}' and contest in (
            select contest from contest_class where election_date='{election_date}' and
                category='{category}' and subcategory='{subcategory}'
            )
        """, self.con)
        return election_results

    def get_election_results_for_contest(self, election_date, contest):
        election_results = pd.read_sql_query(f"""
        select * from election_results where election_date='{election_date}' and contest='{contest}'
        """, self.con)
        return election_results

    def get_precinct_summary(self, county_code):
        ps = pd.read_sql_query(f"""
        select pd.precinct_id as x, ps.* from precinct_summary as ps 
            join precinct_details as pd on ps.precinct_id = pd.id 
            where county_code='{county_code}'
        """, self.con).drop(columns='precinct_id').rename(columns={'x': 'precinct_id'})
        return ps

    def get_vtd_map(self, county_code):
        return pd.read_sql_query(f"select * from vtd_map where county_code='{county_code}'", self.con)

    def voter_history_for_date(self, year, month, day):
        date = f'{year}{month:02d}{day:02d}'
        return pd.read_sql_query(f"select * from voter_history where date='{date}'", self.con)

    def get_voter_history_summary(self, limit=None):
        stmt = f"select * from voter_history_summary"
        if limit is not None:
            stmt += f' limit {limit}'
        return pd.read_sql_query(stmt, self.con, dtype="string")

    def get_voter_score(self):
        return pd.read_sql_query(f"select * from voter_score", self.con)

    def get_election_results(self, election_date):
        return pd.read_sql_query(f"select * from election_results where election_date='{election_date}'", self.con)

    def delete_election_result_over_under(self, election_date, county):
        cur = self.con.cursor()
        cur.execute(
            f"delete from election_results_over_under where election_date='{election_date}' and county='{county}'")
        self.con.commit()

    def get_election_results_over_under(self, election_date):
        return pd.read_sql_query(f"select * from election_results_over_under where election_date='{election_date}'",
                                 self.con)

    # -------------------------------------------------------------------------
    # Update methods
    # -------------------------------------------------------------------------

    def delete_voters_from_table(self, voters, table):
        if 'voter_id' not in voters.columns:
            raise ValueError('voter_id missing from addresses!')
        ids = voters[['voter_id']].to_records(index=False)
        with self.con:
            with self.con.cursor() as cur:
                execute_values(cur, f"""
                    delete from {table} where voter_id in (%s)
                """, ids)

    def insert_election_results(self, results):
        # Purge election results for county and date
        purge = results[['election_date', 'county_code']].drop_duplicates()
        if len(purge.index) != 1:
            raise ValueError(f"Election results contain multiple counties, {', '.join(purge.county_code.unique())}.")
        self.executemany(f"delete from election_results where election_date=? and county_code=?",
                         purge.values.tolist())

        # Ensure proper order of columns and insert results
        results = results[['election_date', 'county_code', 'contest', 'choice', 'party',
                           'is_question', 'precinct_name', 'vote_type', 'votes']]
        stmt = f"""
        insert into election_result_details ('election_date', 'county_code', 'contest', 'choice', 'party',
               'is_question', 'precinct_name','vote_type','votes')
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(stmt, results.values.tolist())

        results_summary = results[['election_date', 'county_code', 'contest', 'choice', 'party',
                                   'is_question', 'precinct_name', 'votes']]
        results_summary = results_summary.groupby(['election_date', 'county_code', 'contest', 'choice', 'party',
                                                   'is_question', 'precinct_name'], dropna=False).sum()
        results_summary = results_summary.rename(columns={0: 'votes'}).reset_index()

        stmt = f"""
        insert into election_results ('election_date', 'county_code', 'contest', 'choice', 'party',
               'is_question', 'precinct_name','votes')
                values (?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(stmt, results_summary.values.tolist())

    def insert_election_results_over_under(self, over_under):
        # Purge election results over and under counts
        # county and date
        purge = over_under[['election_date', 'county_code']].drop_duplicates()
        if len(purge.index) != 1:
            raise ValueError(f"Election results over and under counts contain multiple counties, "
                             f"{', '.join(purge.county.unique())}.")
        self.executemany(f"delete from election_results_over_under where election_date=? and county_code=?",
                         purge.values.tolist())

        # Ensure proper order of columns and insert over and under counts
        over_under = over_under[['election_date', 'county_code', 'contest',
                                 'precinct_name', 'overvotes', 'undervotes']]
        stmt = f"""
         insert into election_results_over_under ('election_date', 'county_code', 'contest',
               'precinct_name', 'overvotes', 'undervotes')
               values (?, ?, ?, ?, ?, ?)

       """
        self.executemany(stmt, over_under.values.tolist())

    def replace_contest_class(self, df):
        df = df[['id', 'election_date', 'contest', 'category', 'canonical_name', 'type',
                 'subcategory', 'party', 'is_question', 'ambiguous']]
        pruge_script = f"""
        delete from contest_class;
        """
        self.run_script(pruge_script)

        stmt = f"""insert into contest_class (id, election_date, contest, category, 
                                              canonical_name, type, subcategory, party, 
                                              is_question, ambiguous)
                   values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_contest_class_map(self, df):
        df = df[['election_date', 'election_result_id', 'contest_class_id']]
        pruge_script = f"""
        delete from contest_class_map;
        """
        self.run_script(pruge_script)

        stmt = f"""insert into contest_class_map (election_date, election_result_id, contest_class_id)
                   values (?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_precinct_details(self, df):
        """
        Update precinct details for a county. Raise an
        error if multiple counties are present in df.
        Purge precinct_details. Put colums in correct order.

        :param df:
        :return:
        """
        df = df[['id', 'county_code', 'precinct_id']]
        self.check_county_code_integrity(df)
        county_code = df.county_code.unique()[0]
        pruge_script = f"""
        delete from precinct_details where county_code='{county_code}';
        """
        self.run_script(pruge_script)
        stmt = f"""insert into precinct_details (id, county_code, precinct_id)
                   values (?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_cng(self, df):
        df = df[['voter_id', 'cng']]
        stmt = f"""
            replace into voter_cng (voter_id, cng)
            values (?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_demographics(self, df):
        df = df[['voter_id', 'race_id', 'gender', 'year_of_birth']]
        stmt = f"""
            replace into voter_demographics (voter_id, race_id, gender, year_of_birth)
            values (?, ?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_hse(self, df):
        df = df[['voter_id', 'hse']]
        stmt = f"""
            replace into voter_hse (voter_id, hse)
            values (?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_name(self, df):
        """
        Delete rows in df.voter_id, then insert
        name details for the voter. Ensure
        correct column
        :param df: name details for voter
        :return: None
        """
        with self.con:
            with self.cursor() as cur:
                execute_values(cur, f"""
                    delete from voter_name where voter_id in (%s)
                """, df[['voter_id']].values.tolist())
        with self.con:
            with self.cursor() as cur:
                execute_values(cur, f"""
                      insert into voter_name (voter_id, last_name, first_name, 
                                               middle_name, name_suffix, name_title)
                      values %s
                    """, df[['voter_id', 'last_name', 'first_name',
                             'middle_name', 'name_suffix', 'name_title']].values.tolist())

    def replace_multiple_voter_precinct(self, df):
        df = df[['voter_id', 'precinct_id']]
        # values = df.apply(lambda row: row.tolist(), axis=1)
        stmt = f"""
          replace into voter_precinct (voter_id, precinct_id) 
          values (?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_sen(self, df):
        df = df[['voter_id', 'sen']]
        stmt = f"""
            replace into voter_sen (voter_id, sen)
            values (?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_multiple_voter_status(self, df):
        df = df[['voter_id', 'status', 'status_reason', 'date_added',
                 'date_changed', 'registration_date', 'last_contact_date']]
        stmt = f"""
            replace into voter_status (voter_id, status, status_reason, date_added,
                                       date_changed, registration_date, last_contact_date) 
            values (?, ?, ?, ?, ?, ?, ?)
        """
        self.executemany(stmt, df.values.tolist())

    def replace_precinct_summary(self, df):
        values = df.to_dict(orient='records')
        truncate_script = f"""
        delete from precinct_summary;
        """
        self.run_script(truncate_script)

        stmt = f"""
        insert into precinct_summary (
            precinct_id, total, AP, AI, HP, BH, OT, U, WH, S, B, GX, M, GZ, WH_F_S, WH_F_B, 
            WH_F_GX, WH_F_M, WH_F_GZ, WH_M_S, WH_M_B, WH_M_GX, WH_M_M, WH_M_GZ, BH_F_S, 
            BH_F_B, BH_F_GX, BH_F_M, BH_F_GZ, BH_M_S, BH_M_B, BH_M_GX, BH_M_M, BH_M_GZ, 
            U_F_S, U_F_B, U_F_GX, U_F_M, U_F_GZ, U_M_S, U_M_B, U_M_GX, U_M_M, U_M_GZ, 
            OT_F_S, OT_F_B, OT_F_GX, OT_F_M, OT_F_GZ, OT_M_S, OT_M_B, OT_M_GX, OT_M_M, 
            OT_M_GZ, HP_F_S, HP_F_B, HP_F_GX, HP_F_M, HP_F_GZ, HP_M_S, HP_M_B, HP_M_GX, 
            HP_M_M, HP_M_GZ, AI_F_S, AI_F_B, AI_F_GX, AI_F_M, AI_F_GZ, AI_M_S, AI_M_B, AI_M_GX, 
            AI_M_M, AI_M_GZ, AP_F_S, AP_F_B, AP_F_GX, AP_F_M, AP_F_GZ, AP_M_S, AP_M_B, AP_M_GX, 
            AP_M_M, AP_M_GZ
        )
        values(
            :precinct_id, :total, :AP, :AI, :HP, :BH, :OT, :U, :WH, :S, :B, :GX, :M, :GZ, 
            :WH_F_S, :WH_F_B, :WH_F_GX, :WH_F_M, :WH_F_GZ, :WH_M_S, :WH_M_B, :WH_M_GX, 
            :WH_M_M, :WH_M_GZ, :BH_F_S, :BH_F_B, :BH_F_GX, :BH_F_M, :BH_F_GZ, :BH_M_S, 
            :BH_M_B, :BH_M_GX, :BH_M_M, :BH_M_GZ, :U_F_S, :U_F_B, :U_F_GX, :U_F_M, :U_F_GZ, 
            :U_M_S, :U_M_B, :U_M_GX, :U_M_M, :U_M_GZ, :OT_F_S, :OT_F_B, :OT_F_GX, :OT_F_M, 
            :OT_F_GZ, :OT_M_S, :OT_M_B, :OT_M_GX, :OT_M_M, :OT_M_GZ, :HP_F_S, :HP_F_B, :HP_F_GX, 
            :HP_F_M, :HP_F_GZ, :HP_M_S, :HP_M_B, :HP_M_GX, :HP_M_M, :HP_M_GZ, :AI_F_S, :AI_F_B, 
            :AI_F_GX, :AI_F_M, :AI_F_GZ, :AI_M_S, :AI_M_B, :AI_M_GX, :AI_M_M, :AI_M_GZ, :AP_F_S, 
            :AP_F_B, :AP_F_GX, :AP_F_M, :AP_F_GZ, :AP_M_S, :AP_M_B, :AP_M_GX, :AP_M_M, :AP_M_GZ
        )
        """
        self.executemany(stmt, values)

    def run_script(self, script):
        cur = self.con.cursor()
        for stmt in script.split(';'):
            # print(stmt)
            cur.execute(stmt)
        self.con.commit()

    def executemany(self, stmt, values):
        cur = self.con.cursor()
        cur.executemany(stmt, values)
        self.con.commit()

    @classmethod
    def check_county_code_integrity(cls, df):
        check = df.county_code.unique()
        if len(check) > 1:
            raise ValueError('More than one county code in voter list!')
