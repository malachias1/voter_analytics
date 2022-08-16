import time
import json
from datetime import date, datetime
from pathlib import Path
import pandas as pd

from county.models import County
from precinct.models import Precinct, PrecinctEdition
from util.address_skills import AddressSkills
from voter.models import MailingAddress, ResidenceAddress, OtherJurisdictions, Voter, ListEdition
from util.addresses import StreetNameNormalizer


class VoterListLoader:
    def __init__(self, path, edition_date: date, precinct_edition_year):
        self.batch_size = 10000
        self.path = Path(path)
        self.edition_date = edition_date
        self.precinct_edition_year = precinct_edition_year
        self.counties = {x.county_code: x for x in County.objects.all()}
        self.precincts = self.get_precinct_map(self.precinct_edition_year)
        self.edition = None
        self.voters = None

    @classmethod
    def on_bad_line(cls, badline):
        print(badline)
        return None

    def check_voter_list(self):
        report = {}
        # precincts = cls.get_precinct_map(precinct_edition_year)
        df_orig = self.read_csv()

        # Filter out voters with either missing or invalid addresses.
        df = df_orig[df_orig.CONGRESSIONAL_DISTRICT != '99999']

        report['bad_address_count'] = len(df_orig.index) - len(df.index)
        report['max_field_widths'] = {
            'LAST_NAME'.lower(): self.to_int(df.LAST_NAME.str.len().dropna().max()),
            'FIRST_NAME'.lower(): self.to_int(df.FIRST_NAME.str.len().dropna().max()),
            'MIDDLE_MAIDEN_NAME'.lower(): self.to_int(df.MIDDLE_MAIDEN_NAME.str.len().dropna().max()),
            'NAME_SUFFIX'.lower(): self.to_int(df.NAME_SUFFIX.str.len().dropna().max()),
            'NAME_TITLE'.lower(): self.to_int(df.NAME_TITLE.str.len().dropna().max()),
            'RESIDENCE_HOUSE_NUMBER'.lower(): self.to_int(df.RESIDENCE_HOUSE_NUMBER.str.len().dropna().max()),
            'RESIDENCE_STREET_NAME'.lower(): self.to_int(df.RESIDENCE_STREET_NAME.str.len().dropna().max()),
            'RESIDENCE_STREET_SUFFIX'.lower(): self.to_int(df.RESIDENCE_STREET_SUFFIX.str.len().dropna().max()),
            'RESIDENCE_APT_UNIT_NBR'.lower(): self.to_int(df.RESIDENCE_APT_UNIT_NBR.str.len().dropna().max()),
            'RESIDENCE_CITY'.lower(): self.to_int(df.RESIDENCE_CITY.str.len().dropna().max()),
            'RACE'.lower(): self.to_int(df.RACE.str.len().dropna().max()),
            'GENDER'.lower(): self.to_int(df.GENDER.str.len().dropna().max()),
            'LAND_DISTRICT'.lower(): self.to_int(df.LAND_DISTRICT.str.len().dropna().max()),
            'LAND_LOT'.lower(): self.to_int(df.LAND_LOT.str.len().dropna().max()),
            'STATUS_REASON'.lower(): self.to_int(df.STATUS_REASON.str.len().dropna().max()),
            'COUNTY_PRECINCT_ID'.lower(): self.to_int(df.COUNTY_PRECINCT_ID.str.len().dropna().max()),
            'CITY_PRECINCT_ID'.lower(): self.to_int(df.CITY_PRECINCT_ID.str.len().dropna().max()),
            'JUDICIAL_DISTRICT'.lower(): self.to_int(df.JUDICIAL_DISTRICT.str.len().dropna().max()),
            'COMMISSION_DISTRICT'.lower(): self.to_int(df.COMMISSION_DISTRICT.str.len().dropna().max()),
            'SCHOOL_DISTRICT'.lower(): self.to_int(df.SCHOOL_DISTRICT.str.len().dropna().max()),
            'COUNTY_DISTRICTA_NAME'.lower(): self.to_int(df.COUNTY_DISTRICTA_NAME.str.len().dropna().max()),
            'COUNTY_DISTRICTA_VALUE'.lower(): self.to_int(df.COUNTY_DISTRICTA_VALUE.str.len().dropna().max()),
            'COUNTY_DISTRICTB_NAME'.lower(): self.to_int(df.COUNTY_DISTRICTB_NAME.str.len().dropna().max()),
            'COUNTY_DISTRICTB_VALUE'.lower(): self.to_int(df.COUNTY_DISTRICTB_VALUE.str.len().dropna().max()),
            'MUNICIPAL_NAME'.lower(): self.to_int(df.MUNICIPAL_NAME.str.len().dropna().max()),
            'MUNICIPAL_CODE'.lower(): self.to_int(df.MUNICIPAL_CODE.str.len().dropna().max()),
            'WARD_CITY_COUNCIL_NAME'.lower(): self.to_int(df.WARD_CITY_COUNCIL_NAME.str.len().dropna().max()),
            'WARD_CITY_COUNCIL_CODE'.lower(): self.to_int(df.WARD_CITY_COUNCIL_CODE.str.len().dropna().max()),
            'CITY_SCHOOL_DISTRICT_NAME'.lower(): self.to_int(df.CITY_SCHOOL_DISTRICT_NAME.str.len().dropna().max()),
            'CITY_SCHOOL_DISTRICT_VALUE'.lower(): self.to_int(df.CITY_SCHOOL_DISTRICT_VALUE.str.len().dropna().max()),
            'CITY_DISTA_NAME'.lower(): self.to_int(df.CITY_DISTA_NAME.str.len().dropna().max()),
            'CITY_DISTA_VALUE'.lower(): self.to_int(df.CITY_DISTA_VALUE.str.len().dropna().max()),
            'CITY_DISTB_NAME'.lower(): self.to_int(df.CITY_DISTB_NAME.str.len().dropna().max()),
            'CITY_DISTB_VALUE'.lower(): self.to_int(df.CITY_DISTB_VALUE.str.len().dropna().max()),
            'CITY_DISTC_NAME'.lower(): self.to_int(df.CITY_DISTC_NAME.str.len().dropna().max()),
            'CITY_DISTC_VALUE'.lower(): self.to_int(df.CITY_DISTC_VALUE.str.len().dropna().max()),
            'CITY_DISTD_NAME'.lower(): self.to_int(df.CITY_DISTD_NAME.str.len().dropna().max()),
            'CITY_DISTD_VALUE'.lower(): self.to_int(df.CITY_DISTD_VALUE.str.len().dropna().max()),
            'DISTRICT_COMBO'.lower(): self.to_int(df.DISTRICT_COMBO.str.len().dropna().max()),
            'MAIL_HOUSE_NBR'.lower(): self.to_int(df.MAIL_HOUSE_NBR.str.len().dropna().max()),
            'MAIL_STREET_NAME'.lower(): self.to_int(df.MAIL_STREET_NAME.str.len().dropna().max()),
            'MAIL_APT_UNIT_NBR'.lower(): self.to_int(df.MAIL_APT_UNIT_NBR.str.len().dropna().max()),
            'MAIL_CITY'.lower(): self.to_int(df.MAIL_CITY.str.len().dropna().max()),
            'MAIL_STATE'.lower(): self.to_int(df.MAIL_STATE.str.len().dropna().max()),
            'MAIL_ZIPCODE'.lower(): self.to_int(df.MAIL_ZIPCODE.str.len().dropna().max()),
            'MAIL_ADDRESS_2'.lower(): df.MAIL_ADDRESS_2.str.len().dropna().max(),
            'MAIL_ADDRESS_3'.lower(): df.MAIL_ADDRESS_3.str.len().dropna().max(),
            'MAIL_COUNTRY'.lower(): self.to_int(df.MAIL_COUNTRY.str.len().dropna().max())
        }

        missing_precincts = {}
        missing = set()
        count = 0
        for i in df.index:
            county_code = df.loc[i].COUNTY_CODE
            if county_code not in missing_precincts:
                missing_precincts[county_code] = set()
            precinct_id = df.loc[i].COUNTY_PRECINCT_ID
            key = f'{county_code}_{precinct_id}'
            if key in self.precincts:
                continue
            count += 1
            missing.add(key)
            missing_precincts[county_code].add(precinct_id)
        report['missing_precincts'] = {k: list(v) for k, v in missing_precincts.items()}
        report['total_missing_precincts'] = len(missing)
        report['voters_with_missing_precincts'] = count

        with self.path.with_suffix('.log').open('w') as f:
            json.dump(report, f, indent=2)

    def get_precinct_map(self, county=None):
        if not PrecinctEdition.objects.filter(year=self.precinct_edition_year).exists():
            raise KeyError(f'No precinct edition exists for {self.precinct_edition_year}!')
        precinct_edition = PrecinctEdition.objects.get(year=self.precinct_edition_year)
        precincts = Precinct.objects.filter(edition=precinct_edition)
        if county is not None:
            precincts = precincts.filter(county=county)
        return {f'{x.county.county_code}_{x.precinct_id}': x for x in precincts}

    def load_mailing_addresses(self, df):
        count = 0
        addresses = []
        for a in self.next_mailing_address(df):
            # If next address is None, there is no
            # separate mailing address
            if a is None:
                continue
            addresses.append(a)
            # bulk create has a batch size, but I'm being
            # a little careful on memory, who knows it might
            # matter
            if len(addresses) >= self.batch_size:
                MailingAddress.objects.bulk_create(addresses)
                count += len(addresses)
                print(f'{count} MailingAddress loaded ...')
                addresses = []
        MailingAddress.objects.bulk_create(addresses)
        count += len(addresses)
        print(f'{count} MailingAddress loaded ... done!')

    def load_other_jurisdictions(self, df):
        count = 0
        others = []
        for o in self.next_other_jurisdiction(df):
            others.append(o)
            # bulk create has a batch size, but I'm being
            # a little careful on memory, who knows it might
            # matter
            if len(others) >= self.batch_size:
                OtherJurisdictions.objects.bulk_create(others)
                count += len(others)
                print(f'{count} OtherJurisdictions loaded ...')
                others = []
        OtherJurisdictions.objects.bulk_create(others)
        count += len(others)
        print(f'{count} OtherJurisdictions loaded ... done!')

    def load_residence_addresses(self, df):
        count = 0
        addresses = []
        for a in self.next_residence_address(df):
            addresses.append(a)
            # bulk create has a batch size, but I'm being
            # a little careful on memory, who knows it might
            # matter
            if len(addresses) >= self.batch_size:
                ResidenceAddress.objects.bulk_create(addresses)
                count += len(addresses)
                print(f'{count} ResidenceAddress loaded ...')
                addresses = []
        ResidenceAddress.objects.bulk_create(addresses)
        count += len(addresses)
        print(f'{count} ResidenceAddress loaded ... done!')

    def load_state(self, comments, voters=True, jurisdictions=True,
                   mailing_addresses=True, residence_addresses=True):
        start = time.perf_counter()
        self.edition = self.update_edition(comments, exists_ok=True)
        end = time.perf_counter()
        print(f'load_state setup time: {end - start:.1f}.')

        start = end
        # Do not interpret NA values. One person's name is Null!
        # After reading, convert all zero-length strings to None.
        df_orig = self.read_csv()
        end = time.perf_counter()
        print(f'load_state read_csv time: {end - start:.1f}.')

        start = end
        df_orig = df_orig.where(df_orig != '', None)
        # Filter out voters with either missing or invalid addresses.
        df = df_orig[df_orig.CONGRESSIONAL_DISTRICT != '99999']
        print(f'load_state cleanup time: {end - start:.1f}.')

        if voters:
            start = end
            Voter.objects.filter(edition=self.edition).delete()
            end = time.perf_counter()
            print(f'load_state purge time: {end - start:.1f}.')

            start = end
            self.load_voters(df)
            end = time.perf_counter()
            print(f'load_state load_voters time: {end - start:.1f}.')

        start = end
        self.voters = {x.voter_id: x for x in Voter.objects.filter(edition=self.edition)}
        end = time.perf_counter()
        print(f'load_state create voter id map time: {end - start:.1f}.')

        if jurisdictions:
            start = end
            self.load_other_jurisdictions(df)
            end = time.perf_counter()
            print(f'load_state load_other_jurisdictions time: {end - start:.1f}.')

        if mailing_addresses:
            start = end
            self.load_mailing_addresses(df)
            end = time.perf_counter()
            print(f'load_state load_mailing_addresses time: {end - start:.1f}.')

        if residence_addresses:
            start = end
            self.load_residence_addresses(df)
            end = time.perf_counter()
            print(f'load_state load_residence_addresses time: {end - start:.1f}.')

    def load_voters(self, df):
        count = 0
        voters = []
        for v in self.next_voter(df):
            voters.append(v)
            # bulk create has a batch size, but I'm being
            # a little careful on memory, who knows it might
            # matter
            if len(voters) >= self.batch_size:
                Voter.objects.bulk_create(voters)
                count += len(voters)
                print(f'{count} Voter loaded ...')
                voters = []
        Voter.objects.bulk_create(voters)
        count += len(voters)
        print(f'{count} Voter loaded ... done!')

    def next_other_jurisdiction(self, df):
        for i in df.index:
            row = df.loc[i]
            county_code = row.COUNTY_CODE
            county = self.counties[county_code]
            voter_id = row.REGISTRATION_NUMBER
            voter = self.voters[voter_id]
            yield OtherJurisdictions(
                voter=voter,
                county=county,
                land_district=row.LAND_DISTRICT,
                land_lot=row.LAND_LOT,
                city_precinct_id=row.CITY_PRECINCT_ID,
                judicial_district=row.JUDICIAL_DISTRICT,
                commission_district=row.COMMISSION_DISTRICT,
                school_district=row.SCHOOL_DISTRICT,
                county_districta_name=row.COUNTY_DISTRICTA_NAME,
                county_districta_value=row.COUNTY_DISTRICTA_VALUE,
                county_districtb_name=row.COUNTY_DISTRICTB_NAME,
                county_districtb_value=row.COUNTY_DISTRICTB_VALUE,
                municipal_name=row.MUNICIPAL_NAME,
                municipal_code=row.MUNICIPAL_CODE,
                ward_city_council_name=row.WARD_CITY_COUNCIL_NAME,
                ward_city_council_code=row.WARD_CITY_COUNCIL_CODE,
                city_school_district_name=row.CITY_SCHOOL_DISTRICT_NAME,
                city_school_district_value=row.CITY_SCHOOL_DISTRICT_VALUE,
                district_combo=row.DISTRICT_COMBO
            )

    def next_residence_address(self, df):
        """
        There are no exceptions. Take any address
        :param df:
        :return:
        """
        n = StreetNameNormalizer()
        for i in df.index:
            row = df.loc[i]
            voter_id = row.REGISTRATION_NUMBER
            voter = self.voters[voter_id]
            house_number = row.RESIDENCE_HOUSE_NUMBER
            street_name = n.normalize(row.RESIDENCE_STREET_NAME)
            apt_no = row.RESIDENCE_APT_UNIT_NBR
            city = row.RESIDENCE_CITY
            zipcode, plus4 = AddressSkills.zip_plus4(row.RESIDENCE_ZIPCODE)
            yield ResidenceAddress(voter=voter,
                                   house_number=house_number,
                                   street_name=street_name,
                                   apt_no=apt_no,
                                   city=city,
                                   state='GA',
                                   zipcode=zipcode,
                                   plus4=plus4)

    def next_mailing_address(self, df):
        for i in df.index:
            address = None
            row = df.loc[i]
            # If there is no city or street name
            # assume that residence address
            # and mailing address are the same -- return None
            # There are bad mailing address, but I don't
            # deal with that here.
            city = row.MAIL_CITY
            street_name = row.MAIL_STREET_NAME
            if city is not None and street_name is not None:
                voter_id = row.REGISTRATION_NUMBER
                voter = self.voters[voter_id]
                house_number = row.MAIL_HOUSE_NBR
                # May be foreign address so don't normalize
                street_name = row.MAIL_STREET_NAME
                apt_no = row.MAIL_APT_UNIT_NBR
                state = row.MAIL_STATE
                # I assume that zipcode is left off if not US address
                zipcode, plus4 = AddressSkills.zip_plus4(row.MAIL_ZIPCODE)
                address_line2 = row.MAIL_ADDRESS_2
                address_line3 = row.MAIL_ADDRESS_3
                country = row.MAIL_COUNTRY

                address = MailingAddress(voter=voter,
                                         house_number=house_number,
                                         street_name=street_name,
                                         apt_no=apt_no,
                                         city=city,
                                         state=state,
                                         zipcode=zipcode,
                                         plus4=plus4,
                                         address_line2=address_line2,
                                         address_line3=address_line3,
                                         country=country)
            yield address

    def next_voter(self, df):
        for i in df.index:
            row = df.loc[i]
            county_code = row.COUNTY_CODE
            precinct_id = row.COUNTY_PRECINCT_ID
            precinct_key = f'{county_code}_{precinct_id}'
            county = self.counties[county_code]
            precinct = self.precincts.get(precinct_key, None)
            yield Voter(
                edition=self.edition,
                voter_id=row.REGISTRATION_NUMBER,
                race_id=row.RACE,
                gender=row.GENDER,
                year_of_birth=self.to_int(row.BIRTHDATE),
                status=row.VOTER_STATUS,
                status_reason=row.STATUS_REASON,
                date_added=self.to_date(row.DATE_ADDED),
                date_changed=self.to_date(row.DATE_CHANGED),
                registration_date=self.to_date(row.REGISTRATION_DATE),
                last_contact_date=self.to_date(row.LAST_CONTACT_DATE),
                last_name=row.LAST_NAME,
                first_name=row.FIRST_NAME,
                middle_name=row.MIDDLE_MAIDEN_NAME,
                name_suffix=row.NAME_SUFFIX,
                precinct=precinct,
                county=county,
                cng=row.CONGRESSIONAL_DISTRICT,
                hse=row.HOUSE_DISTRICT,
                sen=row.SENATE_DISTRICT,

                edition_year=self.precinct_edition_year,
                edition_date=self.edition_date,
                date_loaded=date.today(),

                precinct_id_text=precinct_id,
            )

    def read_csv(self):
        return pd.read_csv(self.path, delimiter='|', dtype=str,
                           on_bad_lines=self.on_bad_line,
                           engine='python', keep_default_na=False)

    def repair_voter_precinct(self, county_code, comments):
        start = time.perf_counter()
        county = County.objects.filter(county_code=county_code).first()
        precincts = self.get_precinct_map(county)
        edition = self.update_edition(comments)
        count = 0
        repaired = 0
        for v in Voter.objects.filter(edition=edition, county=county):
            precinct_id_text = v.precinct_id_text
            key = f'{county_code}_{precinct_id_text}'
            v.precinct = precincts.get(key, None)
            if v.precinct is not None:
                repaired += 1
            v.save()
            count += 1
            if (count % 10000) == 0:
                print(f'{count} processed, {repaired} repaired ...')
        end = time.perf_counter()
        print(f'Total of {count} processed, {repaired} repaired ... done!')
        print(f'Repair time is {end-start:.1f}!')

    @classmethod
    def to_date(cls, date_str: str):
        if pd.isnull(date_str):
            return None
        date_str = date_str.strip()
        if len(date_str) == 0:
            return None
        try:
            return datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            print(date_str)
            return None

    @classmethod
    def to_int(cls, value):
        return int(value) if pd.notnull(value) else None

    def update_edition(self, comments, exists_ok=True):
        try:
            edition = ListEdition.objects.get(date=self.edition_date)
            edition.comments += f'; {comments}'
            edition.save()
        except ListEdition.DoesNotExist:
            if exists_ok:
                edition = ListEdition(date=self.edition_date,
                                      path=str(self.path),
                                      comments=comments)
                edition.save()
            else:
                raise KeyError(f'A ListEdition with a edition date of {self.edition_date.strftime("%Y-%m-%d")}')
        return edition
