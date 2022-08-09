import json

from django.db import models
from county.models import County
from precinct.models import Precinct, PrecinctEdition
from datetime import date, datetime
from pathlib import Path
import pandas as pd
from util.addresses import StreetNameNormalizer
import re
import time


class ListEditionManager(models.Manager):
    COUNTIES = None
    PRECINCTS = None
    EDITION = None
    EDITION_DATE = None
    EDITION_YEAR = None
    BATCH_SIZE = None
    VOTERS = None
    ZIPCODE = re.compile(r'(\d\d\d\d\d)-?(\d\d\d\d)?')

    @classmethod
    def get_precinct_edition(cls, year):
        if not PrecinctEdition.objects.filter(year=year).exists():
            raise KeyError(f'No precinct edition exists for {year}!')
        return PrecinctEdition.objects.get(year=year)

    @classmethod
    def get_precinct_map(cls, year, county=None):
        edition = cls.get_precinct_edition(year)
        precincts = Precinct.objects.filter(edition=edition)
        if county is not None:
            precincts = precincts.filter(county=county)
        return {f'{x.county.county_code}_{x.precinct_id}': x for x in precincts}

    @classmethod
    def on_bad_line(cls, badline):
        print(badline)
        return None

    @classmethod
    def check_voter_list(cls, path, precinct_edition_year):
        report = {}
        p = Path(path)
        precincts = cls.get_precinct_map(precinct_edition_year)
        df_orig = pd.read_csv(p, delimiter='|', dtype=str, on_bad_lines=cls.on_bad_line, engine='python')

        # Filter out voters with either missing or invalid addresses.
        df = df_orig[df_orig.CONGRESSIONAL_DISTRICT != '99999']

        report['bad_address_count'] = len(df_orig.index) - len(df.index)
        report['max_field_widths'] = {
            'LAST_NAME'.lower(): cls.to_int(df.LAST_NAME.str.len().dropna().max()),
            'FIRST_NAME'.lower(): cls.to_int(df.FIRST_NAME.str.len().dropna().max()),
            'MIDDLE_MAIDEN_NAME'.lower(): cls.to_int(df.MIDDLE_MAIDEN_NAME.str.len().dropna().max()),
            'NAME_SUFFIX'.lower(): cls.to_int(df.NAME_SUFFIX.str.len().dropna().max()),
            'NAME_TITLE'.lower(): cls.to_int(df.NAME_TITLE.str.len().dropna().max()),
            'RESIDENCE_HOUSE_NUMBER'.lower(): cls.to_int(df.RESIDENCE_HOUSE_NUMBER.str.len().dropna().max()),
            'RESIDENCE_STREET_NAME'.lower(): cls.to_int(df.RESIDENCE_STREET_NAME.str.len().dropna().max()),
            'RESIDENCE_STREET_SUFFIX'.lower(): cls.to_int(df.RESIDENCE_STREET_SUFFIX.str.len().dropna().max()),
            'RESIDENCE_APT_UNIT_NBR'.lower(): cls.to_int(df.RESIDENCE_APT_UNIT_NBR.str.len().dropna().max()),
            'RESIDENCE_CITY'.lower(): cls.to_int(df.RESIDENCE_CITY.str.len().dropna().max()),
            'RACE'.lower(): cls.to_int(df.RACE.str.len().dropna().max()),
            'GENDER'.lower(): cls.to_int(df.GENDER.str.len().dropna().max()),
            'LAND_DISTRICT'.lower(): cls.to_int(df.LAND_DISTRICT.str.len().dropna().max()),
            'LAND_LOT'.lower(): cls.to_int(df.LAND_LOT.str.len().dropna().max()),
            'STATUS_REASON'.lower(): cls.to_int(df.STATUS_REASON.str.len().dropna().max()),
            'COUNTY_PRECINCT_ID'.lower(): cls.to_int(df.COUNTY_PRECINCT_ID.str.len().dropna().max()),
            'CITY_PRECINCT_ID'.lower(): cls.to_int(df.CITY_PRECINCT_ID.str.len().dropna().max()),
            'JUDICIAL_DISTRICT'.lower(): cls.to_int(df.JUDICIAL_DISTRICT.str.len().dropna().max()),
            'COMMISSION_DISTRICT'.lower(): cls.to_int(df.COMMISSION_DISTRICT.str.len().dropna().max()),
            'SCHOOL_DISTRICT'.lower(): cls.to_int(df.SCHOOL_DISTRICT.str.len().dropna().max()),
            'COUNTY_DISTRICTA_NAME'.lower(): cls.to_int(df.COUNTY_DISTRICTA_NAME.str.len().dropna().max()),
            'COUNTY_DISTRICTA_VALUE'.lower(): cls.to_int(df.COUNTY_DISTRICTA_VALUE.str.len().dropna().max()),
            'COUNTY_DISTRICTB_NAME'.lower(): cls.to_int(df.COUNTY_DISTRICTB_NAME.str.len().dropna().max()),
            'COUNTY_DISTRICTB_VALUE'.lower(): cls.to_int(df.COUNTY_DISTRICTB_VALUE.str.len().dropna().max()),
            'MUNICIPAL_NAME'.lower(): cls.to_int(df.MUNICIPAL_NAME.str.len().dropna().max()),
            'MUNICIPAL_CODE'.lower(): cls.to_int(df.MUNICIPAL_CODE.str.len().dropna().max()),
            'WARD_CITY_COUNCIL_NAME'.lower(): cls.to_int(df.WARD_CITY_COUNCIL_NAME.str.len().dropna().max()),
            'WARD_CITY_COUNCIL_CODE'.lower(): cls.to_int(df.WARD_CITY_COUNCIL_CODE.str.len().dropna().max()),
            'CITY_SCHOOL_DISTRICT_NAME'.lower(): cls.to_int(df.CITY_SCHOOL_DISTRICT_NAME.str.len().dropna().max()),
            'CITY_SCHOOL_DISTRICT_VALUE'.lower(): cls.to_int(df.CITY_SCHOOL_DISTRICT_VALUE.str.len().dropna().max()),
            'CITY_DISTA_NAME'.lower(): cls.to_int(df.CITY_DISTA_NAME.str.len().dropna().max()),
            'CITY_DISTA_VALUE'.lower(): cls.to_int(df.CITY_DISTA_VALUE.str.len().dropna().max()),
            'CITY_DISTB_NAME'.lower(): cls.to_int(df.CITY_DISTB_NAME.str.len().dropna().max()),
            'CITY_DISTB_VALUE'.lower(): cls.to_int(df.CITY_DISTB_VALUE.str.len().dropna().max()),
            'CITY_DISTC_NAME'.lower(): cls.to_int(df.CITY_DISTC_NAME.str.len().dropna().max()),
            'CITY_DISTC_VALUE'.lower(): cls.to_int(df.CITY_DISTC_VALUE.str.len().dropna().max()),
            'CITY_DISTD_NAME'.lower(): cls.to_int(df.CITY_DISTD_NAME.str.len().dropna().max()),
            'CITY_DISTD_VALUE'.lower(): cls.to_int(df.CITY_DISTD_VALUE.str.len().dropna().max()),
            'DISTRICT_COMBO'.lower(): cls.to_int(df.DISTRICT_COMBO.str.len().dropna().max()),
            'MAIL_HOUSE_NBR'.lower(): cls.to_int(df.MAIL_HOUSE_NBR.str.len().dropna().max()),
            'MAIL_STREET_NAME'.lower(): cls.to_int(df.MAIL_STREET_NAME.str.len().dropna().max()),
            'MAIL_APT_UNIT_NBR'.lower(): cls.to_int(df.MAIL_APT_UNIT_NBR.str.len().dropna().max()),
            'MAIL_CITY'.lower(): cls.to_int(df.MAIL_CITY.str.len().dropna().max()),
            'MAIL_STATE'.lower(): cls.to_int(df.MAIL_STATE.str.len().dropna().max()),
            'MAIL_ZIPCODE'.lower(): cls.to_int(df.MAIL_ZIPCODE.str.len().dropna().max()),
            'MAIL_ADDRESS_2'.lower(): df.MAIL_ADDRESS_2.str.len().dropna().max(),
            'MAIL_ADDRESS_3'.lower(): df.MAIL_ADDRESS_3.str.len().dropna().max(),
            'MAIL_COUNTRY'.lower(): cls.to_int(df.MAIL_COUNTRY.str.len().dropna().max())
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
            if key in precincts:
                continue
            count += 1
            missing.add(key)
            missing_precincts[county_code].add(precinct_id)
        report['missing_precincts'] = {k: list(v) for k, v in missing_precincts.items()}
        report['total_missing_precincts'] = len(missing)
        report['voters_with_missing_precincts'] = count

        with p.with_suffix('.log').open('w') as f:
            json.dump(report, f, indent=2)

    @classmethod
    def load_mailing_addresses(cls, df):
        count = 0
        addresses = []
        for a in cls.next_mailing_address(df):
            # If next address is None, there is no
            # separate mailing address
            if a is None:
                continue
            addresses.append(a)
            if len(addresses) >= cls.BATCH_SIZE:
                MailingAddress.objects.bulk_create(addresses)
                count += len(addresses)
                print(f'{count} MailingAddress loaded ...')
                addresses = []
        MailingAddress.objects.bulk_create(addresses)
        count += len(addresses)
        print(f'{count} MailingAddress loaded ... done!')

    @classmethod
    def load_other_jurisdictions(cls, df):
        count = 0
        others = []
        for o in cls.next_other_jurisdiction(df):
            others.append(o)
            if len(others) >= cls.BATCH_SIZE:
                OtherJurisdictions.objects.bulk_create(others)
                count += len(others)
                print(f'{count} OtherJurisdictions loaded ...')
                others = []
        OtherJurisdictions.objects.bulk_create(others)
        count += len(others)
        print(f'{count} OtherJurisdictions loaded ... done!')

    @classmethod
    def load_residence_addresses(cls, df):
        count = 0
        addresses = []
        for a in cls.next_residence_address(df):
            addresses.append(a)
            if len(addresses) >= cls.BATCH_SIZE:
                ResidenceAddress.objects.bulk_create(addresses)
                count += len(addresses)
                print(f'{count} ResidenceAddress loaded ...')
                addresses = []
        ResidenceAddress.objects.bulk_create(addresses)
        count += len(addresses)
        print(f'{count} ResidenceAddress loaded ... done!')

    @classmethod
    def load_state(cls, path, edition_date: date, edition_year, comments,
                   voters=True,
                   jurisdictions=True,
                   mailing_addresses=True,
                   residence_addresses=True):
        start = time.perf_counter()
        cls.EDITION_DATE = edition_date
        cls.EDITION_YEAR = edition_year
        cls.COUNTIES = {x.county_code: x for x in County.objects.all()}
        cls.PRECINCTS = cls.get_precinct_map(edition_year)
        cls.EDITION = cls.update_edition(path, edition_date, comments, exists_ok=True)
        cls.BATCH_SIZE = 10000
        end = time.perf_counter()
        print(f'load_state setup time: {end - start:.1f}.')

        start = end
        p = Path(path)
        # Do not interpret NA values. One person's name is Null!
        # After reading, convert all zero-length strings to None.
        df_orig = pd.read_csv(p, delimiter='|', dtype=str, on_bad_lines=cls.on_bad_line,
                              engine='python', keep_default_na=False)
        end = time.perf_counter()
        print(f'load_state read_csv time: {end - start:.1f}.')

        start = end
        df_orig = df_orig.where(df_orig != '', None)
        # Filter out voters with either missing or invalid addresses.
        df = df_orig[df_orig.CONGRESSIONAL_DISTRICT != '99999']
        print(f'load_state cleanup time: {end - start:.1f}.')

        if voters:
            start = end
            Voter.objects.filter(edition=cls.EDITION).delete()
            end = time.perf_counter()
            print(f'load_state purge time: {end-start:.1f}.')

            start = end
            cls.load_voters(df)
            end = time.perf_counter()
            print(f'load_state load_voters time: {end-start:.1f}.')

        start = end
        cls.VOTERS = {x.voter_id: x for x in Voter.objects.filter(edition=cls.EDITION)}
        end = time.perf_counter()
        print(f'load_state create voter id map time: {end - start:.1f}.')

        if jurisdictions:
            start = end
            cls.load_other_jurisdictions(df)
            end = time.perf_counter()
            print(f'load_state load_other_jurisdictions time: {end-start:.1f}.')

        if mailing_addresses:
            start = end
            cls.load_mailing_addresses(df)
            end = time.perf_counter()
            print(f'load_state load_mailing_addresses time: {end - start:.1f}.')

        if residence_addresses:
            start = end
            cls.load_residence_addresses(df)
            end = time.perf_counter()
            print(f'load_state load_residence_addresses time: {end - start:.1f}.')

    @classmethod
    def load_voters(cls, df):
        count = 0
        voters = []
        for v in cls.next_voter(df):
            voters.append(v)
            if len(voters) >= cls.BATCH_SIZE:
                Voter.objects.bulk_create(voters)
                count += len(voters)
                print(f'{count} Voter loaded ...')
                voters = []
        Voter.objects.bulk_create(voters)
        count += len(voters)
        print(f'{count} Voter loaded ... done!')

    @classmethod
    def next_other_jurisdiction(cls, df):
        for i in df.index:
            row = df.loc[i]
            county_code = row.COUNTY_CODE
            county = cls.COUNTIES[county_code]
            voter_id = row.REGISTRATION_NUMBER
            voter = cls.VOTERS[voter_id]
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

    @classmethod
    def next_residence_address(cls, df):
        """
        There are no exceptions. Take any address
        :param df:
        :return:
        """
        n = StreetNameNormalizer()
        for i in df.index:
            row = df.loc[i]
            voter_id = row.REGISTRATION_NUMBER
            voter = cls.VOTERS[voter_id]
            house_number = row.RESIDENCE_HOUSE_NUMBER
            street_name = n.normalize(row.RESIDENCE_STREET_NAME)
            apt_no = row.RESIDENCE_APT_UNIT_NBR
            city = row.RESIDENCE_CITY
            zipcode, plus4 = cls.zip_plus4(row.RESIDENCE_ZIPCODE)
            yield ResidenceAddress(voter=voter,
                                   house_number=house_number,
                                   street_name=street_name,
                                   apt_no=apt_no,
                                   city=city,
                                   state='GA',
                                   zipcode=zipcode,
                                   plus4=plus4)

    @classmethod
    def next_mailing_address(cls, df):
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
                voter = cls.VOTERS[voter_id]
                house_number = row.MAIL_HOUSE_NBR
                # May be foreign address so don't normalize
                street_name = row.MAIL_STREET_NAME
                apt_no = row.MAIL_APT_UNIT_NBR
                state = row.MAIL_STATE
                # I assume that zipcode is left off if not US address
                zipcode, plus4 = cls.zip_plus4(row.MAIL_ZIPCODE)
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

    @classmethod
    def next_voter(cls, df):
        for i in df.index:
            row = df.loc[i]
            county_code = row.COUNTY_CODE
            precinct_id = row.COUNTY_PRECINCT_ID
            precinct_key = f'{county_code}_{precinct_id}'
            county = cls.COUNTIES[county_code]
            precinct = cls.PRECINCTS.get(precinct_key, None)
            yield Voter(
                edition=cls.EDITION,
                voter_id=row.REGISTRATION_NUMBER,
                race_id=row.RACE,
                gender=row.GENDER,
                year_of_birth=cls.to_int(row.BIRTHDATE),
                status=row.VOTER_STATUS,
                status_reason=row.STATUS_REASON,
                date_added=cls.to_date(row.DATE_ADDED),
                date_changed=cls.to_date(row.DATE_CHANGED),
                registration_date=cls.to_date(row.REGISTRATION_DATE),
                last_contact_date=cls.to_date(row.LAST_CONTACT_DATE),
                last_name=row.LAST_NAME,
                first_name=row.FIRST_NAME,
                middle_name=row.MIDDLE_MAIDEN_NAME,
                name_suffix=row.NAME_SUFFIX,
                precinct=precinct,
                county=county,
                cng=row.CONGRESSIONAL_DISTRICT,
                hse=row.HOUSE_DISTRICT,
                sen=row.SENATE_DISTRICT,

                edition_year=cls.EDITION_YEAR,
                edition_date=cls.EDITION_DATE,
                date_loaded=date.today(),

                precinct_id_text=precinct_id,
            )

    @classmethod
    def repair_voter_precinct(cls, path, county_code, edition_date: date, edition_year, comments):
        start = time.perf_counter()
        county = County.objects.filter(county_code=county_code).first()
        precincts = cls.get_precinct_map(edition_year, county)
        edition = cls.update_edition(path, edition_date, comments)
        end = time.perf_counter()
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
        print(f'Total of {count} processed, {repaired} repaired ... done!')

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

    @classmethod
    def update_edition(cls, path, edition_date: date, comments, exists_ok=True):
        try:
            edition = ListEdition.objects.get(date=edition_date)
            edition.comments += f'; {comments}'
            edition.save()
        except ListEdition.DoesNotExist:
            if exists_ok:
                edition = ListEdition(date=edition_date,
                                      path=str(path),
                                      comments=comments)
                edition.save()
            else:
                raise KeyError(f'A ListEdition with a edition date of {edition_date.strftime("%Y-%m-%d")}')
        return edition

    @classmethod
    def zip_plus4(cls, zipcode):
        if zipcode == '' or pd.isnull(zipcode):
            return '', ''
        m = cls.ZIPCODE.match(zipcode)
        if m is None:
            return '', ''
        return m.group(1), m.group(2) if m.group(2) is not None else ''


class ListEdition(models.Model):
    date = models.DateField()
    path = models.TextField()
    comments = models.TextField()

    objects = ListEditionManager()


class MailingAddress(models.Model):
    voter = models.ForeignKey('Voter', on_delete=models.CASCADE)
    # PO Boxes don't have house numbers
    house_number = models.CharField(max_length=32, blank=True, null=True)
    street_name = models.CharField(max_length=128)
    apt_no = models.CharField(max_length=32, blank=True, null=True)
    city = models.CharField(max_length=64)
    state = models.CharField(max_length=2, blank=True, null=True)
    zipcode = models.CharField(max_length=5, blank=True, null=True)
    plus4 = models.CharField(max_length=4, blank=True, null=True)
    address_line2 = models.CharField(max_length=64, blank=True, null=True)
    address_line3 = models.CharField(max_length=64, blank=True, null=True)
    country = models.CharField(max_length=32, blank=True, null=True)


class OtherJurisdictions(models.Model):
    voter = models.ForeignKey('Voter', on_delete=models.CASCADE)
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    land_district = models.CharField(max_length=5, blank=True, null=True)
    land_lot = models.CharField(max_length=5, blank=True, null=True)
    city_precinct_id = models.CharField(max_length=5, blank=True, null=True)
    judicial_district = models.CharField(max_length=4, blank=True, null=True)
    commission_district = models.CharField(max_length=3, blank=True, null=True)
    school_district = models.CharField(max_length=3, blank=True, null=True)
    county_districta_name = models.CharField(max_length=5, blank=True, null=True)
    county_districta_value = models.CharField(max_length=3, blank=True, null=True)
    county_districtb_name = models.CharField(max_length=5, blank=True, null=True)
    county_districtb_value = models.CharField(max_length=2, blank=True, null=True)
    municipal_name = models.CharField(max_length=32, blank=True, null=True)
    municipal_code = models.CharField(max_length=3, blank=True, null=True)
    ward_city_council_name = models.CharField(max_length=5, blank=True, null=True)
    ward_city_council_code = models.CharField(max_length=3, blank=True, null=True)
    city_school_district_name = models.CharField(max_length=5, blank=True, null=True)
    city_school_district_value = models.CharField(max_length=3, blank=True, null=True)
    district_combo = models.CharField(max_length=3, blank=True, null=True)


class ResidenceAddress(models.Model):
    # I need an address for every voter.
    # I need to also be able to capture
    # the fact that a voter has a bad address.
    voter = models.ForeignKey('Voter', on_delete=models.CASCADE)
    house_number = models.CharField(max_length=32, blank=True, null=True)
    street_name = models.CharField(max_length=64, blank=True, null=True)
    apt_no = models.CharField(max_length=32, blank=True, null=True)
    city = models.CharField(max_length=32, blank=True, null=True)
    state = models.CharField(max_length=2)
    zipcode = models.CharField(max_length=5, blank=True, null=True)
    plus4 = models.CharField(max_length=4, blank=True, null=True)


class VoterManager(models.Manager):
    pass


class Voter(models.Model):
    edition = models.ForeignKey('ListEdition', on_delete=models.SET_NULL, blank=True, null=True)
    voter_id = models.CharField(max_length=8)
    race_id = models.CharField(max_length=2)
    gender = models.CharField(max_length=1)
    year_of_birth = models.IntegerField()
    status = models.CharField(max_length=1)
    status_reason = models.CharField(max_length=16, blank=True, null=True)
    date_added = models.DateField(blank=True, null=True)
    date_changed = models.DateField(blank=True, null=True)
    registration_date = models.DateField(blank=True, null=True)
    last_contact_date = models.DateField(blank=True, null=True)
    last_name = models.CharField(max_length=64)
    first_name = models.CharField(max_length=32)
    middle_name = models.CharField(max_length=32, blank=True, null=True)
    name_suffix = models.CharField(max_length=3, blank=True, null=True)
    precinct = models.ForeignKey(Precinct, on_delete=models.SET_NULL, blank=True, null=True)
    county = models.ForeignKey(County, on_delete=models.SET_NULL, blank=True, null=True)
    cng = models.CharField(max_length=3)
    hse = models.CharField(max_length=3)
    sen = models.CharField(max_length=3)
    # For recovery of edition
    edition_year = models.IntegerField()
    edition_date = models.DateField()
    date_loaded = models.DateField()
    # in case precinct_id is not found.
    precinct_id_text = models.CharField(max_length=16)

    objects = VoterManager()

    class Meta:
        indexes = [
            models.Index(fields=['voter_id']),
            models.Index(fields=['status']),
            models.Index(fields=['cng']),
            models.Index(fields=['hse']),
            models.Index(fields=['sen']),
            models.Index(fields=['county']),
            models.Index(fields=['edition'])
        ]
