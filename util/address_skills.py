import re
import pandas as pd


class AddressSkills:
    ZIPCODE = re.compile(r'(\d\d\d\d\d)-?(\d\d\d\d)?')

    @classmethod
    def zip_plus4(cls, zipcode):
        if zipcode == '' or pd.isnull(zipcode):
            return '', ''
        m = cls.ZIPCODE.match(zipcode)
        if m is None:
            return '', ''
        return m.group(1), m.group(2) if m.group(2) is not None else ''
