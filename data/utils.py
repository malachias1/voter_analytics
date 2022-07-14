import re

ZIPCODE = re.compile(r'(\d\d\d\d\d)-?(\d\d\d\d)?')


def zip_plus4(zipcode):
    if zipcode == '':
        return '', ''
    m = ZIPCODE.match(zipcode)
    if m is None:
        return '', ''
    return m.group(1), m.group(2) if m.group(2) is not None else ''
