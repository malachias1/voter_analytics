from selenium import webdriver
from time import sleep
import re
from pathlib import Path
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from urllib import request
from urllib.error import HTTPError


class CountyElectionResultDownloaderBase:
    def __init__(self, root_url):
        self.root_url = root_url
        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
        profile.set_preference('download.default_directory', str(Path('../election_results_download').absolute()))
        self.driver = webdriver.Firefox(firefox_profile=profile)
        self.driver.implicitly_wait(10)
        self.root_window = None

    @property
    def date_suffix(self):
        raise NotImplemented('date_suffix is not implemented!')

    def wait_for_complete(self):
        WebDriverWait(self.driver, 10).until(
            lambda driver: self.driver.execute_script('return document.readyState') == 'complete')

    def build(self):
        county_pattern = re.compile(r'(.+//GA/)(\w+)/(\d+)/[?]v=(\d+)')
        print(self.root_url)
        self.driver.get(self.root_url)
        self.wait_for_complete()
        county_results = []
        for sleep_time in range(5, 10, 20):
            try:
                county_results = list(self.driver.find_elements(by='xpath', value=' //i[@class="fa fa-chevron-right"]'))
                break
            except NoSuchElementException as _:
                sleep(sleep_time)
                continue
        if len(county_results) == 0:
            print('Cannot get results.')
            # print(self.driver.page_source)
            return
        for j in county_results:
            a = j.find_element(by='xpath', value='../..')
            href = a.get_attribute('href')
            m = county_pattern.search(href)
            base_url = m.group(1)
            county = m.group(2)
            n1 = m.group(3)
            n2 = m.group(4)
            download_url = f'{base_url}{county}/{n1}/{n2}/reports/detailxml.zip'
            local_file = Path('../election_results_download', f'{county}_{self.date_suffix}.zip')
            if local_file.exists():
                continue
            try:
                with request.urlopen(download_url) as fi:
                    data = fi.read()
                    print(local_file)
                    with open(local_file, 'wb') as fo:
                        fo.write(data)
            except HTTPError as e:
                print(f'{str(e)} -- {str(local_file)}, {download_url}')


class CountyElectionResultDownloader20220524(CountyElectionResultDownloaderBase):
    @property
    def date_suffix(self):
        return '2022_05_24'


class CountyElectionResultDownloader2021103(CountyElectionResultDownloaderBase):
    @property
    def date_suffix(self):
        return '2020_11_03'

