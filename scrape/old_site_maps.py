from selenium import webdriver
from time import sleep
import re
from datetime import datetime
from pathlib import Path
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import json
import os
from urllib import request


class ElectionResultCrawler:
    def __init__(self, lz_dir):
        self.root_url = 'https://results.enr.clarityelections.com/GA/'
        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
        self.driver = webdriver.Firefox(firefox_profile=profile)
        self.driver.implicitly_wait(10)
        self.root_window = None

    @classmethod
    def election_site_map_path(cls, version):
        return Path('..', 'resources', f'election-site-map-v{version}.json')

    def wait_for_complete(self):
        WebDriverWait(self.driver, 10).until(
            lambda driver: self.driver.execute_script('return document.readyState') == 'complete')

    def get_election_elements(self):
        self.driver.get(self.root_url)
        self.wait_for_complete()
        self.root_window = self.driver.window_handles[0]
        return self.driver.find_elements_by_class_name('list-group-item')

    def get_election_details(self, election_site_map):
        elections = list(self.get_election_elements())
        top_window = self.driver.window_handles[0]
        p = re.compile(r'(.*)\s+(\d+/\d+/\d+)')
        for i in elections:
            text = i.text
            m = p.match(text)
            name = m.group(1)
            date = m.group(2)
            if date not in election_site_map or election_site_map[date]['url'] == "about:blank":
                self.driver.execute_script("arguments[0].scrollIntoView();", i)
                i.click()
                self.wait_for_complete()
                election_results_window = self.driver.window_handles[1]
                self.driver.switch_to.window(election_results_window)
                url = self.driver.current_url
                election_site_map[date] = {
                    'name': name,
                    'date': date,
                    'url': url
                }
                self.driver.close()
                self.driver.switch_to.window(top_window)
        return election_site_map

    def execute_phase1_scrape(self):
        if Path.exists(self.election_site_map_path(2)):
            with self.election_site_map_path(2).open('r') as f:
                election_site_map = self.get_election_details(json.load(f))
        else:
            election_site_map = self.get_election_details({})

        with self.election_site_map_path(3).open('w') as f:
            json.dump(election_site_map, f, indent=2)

    def execute_phase2_scrape(self, version=3):
        with self.election_site_map_path(version).open('r') as f:
            election_site_map = json.load(f)
        for _, v in election_site_map.items():
            if 'county_results_url' in v:
                continue
            url = v['url']
            self.driver.get(url)
            self.wait_for_complete()
            print(f're-trying getting RESULTS BY COUNTY link for {url}')
            county_results_link = None
            for sleep_time in range(5, 10, 20):
                try:
                    county_results_link = self.driver.find_element_by_xpath(
                        " // *[contains(translate(text(), 'RESULTS BY COUNTY', 'results by county'), 'results by county')]")
                    break
                except NoSuchElementException as _:
                    sleep(sleep_time)
                    continue
            if county_results_link is not None:
                success = False
                for sleep_time in range(5, 10, 20):
                    try:
                        self.driver.execute_script("arguments[0].click();", county_results_link)
                        success = True
                        break
                    except ElementClickInterceptedException as _:
                        sleep(sleep_time)
                        continue
                if success:
                    self.wait_for_complete()
                    v['county_results_url'] = self.driver.current_url
                else:
                    print(f'ElementClickInterceptedException for {url}')
            else:
                print(f'NoSuchElementException for {url}')
        with self.election_site_map_path(4).open('w') as f:
            json.dump(election_site_map, f, indent=2)


if __name__ == '__main__':
    crawler = ElectionResultCrawler('.')
    crawler.execute_phase2_scrape(version=4)


def old():
    crawler = ElectionResultCrawler('.')
    elections = list(crawler.get_election_elements())
    top_window = crawler.driver.window_handles[0]
    for i in elections:
        i.click()
        crawler.wait_for_complete()
        election_results_window = crawler.driver.window_handles[1]
        crawler.driver.switch_to.window(election_results_window)
        county_results_link = crawler.driver.find_element_by_xpath(" // span[contains(text(), 'Results By County')]")
        sleep(5)
        county_results_link.click()
        crawler.wait_for_complete()
        county_results_window = crawler.driver.window_handles[1]
        crawler.driver.switch_to.window(county_results_window)
        county_results = list(crawler.driver.find_elements_by_xpath(" // fa[@name='chevron-right']"))
        for j in county_results:
            j.click()
            crawler.wait_for_complete()
            county_result_window = crawler.driver.window_handles[2]
            crawler.driver.switch_to.window(county_result_window)
            sleep(5)
            download = None
            for sleep_time in range(10, 30, 60):
                try:
                    download = crawler.driver.find_element_by_xpath(" // a[@download='detailxml.zip']")
                except NoSuchElementException as _:
                    sleep(sleep_time)
                    continue
            if download is not None:
                href = download.get_attribute('href')
                print(href)
                parts = Path(href).parts
                local_file = Path('..', 'resources', f'{parts[3]}-{parts[4]}-{parts[5]}.zip'.lower())
                # Download remote and save locally
                with request.urlopen(href) as fi:
                    data = fi.read()
                    with open(local_file, 'wb') as fo:
                        fo.write(data)
            crawler.driver.close()
            crawler.driver.switch_to.window(county_results_window)
        crawler.driver.close()
        crawler.driver.switch_to.window(top_window)
