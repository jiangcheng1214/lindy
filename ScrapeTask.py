import json
import os
import shutil
import time

import pyrebase

from DeltaChecker import DeltaChecker
from EmailSender import EmailSender
from Scraper import Scraper
from Utils import log_info, get_current_pst_time, log_warning, ConsecutiveFailureException


def remove_temp_dir(locale_code):
    temp_dir_path = 'temp/{}'.format(locale_code)
    if os.path.exists(temp_dir_path) and os.path.isdir(temp_dir_path):
        shutil.rmtree(temp_dir_path)


class ScrapeTask:
    def __init__(self, local_code, interval_seconds=60 * 2, debug=False, proxy_list=None):
        self.interval_seconds = interval_seconds
        self.debug = debug
        self.local_code = local_code
        self.deltaChecker = DeltaChecker()
        self.proxy_id = 0
        self.proxy_list = proxy_list
        with open('credentials/firebase_credentials.json', 'r') as f:
            credentials = json.load(f)
        self.database = pyrebase.initialize_app(credentials).database()
        self.emailSender = EmailSender()
        self.scraper = Scraper(proxy=self.get_proxy(), headless=not debug)

    def get_proxy(self, get_next=False):
        if not self.proxy_list:
            return None
        if get_next:
            self.proxy_id += 1
        self.proxy_id %= len(self.proxy_list)
        p = self.proxy_list[self.proxy_id]
        log_info("proxy: {}".format(p))
        return p

    def start(self):
        log_info("Start scraping..")
        index = 0
        sequential_failures = 0
        while 1:
            remove_temp_dir(self.local_code)
            start_time = get_current_pst_time()
            self.scraper.get_product_info(self.local_code)
            scrape_flag, scrape_results = self.scraper.scrape_result()
            scraper_timestamp = self.scraper.get_timestamp()
            database_log_prefix = '{}/logs/task/{}/{}'.format(self.local_code, scraper_timestamp[:8],
                                                              scraper_timestamp[9:])
            self.database.child('{}/scrape'.format(database_log_prefix)).set(scrape_flag)
            products_upload_result = {}
            if scrape_flag == "SUCCESS":
                log_info("update products info attempt started")
                products_upload_result = self.deltaChecker.upload_products_if_necessary(scraper_timestamp,
                                                                                        self.local_code)
                log_info("updated product? : {}".format(products_upload_result))
                self.database.child('{}/upload'.format(database_log_prefix)).set(products_upload_result)
            time_used_in_seconds = (get_current_pst_time() - start_time).total_seconds()
            self.database.child('{}/time_used'.format(database_log_prefix)).set(time_used_in_seconds)
            log_info("==========\n"
                     "  country: {}\n"
                     "  index: {}\n"
                     "  timestamp: {}\n"
                     "  time_used: {}\n"
                     "  scrape_flag: {}\n"
                     "  scrape_results: {}\n"
                     "  products_upload: {}".format(self.local_code, index,
                                                    scraper_timestamp,
                                                    time_used_in_seconds,
                                                    scrape_flag,
                                                    scrape_results,
                                                    products_upload_result))
            sequential_failures += 1
            if scrape_flag == "SUCCESS":
                sequential_failures = 0
                time_until_next_scrape = self.interval_seconds - time_used_in_seconds
                log_info("========== time_until_next_scrape:{}".format(time_until_next_scrape))
                if time_until_next_scrape > 0:
                    time.sleep(time_until_next_scrape)
            elif scrape_flag == "BLOCKED":
                self.scraper.terminate()
                log_warning("Starting new scraper instance after being blocked..")
                self.scraper = Scraper(proxy=self.get_proxy(get_next=True), headless=not self.debug)
            if sequential_failures == 10:
                raise ConsecutiveFailureException("Scraper failed for 10 times in a row")
            index += 1

    def terminate_scraper(self):
        self.scraper.terminate()
