import json
import time

import pyrebase

from DeltaChecker import DeltaChecker
from Scraper import Scraper
from Utils import supported_categories, log_info, get_current_pst_time


class ScrapeTask:
    def __init__(self, iterations, interval_seconds=60, debug=False, on_proxy=False):
        self.category_codes = supported_categories()
        self.results_dict = {}
        self.iterations = iterations
        self.interval_seconds = interval_seconds
        self.debug = debug
        self.deltaChecker = DeltaChecker()
        self.on_proxy = on_proxy
        with open('credentials/firebase_credentials.json', 'r') as f:
            credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(credentials)
        self.database = self.firebase.database()

    def start(self):
        scraper = Scraper(on_proxy=self.on_proxy, headless=not self.debug)
        i = 0
        flag = None
        while 1:
            i += 1
            start_time = get_current_pst_time()
            scraper.get_product_info()
            last_flag = flag
            flag, results = scraper.scrape_result()
            database_path_prefix = 'logs/task/{}/{}'.format(scraper.timestamp[:8], scraper.timestamp[9:])
            self.database.child('{}/scrape'.format(database_path_prefix)).set(flag)
            if flag not in self.results_dict:
                self.results_dict[flag] = 0
            self.results_dict[flag] += 1
            products_upload_result = False
            delta_update_result = False
            if flag != "SUCCESS":
                if last_flag != "BLOCKED" and flag == "BLOCKED":
                    self.database.child('logs/key_timestamps/{}/{}'.format(scraper.timestamp[:8], scraper.timestamp[9:])).set(flag)
                scraper.terminate()
                scraper = Scraper(on_proxy=self.on_proxy, headless=not self.debug)
            else:
                if not last_flag:
                    self.database.child('logs/key_timestamps/{}/{}'.format(scraper.timestamp[:8], scraper.timestamp[9:])).set(flag)
                log_info("update products info attempt started")
                products_upload_result = self.deltaChecker.upload_products_if_necessary(timestamp=scraper.timestamp)
                log_info("updated product? : {}".format(products_upload_result))
                self.database.child('{}/upload'.format(database_path_prefix)).set(products_upload_result)
                log_info("delta update attempt started")
                delta_update_result = self.deltaChecker.check_delta_and_update_cloud()
                log_info("delta updated? : {}".format(delta_update_result))
                self.database.child('{}/delta'.format(database_path_prefix)).set(delta_update_result)
            if i == self.iterations:
                scraper.terminate()
                break
            time_used_in_seconds = (get_current_pst_time() - start_time).total_seconds()
            self.database.child('{}/time_used'.format(database_path_prefix)).set(time_used_in_seconds)
            time_until_next_scrape = self.interval_seconds - time_used_in_seconds
            log_info("===== {} - result:{}".format(i, [start_time, time_used_in_seconds, flag, products_upload_result, delta_update_result, results]))
            log_info("===== time_until_next_scrape:{}".format(time_until_next_scrape))
            if time_until_next_scrape > 0:
                time.sleep(time_until_next_scrape)
