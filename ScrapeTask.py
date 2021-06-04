import json
import os
import shutil
import time

import pyrebase

from DeltaChecker import DeltaChecker
from EmailSender import EmailSender
from Scraper import Scraper
from Utils import supported_categories, log_info, get_current_pst_time, get_current_pst_format_date


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
        self.emailSender = EmailSender()

    def start(self):
        def cleanup():
            dirpath = 'temp'
            if os.path.exists(dirpath) and os.path.isdir(dirpath):
                shutil.rmtree(dirpath)

        scraper = Scraper(on_proxy=self.on_proxy, headless=not self.debug)
        index = 0
        scrape_flag = None
        while 1:
            index += 1
            cleanup()
            start_time = get_current_pst_time()
            scraper.get_product_info()
            last_scrape_flag = scrape_flag
            scrape_flag, scrape_results = scraper.scrape_result()
            scraper_timestamp = scraper.get_timestamp()
            database_log_prefix = 'logs/task/{}/{}'.format(scraper_timestamp[:8], scraper_timestamp[9:])
            self.database.child('{}/scrape'.format(database_log_prefix)).set(scrape_flag)
            if scrape_flag not in self.results_dict:
                self.results_dict[scrape_flag] = 0
            self.results_dict[scrape_flag] += 1
            products_upload_result = {}
            delta_realtime_update_result = {}
            delta_daily_update_result = {}
            if scrape_flag != "SUCCESS":
                if last_scrape_flag != "BLOCKED" and scrape_flag == "BLOCKED":
                    self.database.child(
                        'logs/key_timestamps/{}/{}'.format(scraper_timestamp[:8], scraper_timestamp[9:])).set(
                        scrape_flag)
                scraper.terminate()
                scraper = Scraper(on_proxy=self.on_proxy, headless=not self.debug)
            else:
                if not last_scrape_flag:
                    self.database.child(
                        'logs/key_timestamps/{}/{}'.format(scraper_timestamp[:8], scraper_timestamp[9:])).set(
                        scrape_flag)
                log_info("update products info attempt started")
                products_upload_result = self.deltaChecker.upload_products_if_necessary(timestamp=scraper_timestamp)
                log_info("updated product? : {}".format(products_upload_result))
                self.database.child('{}/upload'.format(database_log_prefix)).set(products_upload_result)
                log_info("delta update attempt started")
                delta_realtime_update_result = self.deltaChecker.update_realtime_delta(scraper_timestamp)
                log_info("delta updated? : {}".format(delta_realtime_update_result))
                self.database.child('{}/delta_realtime'.format(database_log_prefix)).set(delta_realtime_update_result)
                delta_daily_update_result = self.deltaChecker.update_daily_delta_if_necessary()
                log_info("delta daily updated? : {}".format(delta_daily_update_result))
                self.database.child('{}/delta_daily'.format(database_log_prefix)).set(delta_daily_update_result)
                if delta_daily_update_result == "SUCCESS":
                    try:
                        self.emailSender.send_daily_update(get_current_pst_format_date())
                    except:
                        log_info("Failed to send daily email!")
            time_used_in_seconds = (get_current_pst_time() - start_time).total_seconds()
            self.database.child('{}/time_used'.format(database_log_prefix)).set(time_used_in_seconds)
            time_until_next_scrape = self.interval_seconds - time_used_in_seconds
            log_info("==========\n"
                     "  index: {}\n"
                     "  timestamp: {}\n"
                     "  time_used: {}\n"
                     "  scrape_results: {}\n"
                     "  products_upload: {}\n"
                     "  delta_realtime: {}\n"
                     "  delta_daily: {}".format(index,
                                                scraper_timestamp,
                                                time_used_in_seconds,
                                                scrape_results,
                                                products_upload_result,
                                                delta_realtime_update_result,
                                                delta_daily_update_result))
            log_info("========== time_until_next_scrape:{}".format(time_until_next_scrape))
            if index == self.iterations:
                scraper.terminate()
                break
            if last_scrape_flag == "BLOCKED" and scrape_flag == "BLOCKED":
                scraper.terminate()
                raise Exception("Scraper is Blocked")
                break
            if time_until_next_scrape > 0:
                time.sleep(time_until_next_scrape)
