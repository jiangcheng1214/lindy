import json
import os
import shutil
import time

import pyrebase

from DeltaChecker import DeltaChecker
from EmailSender import EmailSender
from Scraper import Scraper
from Utils import supported_categories, log_info, get_current_pst_time, get_current_pst_format_date, supported_locales, \
    log_warning


def remove_temp_dir(locale_code):
    temp_dir_path = 'temp/{}'.format(locale_code)
    if os.path.exists(temp_dir_path) and os.path.isdir(temp_dir_path):
        shutil.rmtree(temp_dir_path)


class ScrapeTask:
    def __init__(self, interval_seconds=60, debug=False, on_proxy=False):
        self.category_codes = supported_categories()
        self.results_dict = {}
        self.interval_seconds = interval_seconds
        self.debug = debug
        self.deltaChecker = DeltaChecker()
        self.on_proxy = on_proxy
        with open('credentials/firebase_credentials.json', 'r') as f:
            credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(credentials)
        self.database = self.firebase.database()
        self.emailSender = EmailSender()
        self.scraper = Scraper(on_proxy=on_proxy, headless=not debug)

    def start(self):
        log_info("Starting new scraper instance..")

        index = 0
        scrape_flag = None
        locale_list = supported_locales()
        while 1:
            locale_code = locale_list[index % len(locale_list)]
            remove_temp_dir(locale_code)
            start_time = get_current_pst_time()
            self.scraper.get_product_info(locale_code)
            last_scrape_flag = scrape_flag
            self.scraper.open_with_timeout("https://www.google.com/", 10)
            scrape_flag, scrape_results = self.scraper.scrape_result()
            scraper_timestamp = self.scraper.get_timestamp()
            database_log_prefix = '{}/logs/task/{}/{}'.format(locale_code, scraper_timestamp[:8], scraper_timestamp[9:])
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
                        '{}/logs/key_timestamps/{}/{}'.format(locale_code, scraper_timestamp[:8], scraper_timestamp[9:])).set(
                        scrape_flag)
                self.scraper.terminate()
                log_warning("Starting new scraper instance..")
                self.scraper = Scraper(on_proxy=self.on_proxy, headless=not self.debug)
            else:
                if not last_scrape_flag:
                    self.database.child(
                        '{}/logs/key_timestamps/{}/{}'.format(locale_code, scraper_timestamp[:8], scraper_timestamp[9:])).set(
                        scrape_flag)
                log_info("update products info attempt started")
                products_upload_result = self.deltaChecker.upload_products_if_necessary(scraper_timestamp, locale_code)
                log_info("updated product? : {}".format(products_upload_result))
                self.database.child('{}/upload'.format(database_log_prefix)).set(products_upload_result)
                log_info("delta update attempt started")
                delta_realtime_update_result = self.deltaChecker.update_realtime_delta(scraper_timestamp, locale_code)
                log_info("delta updated? : {}".format(delta_realtime_update_result))
                should_send_realtime_update_email = False
                for category_code in delta_realtime_update_result:
                    if delta_realtime_update_result[category_code] == "SUCCESS":
                        should_send_realtime_update_email = True
                if should_send_realtime_update_email:
                    try:
                        self.emailSender.send_realtime_update(locale_code)
                    except Exception:
                        log_info("Failed to send daily email!")
                self.database.child('{}/delta_realtime'.format(database_log_prefix)).set(delta_realtime_update_result)
                delta_daily_update_result = self.deltaChecker.update_daily_delta_if_necessary(locale_code)
                log_info("delta daily updated? : {}".format(delta_daily_update_result))
                self.database.child('{}/delta_daily'.format(database_log_prefix)).set(delta_daily_update_result)
                if delta_daily_update_result == "SUCCESS":
                    try:
                        self.emailSender.send_daily_update(get_current_pst_format_date(), locale_code)
                    except Exception:
                        log_info("Failed to send daily email!")
            time_used_in_seconds = (get_current_pst_time() - start_time).total_seconds()
            self.database.child('{}/time_used'.format(database_log_prefix)).set(time_used_in_seconds)
            log_info("==========\n"
                     "  country: {}\n"
                     "  index: {}\n"
                     "  timestamp: {}\n"
                     "  time_used: {}\n"
                     "  scrape_results: {}\n"
                     "  products_upload: {}\n"
                     "  delta_realtime: {}\n"
                     "  delta_daily: {}".format(locale_code, index,
                                                scraper_timestamp,
                                                time_used_in_seconds,
                                                scrape_results,
                                                products_upload_result,
                                                delta_realtime_update_result,
                                                delta_daily_update_result))

            if last_scrape_flag == "BLOCKED" and scrape_flag == "BLOCKED":
                self.scraper.terminate()
                raise Exception("Scraper is Blocked")
                break
            time_until_next_scrape = self.interval_seconds - time_used_in_seconds
            log_info("========== time_until_next_scrape:{}".format(time_until_next_scrape))
            if time_until_next_scrape > 0:
                time.sleep(time_until_next_scrape)
            index += 1


    def terminate_scraper(self):
        self.scraper.terminate()