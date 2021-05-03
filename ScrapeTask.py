import time
from datetime import datetime

from Scraper import Scraper
from Uploader import Uploader
from Utils import supported_categories, log_info


class ScrapeTask:
    def __init__(self, iterations, interval_seconds=5*60, debug=False):
        self.category_codes = supported_categories()
        self.results_dict = {}
        self.iterations = iterations
        self.interval_seconds = interval_seconds
        self.debug = debug
        self.uploader = Uploader()

    def start(self):
        scraper = Scraper(headless=not self.debug)
        i = 0
        while 1:
            start_time = datetime.now()
            scraper.get_product_info()
            flag, results = scraper.all_set()
            time_used_in_seconds = (datetime.now() - start_time).total_seconds()
            if flag not in self.results_dict:
                self.results_dict[flag] = 0
            self.results_dict[flag] += 1
            if flag != "SUCCESS":
                scraper.terminate()
                scraper = Scraper(headless=not self.debug)
            else:
                log_info("upload started")
                self.uploader.upload_products(timestamp=scraper.timestamp)

                time_until_next_scrape = self.interval_seconds - time_used_in_seconds
                log_info("===== time_until_next_scrape:{}".format(time_until_next_scrape))
                if time_until_next_scrape > 0:
                    time.sleep(time_until_next_scrape)
            log_info("===== {} - result:{}".format(i, [start_time, time_used_in_seconds, flag, results]))
            i += 1
            if i == self.iterations:
                scraper.terminate()
                break
        print(self.results_dict)
