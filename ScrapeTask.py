import time
from datetime import datetime

from Scraper import Scraper
from Uploader import Uploader
from Utils import supported_categories, log_info


class ScrapeTask:
    def __init__(self, iterations, interval_seconds=60, debug=False, on_proxy=False):
        self.category_codes = supported_categories()
        self.results_dict = {}
        self.iterations = iterations
        self.interval_seconds = interval_seconds
        self.debug = debug
        self.uploader = Uploader()
        self.on_proxy = on_proxy

    def start(self):
        scraper = Scraper(on_proxy=self.on_proxy, headless=not self.debug)
        i = 0
        while 1:
            start_time = datetime.now()
            scraper.get_product_info()
            flag, results = scraper.all_set()
            time_used_in_seconds = (datetime.now() - start_time).total_seconds()
            if flag not in self.results_dict:
                self.results_dict[flag] = 0
            self.results_dict[flag] += 1
            log_info("===== {} - result:{}".format(i, [start_time, time_used_in_seconds, flag, results]))
            i += 1
            if i == self.iterations:
                scraper.terminate()
                break
            if flag != "SUCCESS":
                scraper.terminate()
                scraper = Scraper(on_proxy=self.on_proxy, headless=not self.debug)
            else:
                log_info("upload started")
                self.uploader.upload_products(timestamp=scraper.timestamp)

                time_until_next_scrape = self.interval_seconds - time_used_in_seconds
                log_info("===== time_until_next_scrape:{}".format(time_until_next_scrape))
                if time_until_next_scrape > 0:
                    time.sleep(time_until_next_scrape)

        log_info(self.results_dict)
