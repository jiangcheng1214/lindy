import time
from datetime import datetime

from Scraper import Scraper
from Uploader import Uploader


class ScrapeTask:
    def __init__(self, iterations, interval_seconds=30,
                 category_codes=['WOMENSILKSCARVESETC', 'WOMENBAGSSMALLLEATHERGOODS'], debug=False):
        self.category_codes = category_codes
        self.results_dict = {}
        self.iterations = iterations
        self.interval_seconds = interval_seconds
        self.debug = debug
        self.uploader = Uploader()

    def start(self):
        scraper = Scraper(category_codes=self.category_codes, headless=not self.debug)
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
                scraper = Scraper(category_codes=self.category_codes, headless=not self.debug)
            else:
                print("upload started")
                self.uploader.upload_products(timestamp=scraper.timestamp)

                time_until_next_scrape = self.interval_seconds - time_used_in_seconds
                print("===== time_until_next_scrape:{}".format(time_until_next_scrape))
                if time_until_next_scrape > 0:
                    time.sleep(time_until_next_scrape)
            print("===== {} - result:{}".format(i, [start_time, time_used_in_seconds, flag, results]))
            i += 1
            if i == self.iterations:
                scraper.terminate()
                break
        print(self.results_dict)
