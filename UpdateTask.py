import json
import time
from DeltaChecker import DeltaChecker
from EmailSender import EmailSender
from Utils import log_info, get_current_pst_format_date, get_current_pst_time
import pyrebase


class UpdateTask:
    def __init__(self, locale_code, interval_seconds=60):
        self.locale_code = locale_code
        self.interval_seconds = interval_seconds
        self.deltaChecker = DeltaChecker()
        with open('credentials/firebase_credentials.json', 'r') as f:
            credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(credentials)
        self.database = self.firebase.database()
        self.emailSender = EmailSender()

    def start(self):
        index = 0
        while 1:
            log_info("delta update attempt started")
            start_time = get_current_pst_time()
            scraper_timestamp = self.deltaChecker.get_timestamp_scraped_forward(self.locale_code)
            database_log_prefix = '{}/logs/task/{}/{}'.format(self.locale_code, scraper_timestamp[:8],
                                                              scraper_timestamp[9:])
            delta_realtime_update_result = self.deltaChecker.update_realtime_delta_if_necessary(self.locale_code)
            log_info("delta updated? : {}".format(delta_realtime_update_result))
            delta_realtime_update_result == "SUCCESS"
            if delta_realtime_update_result:
                try:
                    self.emailSender.send_realtime_update(self.locale_code)
                except Exception as e:
                    log_info("Failed to send realtime update email! {}".format(e))
            self.database.child('{}/delta_realtime'.format(database_log_prefix)).set(delta_realtime_update_result)
            delta_daily_update_result = self.deltaChecker.update_daily_delta_if_necessary(self.locale_code)
            log_info("delta daily updated? : {}".format(delta_daily_update_result))
            self.database.child('{}/delta_daily'.format(database_log_prefix)).set(delta_daily_update_result)
            if delta_daily_update_result == "SUCCESS":
                try:
                    self.emailSender.send_daily_update(get_current_pst_format_date(), self.locale_code)
                except Exception:
                    log_info("Failed to send daily email! {}".format(e))

            time_used_in_seconds = (get_current_pst_time() - start_time).total_seconds()
            self.database.child('{}/time_used'.format(database_log_prefix)).set(time_used_in_seconds)
            log_info("==========\n"
                     "  country: {}\n"
                     "  index: {}\n"
                     "  timestamp: {}\n"
                     "  time_used: {}\n"
                     "  delta_realtime_update_result: {}\n"
                     "  delta_daily_update_result: {}".format(self.locale_code, index,
                                                              scraper_timestamp,
                                                              time_used_in_seconds,
                                                              delta_realtime_update_result,
                                                              delta_daily_update_result))
            time_until_next_scrape = self.interval_seconds - time_used_in_seconds
            log_info("========== time_until_next_scrape:{}".format(time_until_next_scrape))
            if time_until_next_scrape > 0:
                time.sleep(time_until_next_scrape)
            index += 1
