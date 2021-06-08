import os
from threading import Thread

import Scraper
from EmailSender import EmailSender
from ScrapeTask import ScrapeTask
from Utils import supported_locales, wait_random


class ThreadWrappedTask(Thread):
    def __init__(self, locale_code):
        Thread.__init__(self)
        self.locale_code = locale_code

    def run(self):
        retry = 0
        retry_limit = 2
        while retry < retry_limit:
            try:
                task = ScrapeTask(self.locale_code, iterations=-1, interval_seconds=60 * 5, debug=False, on_proxy=False)
                task.start()
            except Exception as e:
                email_sender = EmailSender()
                email_sender.notice_admins_on_exception(e, self.locale_code, retry)
                retry += 1
                wait_random(2, 3)
        print("Exit the whole program due to one thread exception")
        os._exit(1)


for locale_code in supported_locales():
    threaded_task = ThreadWrappedTask(locale_code)
    threaded_task.start()
    wait_random(360, 360)