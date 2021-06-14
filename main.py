import sys

from EmailSender import EmailSender
from ScrapeTask import ScrapeTask
from Utils import wait_random

task = ScrapeTask(iterations=-1, interval_seconds=60 * 3, debug=False, on_proxy=True)
task.start()
# retry = 0
# retry_limit = 2
# while retry < retry_limit:
#     try:
#
#     except Exception as e:
#         emailSender = EmailSender()
#         emailSender.notice_admins_on_exception(e, retry)
#         retry += 1
#         wait_random(2, 3)
#
# sys.exit(-1)
