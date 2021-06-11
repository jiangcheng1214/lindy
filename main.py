from EmailSender import EmailSender
from ScrapeTask import ScrapeTask
# i = 0
from Utils import SlowIPError, log_exception

while 1:
    # i += 1
    try:
        task = ScrapeTask(interval_seconds=60*1, debug=False, on_proxy=True)
        task.start()
    except SlowIPError as e:
        log_exception(e)
        # task.terminate_scraper()
    except Exception as e:
        email_sender = EmailSender()
        email_sender.notice_admins_on_exception(e)
        task.terminate_scraper()
