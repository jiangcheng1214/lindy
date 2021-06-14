from EmailSender import EmailSender
from ScrapeTask import ScrapeTask
from Utils import SlowIPError, log_exception

i = 0
while i < 5:
    i += 1
    try:
        task = ScrapeTask(interval_seconds=60*3, debug=False, on_proxy=False)
        task.start()
    except SlowIPError as e:
        log_exception(e)
        # task.terminate_scraper()
    except Exception as e:
        email_sender = EmailSender()
        email_sender.notice_admins_on_exception(e)
        task.terminate_scraper()
