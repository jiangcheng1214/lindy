from EmailSender import EmailSender
from ScrapeTask import ScrapeTask


try:
    task = ScrapeTask(interval_seconds=60, debug=False, on_proxy=False)
    task.start()
except Exception as e:
    email_sender = EmailSender()
    email_sender.notice_admins_on_exception(e)
