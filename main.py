from EmailSender import EmailSender
from ScrapeTask import ScrapeTask

retry = False
while not retry:
    try:
        task = ScrapeTask(iterations=-1, interval_seconds=60, debug=False, on_proxy=False)
        task.start()
    except Exception as e:
        emailSender = EmailSender()
        emailSender.notice_admins_on_exception(e, retry)
        retry = True
