import sys

from EmailSender import EmailSender
from ScrapeTask import ScrapeTask
from Utils import SlowIPException, log_exception, supported_locales, BlockedIPException

n = len(sys.argv)
print("Total arguments passed:", n)
print("arguments passed:", sys.argv)
assert len(sys.argv) == 3
local_code = sys.argv[1]
if local_code not in supported_locales():
    print("{} is not supported. {}".format(local_code, supported_locales()))
    sys.exit(-1)

job_type = sys.argv[2]
if job_type == "scraping":
    blocked = False
    while not blocked:
        try:
            task = ScrapeTask(interval_seconds=60*3, debug=False, on_proxy=False)
            task.start()
        except SlowIPException as e:
            log_exception(e)
        except BlockedIPException as e:
            log_exception(e)
            blocked = True
        except Exception as e:
            log_exception(e)
            email_sender = EmailSender()
            email_sender.notice_admins_on_exception(e)
        finally:
            task.terminate_scraper()
elif job_type == "updating":
    # TODO:
    pass
else:
    print("job_type {} is not supported.".format(job_type))
sys.exit(-1)
