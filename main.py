import sys

from EmailSender import EmailSender
from ScrapeTask import ScrapeTask
from Utils import SlowIPException, log_exception, supported_locales, BlockedIPException, timeout, TimeoutError


@timeout(3600 * 4)
def scrape_backup():
    blocked = False
    while not blocked:
        try:
            task = ScrapeTask(local_code, interval_seconds=60 * 3, debug=False, on_proxy=False)
            task.start()
        except SlowIPException as e:
            log_exception(e)
        except BlockedIPException as e:
            log_exception(e)
            if job_type == "backup_scraping":
                email_sender = EmailSender()
                email_sender.notice_admins_on_exception(e, local_code, job_type)
            blocked = True
        except TimeoutError:
            print('timeout!')
            sys.exit(0)
        except Exception as e:
            log_exception(e)
            email_sender = EmailSender()
            email_sender.notice_admins_on_exception(e, local_code, job_type)
        finally:
            task.terminate_scraper()


def scrape():
    blocked = False
    while not blocked:
        try:
            task = ScrapeTask(local_code, interval_seconds=60 * 3, debug=False, on_proxy=False)
            task.start()
        except SlowIPException as e:
            log_exception(e)
        except BlockedIPException as e:
            log_exception(e)
            if job_type == "backup_scraping":
                email_sender = EmailSender()
                email_sender.notice_admins_on_exception(e, local_code, job_type)
            blocked = True
        except Exception as e:
            log_exception(e)
            email_sender = EmailSender()
            email_sender.notice_admins_on_exception(e, local_code, job_type)
        finally:
            task.terminate_scraper()


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
    scrape()
elif job_type == "backup_scraping":
    scrape_backup()
elif job_type == "updating":
    # TODO:
    pass
else:
    print("job_type {} is not supported.".format(job_type))
sys.exit(-1)
