import re
import sys
import argparse
from EmailSender import EmailSender
from ScrapeTask import ScrapeTask
from UpdateTask import UpdateTask
from Utils import SlowIPException, log_exception, supported_locales, ConsecutiveFailureException, timeout, wait_random, \
    TimeoutException, log_warning, log_info


@timeout(3600 * 2.5)  # timeout in 2.5 hrs
def scrape_with_timeout(local_code, job_type, proxy_list=None, debug=False):
    failed = False
    while not failed:
        try:
            task = ScrapeTask(local_code, proxy_list=proxy_list, debug=debug)
            task.start()
        except SlowIPException as e:
            log_exception(e)
        except ConsecutiveFailureException as e:
            log_exception(e)
            failed = True
        except TimeoutException:
            log_warning("Timeout! Terminated the program.")
            sys.exit(0)
        except Exception as e:
            log_exception(e)
            if not debug:
                email_sender = EmailSender()
                email_sender.notice_admins_on_exception(e, local_code, job_type)


def rest(hours):
    log_info("start sleeping")
    wait_random(3600 * hours, 3600 * hours)
    log_info("end sleeping")
    sys.exit(0)


def scrape(local_code, job_type, proxy_list=None, debug=False):
    failed = False
    while not failed:
        try:
            task = ScrapeTask(local_code, proxy_list=proxy_list, debug=debug)
            task.start()
        except SlowIPException as e:
            log_exception(e)
        except ConsecutiveFailureException as e:
            log_exception(e)
            failed = True
        except Exception as e:
            log_exception(e)
            if not debug:
                email_sender = EmailSender()
                email_sender.notice_admins_on_exception(e, local_code, job_type)


def update(local_code, job_type, debug=False):
    try:
        task = UpdateTask(local_code)
        task.start()
    except Exception as e:
        log_exception(e)
        if not debug:
            email_sender = EmailSender()
            email_sender.notice_admins_on_exception(e, local_code, job_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Main entrance to hermes scraper / updater / emailSender')
    parser.add_argument('-t', '--type', help='job type (e.g scraping, updating)', required=True)
    parser.add_argument('-l', '--locale', help='job locale (e.g us_en, cn_zh)', required=False)
    parser.add_argument('-p', '--proxy_list', help='proxy list that scraping jobs run on', required=False)
    parser.add_argument('-d', '--debug', help='debug mode', required=False, action="store_true")
    parser.add_argument('-o', '--timeout', help='timeout mode', required=False, action="store_true")
    args = parser.parse_args()

    print("Start job with arguments: {}".format(args))
    job_type = args.type
    if job_type == "scraping":
        assert args.locale in supported_locales()
        if args.proxy_list:
            proxy_list = re.split(',', args.proxy_list)
        else:
            proxy_list = None
        if args.timeout:
            scrape_with_timeout(args.locale, job_type, proxy_list, debug=args.debug)
        else:
            scrape(args.locale, job_type, proxy_list, debug=args.debug)
    elif job_type == "updating":
        assert args.locale in supported_locales()
        update(args.locale, job_type, debug=args.debug)
    elif job_type == "resting":
        rest(4)
    else:
        print("job_type {} is not supported.".format(job_type))
    sys.exit(0)
