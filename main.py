import sys
import argparse
from EmailSender import EmailSender
from ScrapeTask import ScrapeTask
from UpdateTask import UpdateTask
from Utils import SlowIPException, log_exception, supported_locales, BlockedIPException, timeout, TimeoutError


# @timeout(3600 * 4)
# def scrape_backup(local_code, job_type, proxy_list=None):
#     while 1:
#         try:
#             task = ScrapeTask(local_code, proxy_list=proxy_list)
#             task.start()
#         except SlowIPException as e:
#             log_exception(e)
#         except BlockedIPException as e:
#             log_exception(e)
#             email_sender = EmailSender()
#             email_sender.notice_admins_on_exception(e, local_code, job_type)
#             sys.exit(-1)
#         except TimeoutError as e:
#             log_exception(e)
#             sys.exit(0)
#         except Exception as e:
#             log_exception(e)
#             email_sender = EmailSender()
#             email_sender.notice_admins_on_exception(e, local_code, job_type)
#

def scrape(local_code, job_type, proxy_list=None):
    blocked = False
    while not blocked:
        try:
            task = ScrapeTask(local_code, proxy_list=proxy_list, debug=False)
            task.start()
        except SlowIPException as e:
            log_exception(e)
        except BlockedIPException as e:
            log_exception(e)
            blocked = True
        except Exception as e:
            log_exception(e)
            email_sender = EmailSender()
            email_sender.notice_admins_on_exception(e, local_code, job_type)


def update(local_code, job_type):
    try:
        task = UpdateTask(local_code)
        task.start()
    except Exception as e:
        log_exception(e)
        email_sender = EmailSender()
        email_sender.notice_admins_on_exception(e, local_code, job_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Main entrance to hermes scraper / updater / emailSender')
    parser.add_argument('-l', '--locale', help='job locale (e.g us_en, cn_zh)', required=True)
    parser.add_argument('-t', '--type', help='job type (e.g scraping, updating)', required=True)
    parser.add_argument('-p', '--proxy_list', help='proxy list that scraping jobs run on', required=False)
    args = parser.parse_args()

    print("Start job with arguments: {}".format(args))

    local_code = args.locale
    if local_code not in supported_locales():
        print("{} is not supported. {}".format(local_code, supported_locales()))
        sys.exit(-1)

    job_type = args.type

    if job_type == "scraping":
        if args.proxy_list:
            proxy_list = args.proxy_list.split(',')
        else:
            proxy_list = None
        scrape(local_code, job_type, proxy_list)
    elif job_type == "updating":
        update(local_code, job_type)
    else:
        print("job_type {} is not supported.".format(job_type))
    sys.exit(-1)
