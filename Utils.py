import inspect
import logging
import shutil
from datetime import datetime
from pytz import timezone, utc
import random
import time
from functools import wraps
import errno
import os
import signal

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')


class SlowIPException(Exception):
    pass


class BlockedIPException(Exception):
    pass


class TimeoutException(Exception):
    pass


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutException(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(int(seconds))
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def get_current_pst_format_timestamp():
    return datetime.now(tz=utc).astimezone(timezone('US/Pacific')).strftime("%Y%m%d_%H_%M_%S")


def get_current_pst_format_date():
    return datetime.now(tz=utc).astimezone(timezone('US/Pacific')).strftime("%Y%m%d")


def get_current_pst_format_year_month():
    return datetime.now(tz=utc).astimezone(timezone('US/Pacific')).strftime("%Y%m")


def get_current_pst_time():
    return datetime.now(tz=utc).astimezone(timezone('US/Pacific'))


def get_datetime_from_string(timestamp):
    return datetime.strptime(timestamp, "%Y%m%d_%H_%M_%S")


def create_empty_file(dir_path, name):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = os.path.join(dir_path, name)
    with open(file_path, 'w+'):
        log_info('{} created!'.format(file_path))


def log_info(s):
    logging.info(s)


def log_exception(s):
    callerframerecord = inspect.stack()[1]
    frame = callerframerecord[0]
    information = inspect.getframeinfo(frame)
    msg = "Exception: {} {}:{}".format(s, information.filename, information.lineno)
    logging.exception(msg)


def log_warning(s):
    callerframerecord = inspect.stack()[1]
    frame = callerframerecord[0]
    information = inspect.getframeinfo(frame)
    msg = "Exception: {} {}:{}".format(s, information.filename, information.lineno)
    logging.warning(msg)


def wait_random(lower_bound, higher_bound):
    assert lower_bound <= higher_bound
    random_wait = random.uniform(lower_bound, higher_bound)
    time.sleep(random_wait)


def supported_categories():
    return ['WOMENBAGSSMALLLEATHERGOODS', 'MENBAGSSMALLLEATHERGOODS']
    # , 'BIJOUTERIE'


def supported_locales():
    return ['us_en', 'cn_zh', 'uk_en', 'de_de']


def flag_for_country(locale_code):
    locale_flags = {
        'us_en': '🇺🇸',
        'cn_zh': '🇨🇳',
        'uk_en': '🇬🇧',
        'de_de': '🇧🇪'
    }
    if locale_code in locale_flags:
        return locale_flags[locale_code]
    else:
        return '🏳‍🌈'

def close_all_other_tabs(driver):
    cur = driver.current_window_handle
    for handle in driver.window_handles:
        wait_random(0.5, 1)
        driver.switch_to.window(handle)
        if handle != cur:
            driver.close()
    driver.switch_to.window(cur)


def delete_dir(path):
    if os.path.exists(path) and os.path.isdir(path):
        log_info("removing dir: {}".format(path))
        shutil.rmtree(path)
