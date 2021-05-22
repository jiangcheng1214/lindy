import inspect
import logging
import os
import shutil
from datetime import datetime
from pytz import timezone, utc
import random
import time


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
        print('{} created!'.format(file_path))

ts = get_current_pst_format_timestamp()
create_empty_file("logs/", "{}.log".format(ts))
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s', handlers=[
    logging.FileHandler("logs/{}.log".format(ts)),
    logging.StreamHandler()
])


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
    log_info("random_wait: {}".format(random_wait))
    time.sleep(random_wait)


def supported_categories():
    return ['WOMENBAGSSMALLLEATHERGOODS', 'BIJOUTERIE', 'MENBAGSSMALLLEATHERGOODS']


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
