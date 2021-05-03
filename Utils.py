import inspect
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')


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


def create_empty_file(dir_path, name):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = os.path.join(dir_path, name)
    with open(file_path, 'w+'):
        log_info('{} created!'.format(file_path))


def supported_categories():
    # return ['WOMENSILKSCARVESETC', 'WOMENBAGSSMALLLEATHERGOODS']
    return ['WOMENBAGSSMALLLEATHERGOODS']
