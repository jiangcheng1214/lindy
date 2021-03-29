import inspect
from datetime import datetime
import json
import os
import re
import time
import urllib
from random import random

import pydub
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.proxy import Proxy, ProxyType

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

import constants
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import speech_recognition as sr

CHROMEDRIVER_BIN_PATH = '/usr/local/bin/chromedriver'


# global waiting_interval
# waiting_interval = 5

class ScraperReport:
    def __init__(self):
        self.block_timestamp = None
        self.get_call_count = 0
        self.unblock_timestamp = None
        self.allset_timestamps = []

    def reset(self):
        print("========== report ==========")
        print("========== unblock_timestamp:{} ==========".format(self.unblock_timestamp))
        print("========== block_timestamp:{} ==========".format(self.block_timestamp))
        print("========== allset_timestamps ==========")
        for ts in self.allset_timestamps:
            print("====== ts: {}======".format(ts))
        print("========== waiting_interval:{} minutes ==========".format(waiting_interval))
        self.block_timestamp = None
        self.get_call_count = 0
        self.unblock_timestamp = None
        self.allset_timestamps = []


# scraperReport = ScraperReport()

def log(s):
    print("[{}] {}".format(datetime.now(), s))


def create_empty_file(dir_path, name):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = os.path.join(dir_path, name)
    with open(file_path, 'w+'):
        log('{} created!'.format(file_path))


def log_exception(s):
    callerframerecord = inspect.stack()[1]
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    print("[{}] Exception: {}\n     File: {}\n     Line: {}".format(datetime.now(), s, info.filename, info.lineno))


class LocaleInfo:
    def __init__(self, country_name, country_code, language_code, href):
        self.country_name = country_name
        self.country_code = country_code
        self.language_code = language_code
        self.href = href

    def __str__(self):
        return json.dumps({
            "country_name": self.country_name.decode('utf-8'),
            "country_code": self.country_code,
            "language_code": self.language_code,
            "href": self.href,
        })


class Category:
    def __init__(self, name, code, path, has_visible_products, level):
        self.name = name
        self.code = code
        self.path = path
        self.has_visible_products = has_visible_products
        self.level = level

    def __str__(self):
        return json.dumps({
            "level": self.level,
            "name": self.name,
            "code": self.code,
            "path": self.path,
            "has_visible_products": self.has_visible_products,
        })


class Scraper:
    def __init__(self, proxy=None, headless=False, locale_code='us/en'):
        options = Options()
        # setup userAgent
        software_names = [SoftwareName.CHROME.value]
        operation_systems = [OperatingSystem.WINDOWS.name, OperatingSystem.LINUX.name]
        user_agent_rotator = UserAgent(software_names=software_names, operation_systems=operation_systems, limit=100)
        user_agent = user_agent_rotator.get_random_user_agent()
        options.add_argument(f'user-agent={user_agent}')
        # mute audio during cracking recapcha
        options.add_argument("--mute-audio")
        # setup headless mode
        if headless:
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
        # setup proxy
        capabilities = webdriver.DesiredCapabilities.CHROME
        if proxy:
            p = Proxy()
            p.proxyType = ProxyType.MANUAL
            p.autodetect = False
            p.http_proxy = proxy
            p.ssl_proxy = proxy
            p.socks_proxy = proxy
            p.add_to_capabilities(capabilities)

        options.add_argument('--window-size=1420,1080')
        self.driver = webdriver.Chrome(CHROMEDRIVER_BIN_PATH, options=options, desired_capabilities=capabilities)

        self.locale_code = locale_code
        self.ts = datetime.now().strftime("%Y%m%d_%H_%M_%S")
        self.data_dir_path = os.path.join(os.getcwd(), 'data/{}'.format(self.ts))
        if not os.path.isdir(self.data_dir_path):
            os.makedirs(self.data_dir_path)
        self.temp_dir_path = os.path.join(os.getcwd(), 'temp/{}'.format(self.ts))
        if not os.path.isdir(self.temp_dir_path):
            os.makedirs(self.temp_dir_path)
        self.product_dir_path = os.path.join(self.data_dir_path, 'product')
        if not os.path.exists(self.product_dir_path):
            os.makedirs(self.product_dir_path)

    def is_detected_by_anti_bot(self):
        if self.driver.find_elements_by_xpath('//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]'):
            log("detected by anti-bot")
            return True
        return False

    def is_blocked(self):
        if self.driver.find_elements_by_xpath('//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]'):
            self.driver.switch_to.frame(
                self.driver.find_elements_by_xpath(
                    '//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]')[0])
            if self.driver.find_elements_by_xpath('//div[contains(text(), "You have been blocked.")]'):
                self.driver.switch_to.default_content()
                return True
            self.driver.switch_to.default_content()
            return False
        return False

    def type_with_delay(self, xpath, value):
        try:
            if self.driver.find_elements_by_xpath(xpath):
                self.driver.find_elements_by_xpath(xpath)[0].clear()
                for c in value:
                    self.driver.find_elements_by_xpath(xpath)[0].send_keys(c)
                    time.sleep(random() / 10)
            else:
                log("xpath not found: {}".format(xpath))
                return False
        except Exception as ex:
            log_exception(ex)
            return False
        return True

    def solve_recaptha(self):
        def solve_audio_recaptha_attempt(retry_count=0):
            log("solve_audio_recaptha_attempt retry_count={}".format(retry_count))
            try:
                WebDriverWait(self.driver, 10).until(expected_conditions.presence_of_element_located(
                    (By.XPATH, '//div[@class="geetest_replay"]')))
                time.sleep(1)
                WebDriverWait(self.driver, 10).until(expected_conditions.presence_of_element_located(
                    (By.XPATH, '//div[@style="display: none;" and @class="geetest_text_tip"]')))
            except Exception as ex:
                log_exception(ex)
                return False
            try:
                self.driver.find_element_by_xpath('//div[@class="geetest_replay"]').click()
                log("clicked play audio button geetest_replay")
                WebDriverWait(self.driver, 10).until(expected_conditions.presence_of_element_located(
                    (By.XPATH, '//audio[@class="geetest_music"]')))
            except Exception as ex:
                log_exception(ex)
                return False
            audio_element = self.driver.find_element_by_xpath('//audio[@class="geetest_music"]')
            src = audio_element.get_attribute('src')
            if not src:
                log('recapcha audio src is null!')
                return False
            local_mp3_path = os.path.join(self.temp_dir_path, 'recaptha_audio.mp3')
            urllib.request.urlretrieve(src, local_mp3_path)
            log('recapcha audio downloaded')
            sound = pydub.AudioSegment.from_mp3(local_mp3_path)
            local_wav_path = os.path.join(self.temp_dir_path, 'recaptha_audio.wav')
            sound.export(local_wav_path, format='wav')
            sample_audio = sr.AudioFile(local_wav_path)
            r = sr.Recognizer()
            with sample_audio as source:
                audio = r.record(source)
            recognized_string = r.recognize_google(audio).lower()
            log('recapcha recognized_string: {}'.format(recognized_string))
            subs = {
                " to ": " 2 ",
                " too ": " 2 ",
                " for ": " 4 ",
            }
            processed_string = recognized_string
            for k in subs:
                processed_string = re.sub(k, subs[k], processed_string)
            numeric_string = re.sub("[^0-9]", "", processed_string)
            log('recapcha processed_string: {}'.format(numeric_string))
            self.type_with_delay('//input[@class="geetest_input"]', numeric_string)
            self.driver.find_element_by_xpath('//input[@class="geetest_input"]').send_keys(Keys.ENTER)
            try:
                WebDriverWait(self.driver, 10).until(
                    lambda driver: not driver.find_elements_by_xpath('//div[@class="geetest_replay"]'))
            except Exception:
                log_exception('recapcha solving failed!')
                if retry_count >= 5:
                    return False
                self.driver.find_element_by_xpath('//div[@class="geetest_refresh"]').click()
                time.sleep(0.5)
                log("clicked refresh audio button geetest_refresh")
                return solve_audio_recaptha_attempt(retry_count=retry_count + 1)
            return True

        captcha_iframe = self.driver.find_element_by_xpath(
            '//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]')
        self.driver.switch_to.frame(captcha_iframe)
        log("switched to captcha_iframe")
        try:
            WebDriverWait(self.driver, 30).until(expected_conditions.presence_of_element_located(
                (By.XPATH, '//div[@class="geetest_radar_btn"]')))
            self.driver.find_element_by_xpath('//div[@class="geetest_radar_btn"]').click()
            log("clicked verify button geetest_radar_btn")
            WebDriverWait(self.driver, 30).until(expected_conditions.presence_of_element_located(
                (By.XPATH, '//a[@class="geetest_voice"]')))
            time.sleep(1)
            self.driver.find_element_by_xpath('//a[@class="geetest_voice"]').click()
            log("clicked voice verify button geetest_voice")
        except Exception as ex:
            log_exception(ex)
            return False
        anti_bot_solved = solve_audio_recaptha_attempt()
        self.driver.switch_to.default_content()
        return anti_bot_solved

    def open_url_and_crack_antibot(self, url):
        self.driver.get(url)
        log("opened url: {}".format(url))
        # time.sleep(1)
        if self.is_blocked():
            log('blocked!')
            return False
        if self.is_detected_by_anti_bot():
            if self.solve_recaptha():
                log('solve_recaptha done!')
                return True
            else:
                log('solve_recaptha failed!')
                return False
        else:
            return True

    # [['United Arab Emirates', 'dh', 'en', 'https://www.hermes.com/dh/en/'], ['Suisse', 'ch', 'fr', 'https://www.hermes.com/ch/fr/'], ...]
    def get_locale_info_list(self):
        log("get_locale_info_list started")
        # e.g <a href="https://www.hermes.com/de/de/" class="ripple-effect">Deutschland</a>
        open_success = self.open_url_and_crack_antibot(constants.HERMES_HOME_URL)
        results = []
        if open_success:
            try:
                WebDriverWait(self.driver, 5).until(expected_conditions.presence_of_element_located(
                    (By.XPATH, '//a[@class="ripple-effect"]')))
                for element in self.driver.find_elements_by_xpath('//a[@class="ripple-effect"]'):
                    href = str(element.get_attribute('href'))
                    if '.com' in href:
                        parts = href.split('.com')[-1].strip('/').split('/')
                        country_code = parts[0]
                        language_code = parts[-1]
                        country_name = element.get_attribute('text').encode('utf-8')
                        results.append(LocaleInfo(country_name, country_code, language_code, href))
            except Exception as ex:
                log_exception(ex)
                create_empty_file(self.data_dir_path, "FAIL_LOCALE")
                return
            log("results count = {}".format(len(results)))
        else:
            log("failed to open {}".format(constants.HERMES_HOME_URL))
            create_empty_file(self.data_dir_path, "FAIL_LOCALE")
            return
        locale_file_path = os.path.join(self.data_dir_path, 'locale.json')
        with open(locale_file_path, 'w+') as f:
            for r in results:
                json.dump(r, f)
                f.write('\n')
        log("Finished writing to {}".format(locale_file_path))
        create_empty_file(self.data_dir_path, "SUCCESS_LOCALE")

    def get_category(self):

        def get_leveled_category(json_data):
            leveled_category_results = []
            cur = [json_data]
            level = 0
            while cur:
                level_result = []
                nxt = []
                for data in cur:
                    result = {'name': data['name'], 'code': data['pimCode'], 'path': data['path'],
                              'has_visible_products': data['has_visible_products'], 'level': level}
                    leveled_category_results.append(result)
                    level_result.append([])
                    if 'children' in data:
                        for child in data['children']:
                            nxt.append(child)
                cur = nxt
                level += 1
            return leveled_category_results

        log("get_category started")
        url = constants.HERMES_CATEGORIES_API.format(self.locale_code)
        open_success = self.open_url_and_crack_antibot(url)
        if open_success:
            try:
                WebDriverWait(self.driver, 30).until(lambda driver: driver.find_element_by_tag_name("pre").text)
                response_json = json.loads(self.driver.find_element_by_tag_name("pre").text)
                results = get_leveled_category(response_json)
                log("results count = {}".format(len(results)))
                if not len(results):
                    create_empty_file(self.data_dir_path, "FAIL_CATEGORY")
                    return False
            except Exception as ex:
                log_exception(ex)
                create_empty_file(self.data_dir_path, "FAIL_CATEGORY")
                return False
        else:
            log("failed to open {}".format(url))
            create_empty_file(self.data_dir_path, "FAIL_CATEGORY")
            return False
        locale_file_path = os.path.join(self.data_dir_path, 'categories.json')
        with open(locale_file_path, 'w+') as f:
            for r in results:
                json.dump(r, f)
                f.write('\n')
        create_empty_file(self.data_dir_path, "SUCCESS_CATEGORY")
        return True

    def get_product_info(self, level1_only=True):

        def get_product_info_from_category(category, retry=0):
            if retry == 5:
                log("get_product_info_from_category retry limit hit")
                return False
            log("get_product_info_from_category:{} retry:{}".format(category, retry))
            URL = constants.HERMES_PRODUCT_API.format(self.locale_code, category, constants.PRODUCT_PAGE_SIZE, 0)
            open_success = self.open_url_and_crack_antibot(URL)
            if not open_success:
                return get_product_info_from_category(category, retry+1)
            try:
                WebDriverWait(self.driver, 30).until(lambda driver: driver.find_element_by_tag_name("pre").text)
            except Exception:
                log("failed to load json response")
                return get_product_info_from_category(category, retry+1)
            response_json = json.loads(self.driver.find_element_by_tag_name("pre").text)
            total = response_json['total']
            log('total product count = {}'.format(total))
            offset = 0
            results = []
            while response_json['total'] > 0:
                products = response_json['products']['items']
                log('current product list count = {}'.format(len(products)))
                for p in products:
                    results.append(p)
                offset += constants.PRODUCT_PAGE_SIZE
                URL = constants.HERMES_PRODUCT_API.format(self.locale_code, category,
                                                          constants.PRODUCT_PAGE_SIZE,
                                                          offset)
                if not self.open_url_and_crack_antibot(URL):
                    log("open URL failed: {}".format(URL))
                    return get_product_info_from_category(category, retry+1)
                try:
                    WebDriverWait(self.driver, 10).until(
                        lambda driver: driver.find_element_by_tag_name("pre").text)
                    time.sleep(0.2)
                    response_json = json.loads(self.driver.find_element_by_tag_name("pre").text)
                except Exception:
                    log("load json failed: {}".format(URL))
                    return get_product_info_from_category(category, retry+1)
            log('results count = {}'.format(len(results)))
            if len(results) != total:
                log("result count doesn't match, result count: {}, should be {}".format(len(results), len(total)))
                return get_product_info_from_category(category, retry+1)
            file_path = os.path.join(self.product_dir_path, '{}.json'.format(category))
            with open(file_path, 'w+') as f:
                for r in results:
                    json.dump(r, f)
                    f.write('\n')
            log('results dump finished')
            return True

        category_file_path = os.path.join(self.data_dir_path, 'categories.json')
        if not os.path.exists(category_file_path):
            if not self.get_category():
                create_empty_file(self.product_dir_path, "FAIL_PRODUCTS")
                return

        with open(category_file_path, 'r') as f:
            for line in f.readlines():
                category_info = json.loads(line)
                level = category_info['level']
                should_process = (level == 1 or (level > 1 and not level1_only))
                if not should_process:
                    continue
                if get_product_info_from_category(category_info['code']):
                    create_empty_file(self.product_dir_path, "SUCCESS_{}".format(category_info['code']))
                else:
                    create_empty_file(self.product_dir_path, "FAIL_{}".format(category_info['code']))

    def all_set(self, level1_only=True):
        overall_category_success_file_path = os.path.join(self.data_dir_path, "SUCCESS_CATEGORY")
        if not os.path.exists(overall_category_success_file_path):
            return False
        try:
            categories = []
            with open(os.path.join(self.data_dir_path, 'categories.json'), 'r') as f:
                for l in f.readlines():
                    json_data = json.loads(l)
                    level = json_data['level']
                    if level == 0:
                        continue
                    if level1_only and level != 1:
                        continue
                    else:
                        categories.append(json_data['code'])
            for category in categories:
                category_success_file_pathccess = os.path.join(self.product_dir_path, "SUCCESS_{}".format(category))
                if not os.path.exists(category_success_file_pathccess):
                    return False
        except Exception as ex:
            log_exception(ex)
            return False
        return True

    def terminate(self):
        self.driver.quit()


# time.sleep(15 * 60)

scraper = Scraper(headless=False)
scraper.get_product_info()
scraper.terminate()
if scraper.all_set():
    log("ALL SET!")

# while 1:
#     try:
#         scraper = Scraper(headless=True)
#         scraper.get_product_info()
#         scraper.terminate()
#         if scraper.all_set():
#             log("ALL SET!")
#             time.sleep(15 * 60)
#         else:
#             time.sleep(5 * 60)
#     except Exception:
#         scraper.terminate()
#     finally:
#         scraper.terminate()
#
