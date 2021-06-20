from datetime import datetime
import json
import os
import random
import re
import time
import urllib

import pydub
import speech_recognition as sr
from fake_useragent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from random_user_agent.user_agent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait

import constants
from Utils import log_exception, log_info, create_empty_file, log_warning, supported_categories, \
    get_current_pst_format_timestamp, wait_random, delete_dir, SlowIPException

CHROMEDRIVER_BIN_PATH = '/usr/local/bin/chromedriver'

'''
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
'''


class Scraper:
    def __init__(self, proxy=None, headless=True):
        options = Options()

        # setup userAgent
        software_names = [SoftwareName.CHROME.value]
        operation_systems = [OperatingSystem.WINDOWS.name, OperatingSystem.LINUX.name]
        user_agent_rotator = UserAgent(software_names=software_names, operation_systems=operation_systems, limit=100)
        user_agent = user_agent_rotator.get_random_user_agent()
        options.add_argument('user-agent={}'.format(user_agent))

        # mute audio during cracking recapcha
        options.add_argument("--mute-audio")
        options.add_argument('--window-size=1420,1080')
        # setup headless mode
        if headless:
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')

        # avoid being detected
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ['enable-automation'])

        # setup proxy
        if proxy:
            options.add_argument('--proxy-server=http://{}'.format(proxy))
        self.driver = webdriver.Chrome(CHROMEDRIVER_BIN_PATH, options=options)
        self.print_ip()
        self.category_codes = supported_categories()

    def is_detected_by_anti_bot(self):
        if self.driver.find_elements_by_xpath('//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]'):
            log_info("detected by anti-bot")
            create_empty_file(self.product_dir_path, "ANTIBOT")
            return True
        return False

    def is_currently_blocked(self):
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

    def has_been_blocked(self):
        block_flag_path = os.path.join(self.product_dir_path, "BLOCKED")
        antibot_flag_path = os.path.join(self.product_dir_path, "ANTIBOT")
        return os.path.exists(block_flag_path) and not os.path.exists(antibot_flag_path)

    def type_with_delay(self, xpath, value):
        try:
            if self.driver.find_elements_by_xpath(xpath):
                self.driver.find_elements_by_xpath(xpath)[0].clear()
                for c in value:
                    self.driver.find_elements_by_xpath(xpath)[0].send_keys(c)
                    time.sleep(random.random() / 10)
            else:
                log_info("xpath not found: {}".format(xpath))
                return False
        except Exception as ex:
            log_exception(ex)
            return False
        return True

    def solve_recaptha(self, retry=0):

        def solve_audio_recaptha_attempt(retry_count=0):
            log_info("solve_audio_recaptha_attempt retry_count={}".format(retry_count))
            try:
                WebDriverWait(self.driver, 10).until(expected_conditions.presence_of_element_located(
                    (By.XPATH, '//div[@class="geetest_replay"]')))
                time.sleep(random.uniform(1, 1.5))
                WebDriverWait(self.driver, 10).until(expected_conditions.presence_of_element_located(
                    (By.XPATH, '//div[@style="display: none;" and @class="geetest_text_tip"]')))
            except Exception as ex:
                log_exception(ex)
                return False
            try:
                self.driver.find_element_by_xpath('//div[@class="geetest_replay"]').click()
                log_info("clicked play audio button geetest_replay")
                WebDriverWait(self.driver, 10).until(expected_conditions.presence_of_element_located(
                    (By.XPATH, '//audio[@class="geetest_music"]')))
            except Exception as ex:
                log_exception(ex)
                return False
            audio_element = self.driver.find_element_by_xpath('//audio[@class="geetest_music"]')
            src = audio_element.get_attribute('src')
            if not src:
                log_info('recapcha audio src is null!')
                return False
            local_mp3_path = os.path.join(self.temp_dir_path, 'recaptha_audio.mp3')
            urllib.request.urlretrieve(src, local_mp3_path)
            log_info('recapcha audio downloaded')
            sound = pydub.AudioSegment.from_mp3(local_mp3_path)
            local_wav_path = os.path.join(self.temp_dir_path, 'recaptha_audio.wav')
            sound.export(local_wav_path, format='wav')
            sample_audio = sr.AudioFile(local_wav_path)
            r = sr.Recognizer()
            with sample_audio as source:
                audio = r.record(source)
            recognized_string = r.recognize_google(audio).lower()
            log_info('recapcha recognized_string: {}'.format(recognized_string))
            subs = {
                " to ": " 2 ",
                " too ": " 2 ",
                " for ": " 4 ",
                " gate ": " 8 "
            }
            processed_string = recognized_string
            for k in subs:
                processed_string = re.sub(k, subs[k], processed_string)
            numeric_string = re.sub("[^0-9]", "", processed_string)
            log_info('recapcha processed_string: {}'.format(numeric_string))
            self.type_with_delay('//input[@class="geetest_input"]', numeric_string)
            wait_random(0.5, 1)
            self.driver.find_element_by_xpath('//input[@class="geetest_input"]').send_keys(Keys.ENTER)
            try:
                WebDriverWait(self.driver, 20).until(
                    expected_conditions.invisibility_of_element_located((By.XPATH, '//div[@class="geetest_replay"]')))
            except Exception:
                log_warning('recapcha solving failed!')
                if retry_count >= 3:
                    return False
                self.driver.find_element_by_xpath('//div[@class="geetest_refresh"]').click()
                time.sleep(random.uniform(0.5, 1))
                log_info("clicked refresh audio button geetest_refresh")
                return solve_audio_recaptha_attempt(retry_count=retry_count + 1)
            return True

        if retry == 3:
            log_warning("solving recapcha failed after 3 attempts")
            return False

        time.sleep(random.uniform(2, 3))
        captcha_iframe = self.driver.find_element_by_xpath(
            '//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]')
        self.driver.switch_to.frame(captcha_iframe)
        log_info("switched to captcha_iframe")
        try:
            WebDriverWait(self.driver, 30).until(
                expected_conditions.presence_of_element_located((By.XPATH, '//div[@class="geetest_radar_btn"]')))
            time.sleep(random.uniform(1, 2))
            self.driver.find_element_by_xpath('//div[@class="geetest_radar_btn"]').click()
            log_info("clicked verify button geetest_radar_btn")
            WebDriverWait(self.driver, 15).until(
                expected_conditions.presence_of_element_located((By.XPATH, '//a[@class="geetest_voice"]')))
            time.sleep(random.uniform(1, 2))
            self.driver.find_element_by_xpath('//a[@class="geetest_voice"]').click()
            log_info("clicked voice verify button geetest_voice")
        except Exception as ex:
            log_exception(ex)
            self.driver.switch_to.default_content()
            self.driver.refresh()
            return self.solve_recaptha(retry + 1)
        anti_bot_solved = solve_audio_recaptha_attempt()
        self.driver.switch_to.default_content()
        if anti_bot_solved:
            return True
        return self.solve_recaptha(retry + 1)

    def open_url_and_crack_antibot(self, url):
        try:
            self.driver.get(url)
            log_info("opened url: {}".format(url))
            time.sleep(random.uniform(2, 3))
            if self.is_currently_blocked():
                log_info('blocked!')
                return False
            if self.is_detected_by_anti_bot():
                if self.solve_recaptha():
                    log_info('solve_recaptha done!')
                    return True
                else:
                    log_info('solve_recaptha failed!')
                    return False
            else:
                return True
        except Exception as ex:
            log_exception(ex)
            return False

    # [['United Arab Emirates', 'dh', 'en', 'https://www.hermes.com/dh/en/'], ['Suisse', 'ch', 'fr', 'https://www.hermes.com/ch/fr/'], ...]
    # def get_locale_info_list(self):
    #     log_info("get_locale_info_list started")
    #     # e.g <a href="https://www.hermes.com/de/de/" class="ripple-effect">Deutschland</a>
    #     open_success = self.open_url_and_crack_antibot(constants.HERMES_HOME_URL)
    #     results = []
    #     if open_success:
    #         try:
    #             WebDriverWait(self.driver, 5).until(expected_conditions.presence_of_element_located(
    #                 (By.XPATH, '//a[@class="ripple-effect"]')))
    #             for element in self.driver.find_elements_by_xpath('//a[@class="ripple-effect"]'):
    #                 href = str(element.get_attribute('href'))
    #                 if '.com' in href:
    #                     parts = href.split('.com')[-1].strip('/').split('/')
    #                     country_code = parts[0]
    #                     language_code = parts[-1]
    #                     country_name = element.get_attribute('text').encode('utf-8')
    #                     results.append(LocaleInfo(country_name, country_code, language_code, href))
    #         except Exception as ex:
    #             log_exception(ex)
    #             create_empty_file(self.data_dir_path, "FAIL_LOCALE")
    #             return
    #         log_info("results count = {}".format(len(results)))
    #     else:
    #         log_info("failed to open {}".format(constants.HERMES_HOME_URL))
    #         create_empty_file(self.data_dir_path, "FAIL_LOCALE")
    #         return
    #     locale_file_path = os.path.join(self.data_dir_path, 'locale.json')
    #     with open(locale_file_path, 'w+') as f:
    #         for r in results:
    #             json.dump(r, f)
    #             f.write('\n')
    #     log_info("Finished writing to {}".format(locale_file_path))
    #     create_empty_file(self.data_dir_path, "SUCCESS_LOCALE")
    #
    # def get_category(self):
    #
    #     def get_leveled_category(json_data):
    #         leveled_category_results = []
    #         cur = [json_data]
    #         level = 0
    #         while cur:
    #             level_result = []
    #             nxt = []
    #             for data in cur:
    #                 result = {'name': data['name'], 'code': data['pimCode'], 'path': data['path'],
    #                           'has_visible_products': data['has_visible_products'], 'level': level}
    #                 leveled_category_results.append(result)
    #                 level_result.append([])
    #                 if 'children' in data:
    #                     for child in data['children']:
    #                         nxt.append(child)
    #             cur = nxt
    #             level += 1
    #         return leveled_category_results
    #
    #     log_info("get_category started")
    #     url = constants.HERMES_CATEGORIES_API.format(self.locale_code)
    #     open_success = self.open_url_and_crack_antibot(url)
    #     if open_success:
    #         try:
    #             WebDriverWait(self.driver, 30).until(expected_conditions.presence_of_element_located(
    #                 (By.XPATH, '//html/body/pre')))
    #             response_json = json.loads(self.driver.find_element_by_xpath('//html/body/pre').text)
    #             results = get_leveled_category(response_json)
    #             log_info("results count = {}".format(len(results)))
    #             if not len(results):
    #                 create_empty_file(self.data_dir_path, "FAIL_CATEGORY")
    #                 return False
    #         except Exception as ex:
    #             log_exception(ex)
    #             create_empty_file(self.data_dir_path, "FAIL_CATEGORY")
    #             return False
    #     else:
    #         log_info("failed to open {}".format(url))
    #         create_empty_file(self.data_dir_path, "FAIL_CATEGORY")
    #         return False
    #     locale_file_path = os.path.join(self.data_dir_path, 'categories.json')
    #     with open(locale_file_path, 'w+') as f:
    #         for r in results:
    #             json.dump(r, f)
    #             f.write('\n')
    #     create_empty_file(self.data_dir_path, "SUCCESS_CATEGORY")
    #     return True

    def create_timestamped_data_dir(self, locale_code):
        delete_dir(os.path.join(os.getcwd(), 'temp', locale_code, 'scraper'))

        self.timestamp = get_current_pst_format_timestamp()
        self.temp_dir_path = os.path.join(os.getcwd(), 'temp', locale_code, 'scraper/{}'.format(self.timestamp))
        if not os.path.isdir(self.temp_dir_path):
            os.makedirs(self.temp_dir_path)
        self.product_dir_path = os.path.join(self.temp_dir_path, 'product')
        if not os.path.exists(self.product_dir_path):
            os.makedirs(self.product_dir_path)

    def get_timestamp(self):
        return self.timestamp

    def open_with_timeout(self, URL, timeout):
        time_start = datetime.now()
        self.driver.get(URL)
        time_spent = (datetime.now() - time_start).total_seconds()
        if time_spent > timeout:
            raise SlowIPException("{} loading too slow. time spent: {}".format(URL, time_spent))

    def get_product_info(self, locale_code):

        def get_product_info_from_category(category, retry=0):
            if retry == 2:
                log_info("get_product_info_from_category retry limit hit")
                return False
            log_info("get_product_info_from_category:{} retry:{}".format(category, retry))
            URL = constants.HERMES_PRODUCT_API.format(locale_code, category, constants.PRODUCT_PAGE_SIZE, 0)

            # workaround to simulate human behavior
            blocked = True
            attempt = 0
            while attempt < 3:
                attempt += 1
                log_info("get URL:{}".format(URL))
                self.driver.get(URL)
                wait_random(1, 2)
                if not self.is_currently_blocked():
                    blocked = False
                    break
                else:
                    self.driver.get('https://www.google.com/')
                    wait_random(3, 4)

            if blocked:
                log_warning("after {} attempts, still being BLOCKED!".format(attempt))
                create_empty_file(self.product_dir_path, "BLOCKED")
                return False

            if self.is_detected_by_anti_bot():
                if self.solve_recaptha():
                    log_info('solve_recaptha done!')
                    recaptha_solved = True
                else:
                    log_info('solve_recaptha failed!')
                    recaptha_solved = False
            else:
                recaptha_solved = True

            if not recaptha_solved:
                return get_product_info_from_category(category, retry + 1)

            try:
                wait_random(2, 3)
                WebDriverWait(self.driver, 10).until(lambda d: len(d.find_elements_by_xpath('//html/body/pre')) > 0)
            except Exception:
                log_info("failed to detect '//html/body/pre' with {}".format(URL))
                return get_product_info_from_category(category, retry + 1)
            response_json = json.loads(self.driver.find_element_by_xpath('//html/body/pre').text)
            try:
                total = response_json['total']
                log_info('total product count = {}'.format(total))
            except Exception:
                log_info("invalid response_json: {}".format(response_json))
                return get_product_info_from_category(category, retry + 1)

            reach_end_of_product_list = False
            results = []
            offset = 0
            while not reach_end_of_product_list:
                products = response_json['products']['items']
                log_info('current product list count = {}'.format(len(products)))
                for p in products:
                    results.append(p)
                offset += constants.PRODUCT_PAGE_SIZE
                URL = constants.HERMES_PRODUCT_API.format(locale_code, category,
                                                          constants.PRODUCT_PAGE_SIZE,
                                                          offset)
                self.driver.get(URL)
                try:
                    wait_random(2, 3)
                    WebDriverWait(self.driver, 10).until(lambda d: len(d.find_elements_by_xpath('//html/body/pre')) > 0)
                except Exception:
                    log_info("failed to load json response")
                    return get_product_info_from_category(category, retry + 1)
                try:
                    response_json = json.loads(self.driver.find_element_by_tag_name("pre").text)
                except Exception:
                    log_exception("load json failed: {}".format(URL))
                if not response_json or 'total' not in response_json:
                    log_exception("invalid json response: {}".format(response_json))
                    return get_product_info_from_category(category, retry + 1)
                if response_json['total'] == 0 or not response_json['products']['items']:
                    log_info('reached end of product list')
                    reach_end_of_product_list = True

            log_info('results count = {}'.format(len(results)))
            if len(results) != total:
                log_info("result count doesn't match, result count: {}, should be {}".format(len(results), total))
                return get_product_info_from_category(category, retry + 1)
            file_path = os.path.join(self.product_dir_path, category)
            with open(file_path, 'w+') as f:
                for r in results:
                    json.dump(r, f)
                    f.write('\n')
            log_info('results dump finished')
            return True

        log_info("Started scraping product info for {}...".format(locale_code))
        self.create_timestamped_data_dir(locale_code)
        for category_code in self.category_codes:
            if get_product_info_from_category(category_code):
                create_empty_file(self.product_dir_path, "SUCCESS_{}".format(category_code))
            else:
                create_empty_file(self.product_dir_path, "FAIL_{}".format(category_code))

    def scrape_result(self):
        results = {}
        if not self.timestamp:
            return "NOT_READY", results
        flag = "SUCCESS"
        for category_code in self.category_codes:
            category_success_file_path = os.path.join(self.product_dir_path, "SUCCESS_{}".format(category_code))
            if os.path.exists(category_success_file_path):
                results[category_code] = "SUCCESS"
            else:
                results[category_code] = "FAIL"
                flag = "FAIL"
        if self.has_been_blocked():
            flag = "BLOCKED"
        return flag, results

    def print_ip(self):
        try:
            self.driver.get('https://api.ipify.org/')
            log_info("checking ip")
            WebDriverWait(self.driver, 2).until(expected_conditions.presence_of_element_located(
                (By.XPATH, '//html/body/pre')))
            detected_ip = self.driver.find_element_by_xpath('//html/body/pre').text
            log_info('ip: {}'.format(detected_ip))
        except Exception:
            log_info('print_ip exception')
            raise SlowIPException("proxy slow")

    def terminate(self):
        self.driver.quit()

# port = 20001
# while port <= 20300:
# scraper = Scraper(on_proxy=True, headless=True)
# scraper.terminate()
#     port += 1

# def thread_function(port):
#     scraper = Scraper(port, on_proxy=True, headless=True)
#     scraper.terminate()
#
# t = threading.Thread(target=thread_function, args=(20001,))
# t.start()

# port = 20001
# threads = []
# while port <= 20100:
#     for p in range(port, port+20):
#         t = threading.Thread(target=thread_function, args=(p,))
#         t.start()
#         threads.append(t)
#     for t in threads:
#         t.join()
#     threads = []
#     port += 20
