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

import constants
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import speech_recognition as sr

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


def is_detected_by_anti_bot(driver):
    if driver.find_elements_by_xpath('//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]'):
        print("detected by anti-bot")
        return True
    return False

def is_blocked(driver):
    if driver.find_elements_by_xpath('//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]'):
        driver.switch_to.frame(driver.find_elements_by_xpath('//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]')[0])
        if driver.find_elements_by_xpath('//div[contains(text(), "You have been blocked.")]'):
            driver.switch_to.default_content()
            return True
        driver.switch_to.default_content()
        return False
    return False

def type_with_delay(driver, xpath, value):
    try:
        if driver.find_elements_by_xpath(xpath):
            driver.find_elements_by_xpath(xpath)[0].clear()
            for c in value:
                driver.find_elements_by_xpath(xpath)[0].send_keys(c)
                time.sleep(random() / 10)
        else:
            print("xpath not found: {}".format(xpath))
            return False
    except Exception:
        return False
    return True

def solve_recaptha(driver):
    def solve_audio_recaptha_attempt(driver):
        WebDriverWait(driver, 3).until(expected_conditions.presence_of_element_located(
            (By.XPATH, '//audio[@class="geetest_music"]')))
        time.sleep(2)
        audio_element = driver.find_element_by_xpath('//audio[@class="geetest_music"]')
        src = audio_element.get_attribute('src')
        local_mp3_path = os.path.join(os.getcwd(), 'temp', 'recaptha_audio.mp3')
        if not os.path.isdir(os.path.join(os.getcwd(), 'temp')):
            os.makedirs(os.path.join(os.getcwd(), 'temp'))
        urllib.request.urlretrieve(src, local_mp3_path)
        sound = pydub.AudioSegment.from_mp3(local_mp3_path)
        local_wav_path = os.path.join(os.getcwd(), 'temp', 'recaptha_audio.wav')
        sound.export(local_wav_path, format='wav')
        sample_audio = sr.AudioFile(local_wav_path)
        r = sr.Recognizer()
        with sample_audio as source:
            audio = r.record(source)
        key = r.recognize_google(audio)
        numeric_string = re.sub("[^0-9]", "", key)
        type_with_delay(driver, '//input[@class="geetest_input"]', numeric_string)
        driver.find_element_by_xpath('//input[@class="geetest_input"]').send_keys(Keys.ENTER)
        time.sleep(1)
        if driver.find_elements_by_xpath(
                '//div[@class="geetest_result_tip" and contains(test(), "Sorry, it doesn\'t match.")]'):
            return False
        else:
            return True

    iframe = driver.find_element_by_xpath('//iframe[contains(@src, "https://geo.captcha-delivery.com/captcha")]')
    driver.switch_to.frame(iframe)
    driver.find_element_by_xpath('//div[@class="geetest_radar_btn"]').click()
    while not solve_audio_recaptha_attempt(driver):
        driver.find_element_by_xpath('//div[@class="geetest_refresh"]').click()
    return True


def get_locale_info_list():  # [['United Arab Emirates', 'dh', 'en', 'https://www.hermes.com/dh/en/'], ['Suisse', 'ch', 'fr', 'https://www.hermes.com/ch/fr/'], ...]
    options = Options()
    # options.add_argument('--headless')
    # options.add_argument('--disable-gpu')  # Last I checked this was necessary.
    driver = webdriver.Chrome('/usr/local/bin/chromedriver', options=options)
    driver.get(constants.HERMES_HOME_URL)
    # e.g <a href="https://www.hermes.com/de/de/" class="ripple-effect">Deutschland</a>
    time.sleep(2)
    if is_blocked(driver):
        print('blocked!')
        driver.quit()
        return []
    if is_detected_by_anti_bot(driver):
        if solve_recaptha(driver):
            print('solve_recaptha done!')
        else:
            print('solve_recaptha failed!')
            driver.quit()
            return []
    results = []
    for element in driver.find_elements_by_xpath('//a[@class="ripple-effect"]'):
        try:
            href = str(element.get_attribute('href'))
            if '.com' in href:
                parts = href.split('.com')[-1].strip('/').split('/')
                country_code = parts[0]
                language_code = parts[-1]
                country_name = element.get_attribute('text').encode('utf-8')
                results.append(LocaleInfo(country_name, country_code, language_code, href))
        except Exception:
            print('Failed parsing element!')
    driver.quit()
    for r in results:
        print(r)
    return results


def get_category(locale_code='us_en'):
    def get_leveled_category(json_data):
        result = []
        cur = [json_data]
        level = 0
        while cur:
            level_result = []
            nxt = []
            for data in cur:
                name = data['name']
                code = data['pimCode']
                path = data['path']
                has_visible_products = data['has_visible_products']
                result.append(Category(name, code, path, has_visible_products, level))
                level_result.append([])
                if 'children' in data:
                    for child in data['children']:
                        nxt.append(child)
            cur = nxt
            level += 1
        return result

    options = Options()
    # options.add_argument('--headless')
    # options.add_argument('--disable-gpu')
    driver = webdriver.Chrome('/usr/local/bin/chromedriver', options=options)
    url = constants.HERMES_CATEGORIES_API.format(locale_code)
    driver.get(url)
    time.sleep(2)
    if is_blocked(driver):
        print('blocked!')
        driver.quit()
        return []
    if is_detected_by_anti_bot(driver):
        if solve_recaptha(driver):
            print('solve_recaptha done!')
        else:
            print('solve_recaptha failed!')
            driver.quit()
            return []
    try:
        WebDriverWait(driver, 5).until(expected_conditions.presence_of_element_located(
            (By.XPATH, '//pre[]')))
        time.sleep(2)
        response_json = json.loads(driver.find_element_by_tag_name("pre").text)
    except Exception:
        driver.quit()
        print("Exception raised during fetching JSON response")
        return []
    try:
        results = get_leveled_category(response_json)
    except Exception:
        driver.quit()
        print("Exception raised during parsing JSON response")
        return []
    driver.quit()
    for r in results:
        print(r)
    return results

# sound=pydub.AudioSegment.from_mp3('dev/601f94c51ef397465e10cce4ba687a3c.mp3')
# sound.export('dev/sound.wav', format='wav')
# sample_audio = sr.AudioFile('dev/sound.wav')
# r = sr.Recognizer()
# with sample_audio as source:
#     audio = r.record(source)
# key = r.recognize_google(audio)
# print(key)

get_locale_info_list()
get_category()

