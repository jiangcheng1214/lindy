import json
import os

import firebase_admin
import pyrebase
from firebase_admin import storage, credentials

from Utils import log_info, log_warning, supported_categories, log_exception, get_current_pst_format_date, \
    get_datetime_from_string, get_current_pst_format_year_month, get_current_pst_time

service_account_credentials = credentials.Certificate('credentials/firebase_service_account_credentials.json')
with open('credentials/firebase_credentials.json', 'r') as f:
    firebase_credentials = json.load(f)
firebase_admin.initialize_app(service_account_credentials, firebase_credentials)


class StorageFileTransformer:
    def __init__(self):
        self.bucket = storage.bucket()

    def upload(self, destination_path, from_path):
        try:
            log_info("Uploading {} -> {}".format(from_path, destination_path))
            blob = self.bucket.blob(destination_path)
            blob.upload_from_filename(from_path)
            log_info("Uploaded.")
        except Exception as s:
            log_exception(s)

    # def download(self, destination_path, from_path):
    #     try:
    #         log_info("Downloading {} -> {}".format(from_path, destination_path))
    #         blob = self.bucket.blob(from_path)
    #         blob.download_to_filename(destination_path)
    #         log_info("Downloaded.")
    #     except Exception as s:
    #         log_exception(s)


class DeltaChecker:
    def __init__(self):
        with open('credentials/firebase_credentials.json', 'r') as f:
            credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(credentials)
        self.storage = self.firebase.storage()
        self.database = self.firebase.database()
        self.fileTransformer = StorageFileTransformer()

    def temp_dir_path(self, locale_code):
        path = os.path.join(os.getcwd(), 'temp', locale_code, 'delta')
        if not os.path.isdir(path):
            os.makedirs(path)
        return path

    def scraped_data_dir_path(self, locale_code):
        return os.path.join(os.getcwd(), 'temp', locale_code, 'scraper')

    def forward_data_dir_path(self, locale_code):
        return os.path.join(os.getcwd(), 'temp', locale_code, 'forward')

    def get_timestamp_scraped_forward(self, locale_code):
        return self.database.child('{}/timestamp_scraped_forward'.format(locale_code)).get().val()

    def update_timestamp_scraped_forward(self, timestamp, locale_code):
        self.database.child('{}/timestamp_scraped_forward'.format(locale_code)).set(timestamp)

    def download(self, cloud_path, local_path):
        log_info("download. cloud_path: {} local_path: {}".format(cloud_path, local_path))
        try:
            self.storage.child(cloud_path).download(filename=local_path)
            return
        except Exception:
            log_info("self.storage.child(cloud_path).download(filename=local_path) exception")
        try:
            self.storage.child(cloud_path).download(path=local_path, filename=local_path)
            return
        except Exception:
            log_info("self.storage.child(cloud_path).download(path=local_path, filename=local_path) exception")

    def download_data_for_delta_check(self, category_code, timestamp_base, test_timestamp, locale_code):
        log_info("download_data_for_delta_check: timestamp_base: {} test_timestamp: {}".format(timestamp_base, test_timestamp))
        path_to_local_dir = "{}/{}_to_{}".format(self.temp_dir_path(locale_code), timestamp_base, test_timestamp)
        if not os.path.isdir(path_to_local_dir):
            os.makedirs(path_to_local_dir)
        path_to_base_data_on_cloud = "{}/products/{}/{}".format(locale_code, timestamp_base, category_code)
        path_to_base_data = "{}/{}_{}".format(path_to_local_dir, timestamp_base, category_code)
        self.download(path_to_base_data_on_cloud, path_to_base_data)
        if not os.path.isfile(path_to_base_data):
            return None
        path_to_test_data_on_cloud = "{}/products/{}/{}".format(locale_code, test_timestamp, category_code)
        path_to_test_data = "{}/{}_{}".format(path_to_local_dir, test_timestamp, category_code)
        self.download(path_to_test_data_on_cloud, path_to_test_data)
        if not os.path.isfile(path_to_test_data):
            return None
        return path_to_local_dir

    def get_delta_info(self, category, timestamp_base, timestamp_forward, locale_code):
        def is_same(item1, item2):
            check_fields = ['price', 'stock']
            for field in check_fields:
                if item1[field] != item2[field]:
                    return False
            return True

        log_info("Start getting delta info. timestamp_base: {} timestamp_forward: {}".format(timestamp_base, timestamp_forward))
        data_dir_path = self.download_data_for_delta_check(category, timestamp_base, timestamp_forward, locale_code)
        if not data_dir_path:
            log_info("Failed to download! {}".format(data_dir_path))
            return None
        all_base_data = {}
        all_test_data = {}
        all_ids = set()
        base_data_path = os.path.join(data_dir_path, "{}_{}".format(timestamp_base, category))
        with open(base_data_path, 'r') as base_file:
            for l in base_file.readlines():
                json_data = json.loads(l)
                all_base_data[json_data['sku']] = json_data
                all_ids.add(json_data['sku'])

        test_data_path = os.path.join(data_dir_path, "{}_{}".format(timestamp_forward, category))
        with open(test_data_path, 'r') as forward_file:
            for l in forward_file.readlines():
                json_data = json.loads(l)
                all_test_data[json_data['sku']] = json_data
                all_ids.add(json_data['sku'])
        log_info("base items count: {}".format(len(all_base_data)))
        log_info("test items count: {}".format(len(all_test_data)))
        added_items = []
        removed_items = []
        updated_items = []
        for id in all_ids:
            if id in all_base_data and id not in all_test_data:
                removed_items.append(all_base_data[id])
                continue
            if id not in all_base_data and id in all_test_data:
                added_items.append(all_test_data[id])
                continue
            assert id in all_base_data and id in all_test_data
            if not is_same(all_base_data[id], all_test_data[id]):
                updated_items.append(all_base_data[id])
                updated_items.append(all_test_data[id])
        log_info("added items count: {}".format(len(added_items)))
        log_info("removed items count: {}".format(len(removed_items)))
        log_info("updated items count: {}".format(len(updated_items)))
        return {"ADDED": added_items, "REMOVED": removed_items, "UPDATED": updated_items}

    def update_realtime_delta_if_necessary(self, locale_code, timestamp_forward=None, timestamp_base=None):
        def get_realtime_delta_timestamp_base():
            return self.database.child('{}/delta_realtime/timestamp_base'.format(locale_code)).get().val()

        if not timestamp_forward:
            timestamp_forward = self.get_timestamp_scraped_forward(locale_code)
        if not timestamp_base:
            timestamp_base = get_realtime_delta_timestamp_base()
        log_info("delta realtime check {} ({} -> {})".format(locale_code, timestamp_base, timestamp_forward))
        if not timestamp_base:
            self.database.child('{}/delta_realtime/timestamp_base'.format(locale_code)).set(timestamp_forward)
            return "INIT"
        if timestamp_base == timestamp_forward:
            log_info("skip delta check (timestamp_base equals to timestamp_forward)")
            return "SKIP"
        assert timestamp_base < timestamp_forward

        update_stamp = "{}_to_{}".format(timestamp_base, timestamp_forward)
        delta_db_path = "{}/delta_realtime/{}/{}".format(locale_code, timestamp_base[:6], update_stamp)
        check_delta_result = "SKIP"
        delta_realtime_uploaded = False
        total_delta_info = {}
        for category in supported_categories():
            delta_info = self.get_delta_info(category, timestamp_base, timestamp_forward, locale_code)
            total_delta_info[category] = delta_info
            if not delta_info:
                log_info("no delta info found")
                continue
            if len(delta_info["ADDED"]) + len(delta_info["REMOVED"]) + len(delta_info["UPDATED"]) == 0:
                log_info("no added/removed/updated items detected from {} to {} for {}".format(timestamp_base,
                                                                                               timestamp_forward,
                                                                                               category))
                continue
            # TODO: make time_added and time_removed a list
            for type in delta_info:
                for item_json in delta_info[type]:
                    sku = item_json['sku']
                    self.database.child(delta_db_path).child(category).child(type).child(sku).set(item_json)
                    if type == 'ADDED':
                        self.database.child('{}/product_updates'.format(locale_code)).child(category).child(type).child(sku).set(item_json)
                        self.database.child('{}/product_updates'.format(locale_code)).child(category).child(type).child(sku).child(
                            'time_added').set(timestamp_forward)
                        if self.database.child('{}/product_updates'.format(locale_code)).child(category).child("REMOVED").child(sku).get().val():
                            self.database.child('{}/product_updates'.format(locale_code)).child(category).child(type).child(sku).child(
                                'is_restock').set(True)
                            self.database.child('{}/product_updates'.format(locale_code)).child(category).child("REMOVED").child(
                                sku).remove()
                        else:
                            self.database.child('{}/product_updates'.format(locale_code)).child(category).child(type).child(sku).child(
                                'is_restock').set(False)
                    if type == 'REMOVED':
                        if self.database.child('{}/product_updates'.format(locale_code)).child(category).child("ADDED").child(sku).get().val():
                            item_detail = self.database.child('{}/product_updates'.format(locale_code)).child(
                                category).child("ADDED").child(sku).get().val()
                            time_added = get_datetime_from_string(self.database.child('{}/product_updates'.format(locale_code)).child(
                                category).child("ADDED").child(sku).child('time_added').get().val())
                            time_removed = get_datetime_from_string(timestamp_forward)
                            time_available_hours = (time_removed - time_added).total_seconds() / 3600
                            item_detail['time_removed'] = timestamp_forward
                            item_detail['time_available_hours'] = time_available_hours
                            self.database.child('{}/product_updates'.format(locale_code)).child(category).child(type).child(sku).set(
                                item_detail)
                            self.database.child('{}/product_updates'.format(locale_code)).child(category).child("ADDED").child(
                                sku).remove()
            check_delta_result = "SUCCESS"
            delta_realtime_uploaded = True

        if delta_realtime_uploaded:
            log_info("updating realtime delta: {}".format(total_delta_info))
            self.database.child(delta_db_path).child("timestamp_base").set(timestamp_base)
            self.database.child(delta_db_path).child("timestamp_forward").set(timestamp_forward)
            self.database.child('{}/delta_realtime/last_update'.format(locale_code)).set(update_stamp)
            self.database.child('{}/delta_realtime/timestamp_base'.format(locale_code)).set(timestamp_forward)
        return check_delta_result

    def update_daily_delta_if_necessary(self, locale_code, timestamp_forward=None):

        def get_daily_delta_timestamp_base():
            return self.database.child('{}/delta_daily/timestamp_base'.format(locale_code)).get().val()

        def should_update(timestamp_base, timestamp_forward):
            hours_since_last_update = (get_datetime_from_string(timestamp_forward) - get_datetime_from_string(
                timestamp_base)).total_seconds() / 3600
            current_pst_hour = get_current_pst_time().hour
            in_time_window = current_pst_hour >= 16
            log_info("should_update? current_pst_hour: {} hours_since_last_update: {}".format(current_pst_hour, hours_since_last_update))
            if in_time_window and hours_since_last_update >= 16:
                return True
            else:
                return False

        if not timestamp_forward:
            timestamp_forward = self.get_timestamp_scraped_forward(locale_code)
        timestamp_base = get_daily_delta_timestamp_base()
        if not timestamp_base:
            self.database.child('{}/delta_daily/timestamp_base'.format(locale_code)).set(timestamp_forward)
            return "INIT"
        if not should_update(timestamp_base, timestamp_forward):
            log_info("No need to daily update")
            return "SKIP"

        log_info("delta daily check ({} -> {})".format(timestamp_base, timestamp_forward))
        date_today = get_current_pst_format_date()
        daily_delta_db_path = "{}/delta_daily/{}/{}".format(locale_code, get_current_pst_format_year_month(), date_today)
        if self.database.child(daily_delta_db_path).get().val():
            log_warning("daily delta already existed: {}".format(daily_delta_db_path))
            return "SKIP"
        log_info("checking delta timestamp_base: {} timestamp_forward: {}".format(timestamp_base, timestamp_forward))
        if timestamp_base == timestamp_forward:
            log_info("skip delta check (timestamp_base equals to timestamp_forward)")
            return "SKIP"
        assert timestamp_base < timestamp_forward
        check_delta_results = {}
        for category in supported_categories():
            delta_info = self.get_delta_info(category, timestamp_base, timestamp_forward, locale_code)
            if not delta_info:
                log_info("no delta info found")
                check_delta_results[category] = "SKIP"
                continue
            if len(delta_info["ADDED"]) + len(delta_info["REMOVED"]) + len(delta_info["UPDATED"]) == 0:
                log_info("no added/removed/updated items detected from {} to {} for {}".format(
                    timestamp_base, timestamp_forward, category))
                check_delta_results[category] = "SKIP"
                continue

            for type in delta_info:
                for item_json in delta_info[type]:
                    sku = item_json['sku']
                    self.database.child(daily_delta_db_path).child(category).child(type).child(sku).set(item_json)
            check_delta_results[category] = "SUCCESS"

        self.database.child('{}/delta_daily/timestamp_base'.format(locale_code)).set(timestamp_forward)
        self.database.child(daily_delta_db_path).child("timestamp_base").set(timestamp_base)
        self.database.child(daily_delta_db_path).child("timestamp_forward").set(timestamp_forward)
        return "SUCCESS"

    def is_scrape_success(self, timestamp, locale_code):
        for category_code in supported_categories():
            flag_path = os.path.join(self.scraped_data_dir_path(locale_code), timestamp, "product", "SUCCESS_" + category_code)
            if not os.path.exists(flag_path):
                log_warning(flag_path + " doesn't exist!")
                return False
        return True

    def upload_products_if_necessary(self, timestamp, locale_code):

        def is_identical(forward_data_path, test_data_path):

            def is_same_item(item1, item2):
                check_fields = ['price', 'stock']
                for field in check_fields:
                    if item1[field] != item2[field]:
                        return False
                return True

            log_info("Checking identification {} - {}".format(forward_data_path, test_data_path))
            try:
                all_forward_data = {}
                all_test_data = {}
                all_ids = set()
                with open(forward_data_path, 'r') as forward_file:
                    for l in forward_file.readlines():
                        json_data = json.loads(l)
                        all_forward_data[json_data['sku']] = json_data
                        all_ids.add(json_data['sku'])

                with open(test_data_path, 'r') as test_file:
                    for l in test_file.readlines():
                        json_data = json.loads(l)
                        all_test_data[json_data['sku']] = json_data
                        all_ids.add(json_data['sku'])
                for id in all_ids:
                    if id in all_forward_data and id not in all_test_data:
                        return False
                    if id not in all_forward_data and id in all_test_data:
                        return False
                    assert id in all_forward_data and id in all_test_data
                    if not is_same_item(all_forward_data[id], all_test_data[id]):
                        return False
                return True
            except Exception:
                log_exception("Failed to check identical of two files")
                return True

        if not self.is_scrape_success(timestamp, locale_code):
            log_warning("Not a success scrape with timestamp " + timestamp)
            return "INVALID_DATA_FAIL"
        timestamp_scraped_forward = self.get_timestamp_scraped_forward(locale_code)
        if not timestamp_scraped_forward:
            timestamp_scraped_forward = timestamp
        if timestamp < timestamp_scraped_forward:
            log_warning("skip delta check (invalid timestamp: {} < {})".format(timestamp, timestamp_scraped_forward))
            return "SKIP"
        forward_data_dir_path = os.path.join(self.forward_data_dir_path(locale_code), timestamp_scraped_forward)
        if not os.path.isdir(forward_data_dir_path):
            os.makedirs(forward_data_dir_path)
        test_data_dir_path = os.path.join(self.scraped_data_dir_path(locale_code), timestamp, "product")
        should_upload = False
        try:
            for category_code in supported_categories():
                local_forward_file_path = os.path.join(forward_data_dir_path, category_code)
                local_test_file_path = os.path.join(test_data_dir_path, category_code)
                if not os.path.exists(local_forward_file_path):
                    cloud_forward_file_path = "{}/products/{}/{}".format(locale_code, timestamp_scraped_forward, category_code)
                    self.download(cloud_forward_file_path, local_forward_file_path)
                    if not os.path.exists(local_forward_file_path):
                        log_exception("Downloading {} failed! Upload scraped data with no diff check".format(cloud_forward_file_path))
                        should_upload = True
                        break
                if not is_identical(local_forward_file_path, local_test_file_path):
                    should_upload = True
                    break
        except Exception:
            log_exception("Exception during checking should upload or not")
            return "CHECK_DIFF_FAIL"
        if not should_upload:
            log_info("Don't need to upload {}, forward: {}".format(timestamp, timestamp_scraped_forward))
            return "SKIP"
        try:
            for category_code in supported_categories():
                local_test_file_path = os.path.join(test_data_dir_path, category_code)
                cloud_local_test_file_path = '{}/products/{}/{}'.format(locale_code, timestamp, category_code)
                self.fileTransformer.upload(cloud_local_test_file_path, local_test_file_path)
                log_info("{} has been uploaded to {}".format(local_test_file_path, cloud_local_test_file_path))
                # workaround to make sure file is uploaded (pyrebase uploads folder accidentally instead)
                self.storage.child(cloud_local_test_file_path).put(local_test_file_path)
        except Exception as e:
            log_exception("Exception during uploading {}".format(e))
            return "UPLOAD_FAIL"
        self.update_timestamp_scraped_forward(timestamp, locale_code)
        return "SUCCESS"


# deltaChecker = DeltaChecker()
# print(deltaChecker.update_daily_delta_if_necessary("us_en"))
# deltaChecker.update_realtime_delta("cn_zh", "20210611_17_12_58", "20210611_16_05_14")
# deltaChecker.upload_products_if_necessary("20210608_01_28_39", "us_en")
# i = 0
# while i < len(ts_list) - 1:
#     deltaChecker.update_realtime_delta(timestamp_base=ts_list[i+1], timestamp_forward=ts_list[i], should_update_timestamp=False)
#     i += 1
#
# deltaChecker.update_daily_delta()
# deltaChecker = DeltaChecker()
# deltaChecker.update_realtime_delta_if_necessary('us_en', '20210615_09_39_18','20210615_09_09_17')
# deltaChecker.upload_products_if_necessary("20210521_23_41_00")
# deltaChecker.update_realtime_delta("20210521_23_41_00")
# deltaChecker.update_daily_delta_if_necessary("20210525_16_49_14")
# deltaChecker.get_all_scraped_timestamps()
