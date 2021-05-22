import json
import os

import pyrebase

from Utils import log_info, log_warning, supported_categories, log_exception, get_current_pst_format_date, \
    get_datetime_from_string, get_current_pst_format_year_month, get_current_pst_time


class DeltaChecker:
    def __init__(self):
        self.temp_dir_path = os.path.join(os.getcwd(), 'temp', 'delta')
        if not os.path.isdir(self.temp_dir_path):
            os.makedirs(self.temp_dir_path)

        with open('credentials/firebase_credentials.json', 'r') as f:
            credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(credentials)
        self.storage = self.firebase.storage()
        self.database = self.firebase.database()
        self.scraped_data_dir_path = os.path.join(os.getcwd(), 'temp', 'scraper')
        self.forward_data_dir_path = os.path.join(os.getcwd(), 'temp', 'forward')

    def get_timestamp_scraped_forward(self):
        return self.database.child('timestamp_scraped_forward').get().val()

    def update_timestamp_scraped_forward(self, timestamp):
        self.database.child('timestamp_scraped_forward').set(timestamp)

    def download(self, cloud_path, local_path):
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

    def download_data_for_delta_check(self, category_code, timestamp_base, test_timestamp):
        path_to_local_dir = "{}/{}_to_{}".format(self.temp_dir_path, timestamp_base, test_timestamp)
        if not os.path.isdir(path_to_local_dir):
            os.makedirs(path_to_local_dir)
        path_to_base_data_on_cloud = "products/{}/{}".format(timestamp_base, category_code)
        path_to_base_data = "{}/{}_{}".format(path_to_local_dir, timestamp_base, category_code)
        self.download(path_to_base_data_on_cloud, path_to_base_data)
        if not os.path.isfile(path_to_base_data):
            return None
        path_to_test_data_on_cloud = "products/{}/{}".format(test_timestamp, category_code)
        path_to_test_data = "{}/{}_{}".format(path_to_local_dir, test_timestamp, category_code)
        self.download(path_to_test_data_on_cloud, path_to_test_data)
        if not os.path.isfile(path_to_test_data):
            return None
        return path_to_local_dir

    def get_delta_info(self, category, timestamp_base, timestamp_forward):
        def is_same(item1, item2):
            check_fields = ['price', 'stock']
            for field in check_fields:
                if item1[field] != item2[field]:
                    return False
            return True

        data_dir_path = self.download_data_for_delta_check(category, timestamp_base, timestamp_forward)
        if not data_dir_path:
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

    def update_realtime_delta(self, timestamp_forward, timestamp_base=None):
        def get_realtime_delta_timestamp_base():
            return self.database.child('delta_realtime/timestamp_base').get().val()

        if not timestamp_base:
            timestamp_base = get_realtime_delta_timestamp_base()
        log_info("delta realtime check ({} -> {})".format(timestamp_base, timestamp_forward))
        if timestamp_base == timestamp_forward:
            log_info("skip delta check (timestamp_base equals to timestamp_forward)")
            return "SKIP"
        assert timestamp_base < timestamp_forward

        delta_db_path = "delta_realtime/{}/{}_to_{}".format(get_current_pst_format_year_month(), timestamp_base, timestamp_forward)
        check_delta_results = {}
        delta_realtime_uploaded = False
        for category in supported_categories():
            delta_info = self.get_delta_info(category, timestamp_base, timestamp_forward)
            if not delta_info:
                log_info("no delta info found")
                check_delta_results[category] = "SKIP"
                continue
            if len(delta_info["ADDED"]) + len(delta_info["REMOVED"]) + len(delta_info["UPDATED"]) == 0:
                log_info("no added/removed/updated items detected from {} to {} for {}".format(timestamp_base,
                                                                                               timestamp_forward,
                                                                                               category))
                check_delta_results[category] = "SKIP"
                continue

            for type in delta_info:
                for item_json in delta_info[type]:
                    sku = item_json['sku']
                    self.database.child(delta_db_path).child(category).child(type).child(sku).set(item_json)
                    if type == 'ADDED':
                        self.database.child('product_updates').child(category).child(type).child(sku).set(item_json)
                        self.database.child('product_updates').child(category).child(type).child(sku).child(
                            'time_added').set(timestamp_forward)
                        if self.database.child('product_updates').child(category).child("REMOVED").child(sku).get().val():
                            self.database.child('product_updates').child(category).child(type).child(sku).child(
                                'is_restock').set(True)
                            self.database.child('product_updates').child(category).child("REMOVED").child(
                                sku).remove()
                        else:
                            self.database.child('product_updates').child(category).child(type).child(sku).child(
                                'is_restock').set(False)
                    if type == 'REMOVED':
                        if self.database.child('product_updates').child(category).child("ADDED").child(sku).get().val():
                            item_detail = self.database.child('product_updates').child(
                                category).child("ADDED").child(sku).get().val()
                            time_added = get_datetime_from_string(self.database.child('product_updates').child(
                                category).child("ADDED").child(sku).child('time_added').get().val())
                            time_removed = get_datetime_from_string(timestamp_forward)
                            time_available_hours = (time_removed - time_added).total_seconds() / 3600
                            item_detail['time_removed'] = timestamp_forward
                            item_detail['time_available_hours'] = time_available_hours
                            self.database.child('product_updates').child(category).child(type).child(sku).set(
                                item_detail)
                            self.database.child('product_updates').child(category).child("REMOVED").child(
                                sku).remove()
            check_delta_results[category] = "SUCCESS"
            delta_realtime_uploaded = True

        if delta_realtime_uploaded:
            self.database.child(delta_db_path).child("timestamp_base").set(timestamp_base)
            self.database.child(delta_db_path).child("timestamp_forward").set(timestamp_forward)
            self.database.child('delta_realtime/timestamp_base').set(timestamp_forward)
        return check_delta_results

    def update_daily_delta_if_necessary(self, timestamp_forward=None):

        def get_daily_delta_timestamp_base():
            return self.database.child('delta_daily/timestamp_base').get().val()

        def get_timestamp_scraped_forward():
            return self.database.child('timestamp_scraped_forward').get().val()

        def should_update(timestamp_base, timestamp_forward):
            hours_since_last_update = (get_datetime_from_string(timestamp_forward) - get_datetime_from_string(
                timestamp_base)).total_seconds() / 3600
            # daily digest between 4pm and 6pm pst TODO: check historical data for optimal value
            current_pst_hour = get_current_pst_time().hour
            in_time_window = current_pst_hour in [16, 17]
            if in_time_window and hours_since_last_update > 22:
                return True
            else:
                return False

        if not timestamp_forward:
            timestamp_forward = get_timestamp_scraped_forward()
        timestamp_base = get_daily_delta_timestamp_base()
        if not should_update(timestamp_base, timestamp_forward):
            log_info("No need to daily update")
            return "SKIP"

        log_info("delta daily check ({} -> {})".format(timestamp_base, timestamp_forward))
        date_today = get_current_pst_format_date()
        daily_delta_db_path = "delta_daily/{}/{}".format(get_current_pst_format_year_month(), date_today)
        if self.database.child(daily_delta_db_path).get().val():
            log_warning("daily delta already existed: {}".format(daily_delta_db_path))
            return "DAILY_DELTA_EXISTED"
        log_info("checking delta timestamp_base: {} timestamp_forward: {}".format(timestamp_base, timestamp_forward))
        if timestamp_base == timestamp_forward:
            log_info("skip delta check (timestamp_base equals to timestamp_forward)")
            return "SKIP"
        assert timestamp_base < timestamp_forward
        check_delta_results = {}
        for category in supported_categories():
            delta_info = self.get_delta_info(category, timestamp_base, timestamp_forward)
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

        self.database.child('delta_daily/timestamp_base').set(timestamp_forward)
        self.database.child(daily_delta_db_path).child("timestamp_base").set(timestamp_base)
        self.database.child(daily_delta_db_path).child("timestamp_forward").set(timestamp_forward)
        return check_delta_results

    def is_scrape_success(self, timestamp):
        for category_code in supported_categories():
            flag_path = os.path.join(self.scraped_data_dir_path, timestamp, "product", "SUCCESS_" + category_code)
            if not os.path.exists(flag_path):
                log_warning(flag_path + " doesn't exist!")
                return False
        return True

    def upload_products_if_necessary(self, timestamp):

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

        if not self.is_scrape_success(timestamp):
            log_warning("Not a success scrape with timestamp " + timestamp)
            return "INVALID_DATA_FAIL"
        timestamp_scraped_forward = self.get_timestamp_scraped_forward()
        if timestamp <= timestamp_scraped_forward:
            log_warning("skip delta check (invalid timestamp: {} <= {})".format(timestamp, timestamp_scraped_forward))
            return "SKIP"
        forward_data_dir_path = os.path.join(self.forward_data_dir_path, timestamp_scraped_forward)
        if not os.path.isdir(forward_data_dir_path):
            os.makedirs(forward_data_dir_path)
        test_data_dir_path = os.path.join(self.scraped_data_dir_path, timestamp, "product")
        should_upload = False
        try:
            for category_code in supported_categories():
                local_forward_file_path = os.path.join(forward_data_dir_path, category_code)
                local_test_file_path = os.path.join(test_data_dir_path, category_code)
                if not os.path.exists(local_forward_file_path):
                    cloud_forward_file_path = "products/{}/{}".format(timestamp_scraped_forward, category_code)
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
                cloud_local_test_file_path = 'products/{}/{}'.format(timestamp, category_code)
                self.storage.child(cloud_local_test_file_path).put(local_test_file_path)
                log_info("{} has been uploaded to {}".format(local_test_file_path, cloud_local_test_file_path))
        except Exception:
            log_exception(
                "Exception during uploading {} to {}".format(local_test_file_path, cloud_local_test_file_path))
            return "UPLOAD_FAIL"
        self.update_timestamp_scraped_forward(timestamp)
        return "SUCCESS"


'''Daily Delta'''
# ts_list = [
# "20210515_22_57_48",
# "20210515_23_12_48",
# "20210515_23_17_48",
# "20210515_23_27_48",
# "20210516_04_07_52",
# "20210516_05_27_54",
# "20210516_10_47_59",
# "20210516_11_27_59",
# "20210516_13_03_01",
# "20210516_17_23_04",
# "20210516_20_13_07",
# "20210516_21_08_08",
# ]
#
# deltaChecker = DeltaChecker()
# i = 0
# while i < len(ts_list) - 1:
#     deltaChecker.update_realtime_delta(timestamp_base=ts_list[i+1], timestamp_forward=ts_list[i], should_update_timestamp=False)
#     i += 1
#
# deltaChecker.update_daily_delta()
# deltaChecker = DeltaChecker()
# deltaChecker.upload_products_if_necessary("20210521_23_41_00")
# deltaChecker.update_realtime_delta("20210521_23_41_00")
# deltaChecker.update_daily_delta_if_necessary()
