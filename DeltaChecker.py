import json
import os

import pyrebase

from Utils import log_info, log_warning, supported_categories, log_exception


class DeltaChecker:
    def __init__(self):
        self.category_codes = supported_categories()
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

    def get_timestamp_base(self):
        return self.database.child('timestamp_base').get().val()

    def update_timestamp_base(self, timestamp):
        self.database.child('timestamp_base').set(timestamp)

    def get_timestamp_forward(self):
        return self.database.child('timestamp_forward').get().val()

    def update_timestamp_forward(self, timestamp):
        self.database.child('timestamp_forward').set(timestamp)

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

    def download_data_for_delta_check(self, timestamp_base, test_timestamp):
        path_to_local_dir = "{}/{}_to_{}".format(self.temp_dir_path, timestamp_base, test_timestamp)
        if not os.path.isdir(path_to_local_dir):
            os.makedirs(path_to_local_dir)
        for category_code in self.category_codes:
            path_to_base_data_on_cloud = "products/{}/{}".format(timestamp_base, category_code)
            path_to_base_data = "{}/{}_{}".format(path_to_local_dir, timestamp_base, category_code)
            self.download(path_to_base_data_on_cloud, path_to_base_data)
            assert os.path.isfile(path_to_base_data)
            path_to_test_data_on_cloud = "products/{}/{}".format(test_timestamp, category_code)
            path_to_test_data = "{}/{}_{}".format(path_to_local_dir, test_timestamp, category_code)
            self.download(path_to_test_data_on_cloud, path_to_test_data)
            assert os.path.isfile(path_to_test_data)
        return path_to_local_dir

    def check_delta_and_update_cloud(self):
        def dump_jsons_to_file(list_of_jsons, file_path):
            with open(file_path, 'w+') as f:
                for data in list_of_jsons:
                    json.dump(data, f)
                    f.write('\n')

        def is_same(item1, item2):
            check_fields = ['price', 'stock']
            for field in check_fields:
                if item1[field] != item2[field]:
                    return False
            return True

        timestamp_base = self.get_timestamp_base()
        timestamp_forward = self.get_timestamp_forward()
        log_info("checking delta timestamp_base: {} timestamp_forward: {}".format(timestamp_base, timestamp_forward))
        if timestamp_base == timestamp_forward:
            log_info("skip delta check (timestamp_base equals to timestamp_forward)")
            return "SKIP"
        assert timestamp_base < timestamp_forward
        data_dir_path = self.download_data_for_delta_check(timestamp_base, timestamp_forward)
        if not data_dir_path:
            log_warning("skip delta check (data download failed)")
            return "DOWNLOAD_FAIL"
        all_base_data = {}
        all_test_data = {}
        all_ids = set()
        for category_code in self.category_codes:
            base_data_path = os.path.join(data_dir_path, "{}_{}".format(timestamp_base, category_code))
            with open(base_data_path, 'r') as base_file:
                for l in base_file.readlines():
                    json_data = json.loads(l)
                    all_base_data[json_data['sku']] = json_data
                    all_ids.add(json_data['sku'])

            test_data_path = os.path.join(data_dir_path, "{}_{}".format(timestamp_forward, category_code))
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

        if len(added_items) + len(removed_items) + len(updated_items) == 0:
            log_info("no added/removed/updated items detected from {} to {}".format(timestamp_base, timestamp_forward))
            return "SKIP"

        path_to_delta_dir_on_cloud = "delta/{}_to_{}/".format(timestamp_base, timestamp_forward)
        try:
            if added_items:
                path_to_delta_added_data = os.path.join(data_dir_path, "ADDED")
                dump_jsons_to_file(added_items, path_to_delta_added_data)
                path_to_delta_added_data_on_cloud = path_to_delta_dir_on_cloud + "ADDED"
                self.storage.child(path_to_delta_added_data_on_cloud).put(path_to_delta_added_data)
                log_info("added items record uploaded to {}".format(path_to_delta_added_data_on_cloud))

            if removed_items:
                path_to_delta_removed_data = os.path.join(data_dir_path, "REMOVED")
                dump_jsons_to_file(removed_items, path_to_delta_removed_data)
                path_to_delta_removed_data_on_cloud = path_to_delta_dir_on_cloud + "REMOVED"
                self.storage.child(path_to_delta_removed_data_on_cloud).put(path_to_delta_removed_data)
                log_info("removed items record uploaded to {}".format(path_to_delta_removed_data_on_cloud))
            else:
                log_info("no removed items detected")

            if updated_items:
                path_to_delta_updated_data = os.path.join(data_dir_path, "UPDATED")
                dump_jsons_to_file(updated_items, path_to_delta_updated_data)
                path_to_delta_updated_data_on_cloud = path_to_delta_dir_on_cloud + "UPDATED"
                self.storage.child(path_to_delta_updated_data_on_cloud).put(path_to_delta_updated_data)
                log_info("updated items record uploaded to {}".format(path_to_delta_updated_data_on_cloud))
            else:
                log_info("no updated items detected")
        except Exception:
            log_exception("Exception during uploading")
            return "UPLOAD_FAIL"

        # update timestamp_base after uploading delta record
        self.update_timestamp_base(timestamp_forward)
        return "SUCCESS"

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
        timestamp_forward = self.get_timestamp_forward()
        if timestamp <= timestamp_forward:
            log_warning("skip delta check (invalid timestamp: {} <= {})".format(timestamp, timestamp_forward))
            return "SKIP"
        forward_data_dir_path = os.path.join(self.forward_data_dir_path, timestamp_forward)
        if not os.path.isdir(forward_data_dir_path):
            os.makedirs(forward_data_dir_path)
        test_data_dir_path = os.path.join(self.scraped_data_dir_path, timestamp, "product")
        should_upload = False
        try:
            for category_code in supported_categories():
                local_forward_file_path = os.path.join(forward_data_dir_path, category_code)
                local_test_file_path = os.path.join(test_data_dir_path, category_code)
                if not os.path.exists(local_forward_file_path):
                    cloud_forward_file_path = "products/{}/{}".format(timestamp_forward, category_code)
                    self.download(cloud_forward_file_path, local_forward_file_path)
                    assert os.path.exists(local_forward_file_path)
                if not is_identical(local_forward_file_path, local_test_file_path):
                    should_upload = True
                    break
        except Exception:
            log_exception("Exception during checking should upload or not")
            return "CHECK_DIFF_FAIL"
        if not should_upload:
            log_info("Don't need to upload {}, forward: {}".format(timestamp,timestamp_forward))
            return "SKIP"
        try:
            for category_code in supported_categories():
                local_test_file_path = os.path.join(test_data_dir_path, category_code)
                cloud_local_test_file_path = 'products/{}/{}'.format(timestamp, category_code)
                self.storage.child(cloud_local_test_file_path).put(local_test_file_path)
                log_info("{} has been uploaded to {}".format(local_test_file_path, cloud_local_test_file_path))
                self.update_timestamp_forward(timestamp)
        except Exception:
            log_exception("Exception during uploading {} to {}".format(local_test_file_path, cloud_local_test_file_path))
            return "UPLOAD_FAIL"
        return "SUCCESS"

# deltaChecker = DeltaChecker()
# deltaChecker.upload_products_if_necessary("20210513_22_59_56")
# deltaChecker.check_delta_and_update_cloud()
