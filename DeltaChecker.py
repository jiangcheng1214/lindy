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
        self.latest_data_dir_path = os.path.join(os.getcwd(), 'temp', 'latest')

    def get_base_timestamp(self):
        return self.database.child('base_timestamp').get().val()

    def update_base_timestamp(self, timestamp):
        self.database.child('base_timestamp').set(timestamp)

    def get_latest_timestamp(self):
        return self.database.child('latest_timestamp').get().val()

    def update_latest_timestamp(self, timestamp):
        self.database.child('latest_timestamp').set(timestamp)

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

    def download_data_for_delta_check(self, base_timestamp, test_timestamp):
        path_to_local_dir = "{}/{}_to_{}".format(self.temp_dir_path, base_timestamp, test_timestamp)
        if not os.path.isdir(path_to_local_dir):
            os.makedirs(path_to_local_dir)
        for category_code in self.category_codes:
            path_to_base_data_on_cloud = "products/{}/{}".format(base_timestamp, category_code)
            path_to_base_data = "{}/{}_{}".format(path_to_local_dir, base_timestamp, category_code)
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

        base_timestamp = self.get_base_timestamp()
        latest_timestamp = self.get_latest_timestamp()
        log_info("checking delta base_timestamp: {} latest_timestamp: {}".format(base_timestamp, latest_timestamp))
        if base_timestamp == latest_timestamp:
            log_info("skip delta check (base_timestamp equals to latest_timestamp)")
            return False
        assert base_timestamp < latest_timestamp
        data_dir_path = self.download_data_for_delta_check(base_timestamp, latest_timestamp)
        if not data_dir_path:
            log_warning("skip delta check (data download failed)")
            return False
        all_base_data = {}
        all_test_data = {}
        all_ids = set()
        for category_code in self.category_codes:
            base_data_path = os.path.join(data_dir_path, "{}_{}".format(base_timestamp, category_code))
            with open(base_data_path, 'r') as base_file:
                for l in base_file.readlines():
                    json_data = json.loads(l)
                    all_base_data[json_data['sku']] = json_data
                    all_ids.add(json_data['sku'])

            test_data_path = os.path.join(data_dir_path, "{}_{}".format(latest_timestamp, category_code))
            with open(test_data_path, 'r') as latest_file:
                for l in latest_file.readlines():
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
            log_info("no added/removed/updated items detected from {} to {}".format(base_timestamp, latest_timestamp))
            return False

        path_to_delta_dir_on_cloud = "delta/{}_to_{}/".format(base_timestamp, latest_timestamp)
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

        # update latest timestamp after uploading delta record
        self.update_base_timestamp(latest_timestamp)
        return True

    def is_scrape_success(self, timestamp):
        for category_code in supported_categories():
            flag_path = os.path.join(self.scraped_data_dir_path, timestamp, "product", "SUCCESS_" + category_code)
            if not os.path.exists(flag_path):
                log_warning(flag_path + " doesn't exist!")
                return False
        return True

    def upload_products_if_necessary(self, timestamp):
        def is_identical(path1, path2):
            log_info("Checking identification {} - {}".format(path1, path2))
            try:
                set1 = set()
                set2 = set()
                with open(path1) as f:
                    for json_line in f.readlines():
                        set1.add(json_line)
                with open(path2) as f:
                    for json_line in f.readlines():
                        set2.add(json_line)
                if len(set1) != len(set2):
                    return False
                for json_line in set1:
                    if not json_line in set2:
                        return False
                return True
            except Exception:
                log_exception("Failed to check identical of two files")
                return True

        if not self.is_scrape_success(timestamp):
            log_warning("Not a success scrape with timestamp " + timestamp)
            return False
        latest_timestamp = self.get_latest_timestamp()
        if timestamp <= latest_timestamp:
            log_warning("skip delta check (invalid timestamp: {} <= {})".format(timestamp, latest_timestamp))
            return False
        latest_data_dir_path = os.path.join(self.latest_data_dir_path, latest_timestamp)
        if not os.path.isdir(latest_data_dir_path):
            os.makedirs(latest_data_dir_path)
        test_data_dir_path = os.path.join(self.scraped_data_dir_path, timestamp, "product")
        should_upload = False
        try:
            for category_code in supported_categories():
                local_latest_file_path = os.path.join(latest_data_dir_path, category_code)
                local_test_file_path = os.path.join(test_data_dir_path, category_code)
                if not os.path.exists(local_latest_file_path):
                    cloud_latest_file_path = "products/{}/{}".format(latest_timestamp, category_code)
                    self.download(cloud_latest_file_path, local_latest_file_path)
                    assert os.path.exists(local_latest_file_path)
                if not is_identical(local_latest_file_path, local_test_file_path):
                    should_upload = True
                    break
        except Exception:
            log_exception("Exception during checking should upload or not")
            return False
        if not should_upload:
            log_info("Don't need to upload {}, latest: {}".format(timestamp,latest_timestamp))
            return True
        try:
            for category_code in supported_categories():
                local_test_file_path = os.path.join(test_data_dir_path, category_code)
                cloud_local_test_file_path = 'products/{}/{}'.format(timestamp, category_code)
                self.storage.child(cloud_local_test_file_path).put(local_test_file_path)
                log_info("{} has been uploaded to {}".format(local_test_file_path, cloud_local_test_file_path))
                self.update_latest_timestamp(timestamp)
        except Exception:
            log_exception("Exception during uploading {} to {}".format(local_test_file_path, cloud_local_test_file_path))
            return False
        return True

# deltaChecker = DeltaChecker()
# deltaChecker.upload_products_if_necessary("20210512_22_35_35")
# deltaChecker.check_delta_and_update_cloud()
