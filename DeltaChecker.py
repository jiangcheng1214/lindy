import json
import os

import pyrebase

from Utils import log_info, log_warning, supported_categories


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

    def get_base_timestamp(self):
        return self.database.child('base_timestamp').get().val()

    def update_base_timestamp(self, timestamp):
        self.database.child('base_timestamp').set(timestamp)

    def get_latest_timestamp(self):
        return self.database.child('latest_timestamp').get().val()

    def download_data_for_delta_check(self, base_timestamp, test_timestamp):
        try:
            path_to_local_dir = "{}/{}_to_{}".format(self.temp_dir_path, base_timestamp, test_timestamp)
            if not os.path.isdir(path_to_local_dir):
                os.makedirs(path_to_local_dir)
            for category_code in self.category_codes:
                path_to_base_data_on_cloud = "products/{}/{}".format(base_timestamp, category_code)
                path_to_base_data = "{}/{}_{}".format(path_to_local_dir, base_timestamp, category_code)
                self.storage.child(path_to_base_data_on_cloud).download(path=path_to_base_data, filename=path_to_base_data)
                assert os.path.isfile(path_to_base_data)
                path_to_test_data_on_cloud = "products/{}/{}".format(test_timestamp, category_code)
                path_to_test_data = "{}/{}_{}".format(path_to_local_dir, test_timestamp, category_code)
                self.storage.child(path_to_test_data_on_cloud).download(path=path_to_test_data,
                                                                        filename=path_to_test_data)
                assert os.path.isfile(path_to_test_data)
            return path_to_local_dir
        except Exception:
            return None

    def check_delta(self, test_timestamp):
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
        log_info("checking delta base_timestamp: {} test_timestamp: {}".format(base_timestamp, test_timestamp))
        if test_timestamp < base_timestamp:
            log_info("skip delta check (invalid timestamp: test_timestamp < base_timestamp)")
            return

        if base_timestamp == test_timestamp:
            log_info("skip delta check (base_timestamp equals to test_timestamp)")
            return
        data_dir_path = self.download_data_for_delta_check(base_timestamp, test_timestamp)
        if not data_dir_path:
            log_warning("skip delta check (data download failed)")
            return
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

            test_data_path = os.path.join(data_dir_path, "{}_{}".format(test_timestamp, category_code))
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

        # Only update on cloud storage when new items are added
        if added_items:
            path_to_delta_dir_on_cloud = "delta/{}_to_{}/".format(base_timestamp, test_timestamp)

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
            self.update_base_timestamp(test_timestamp)

        else:
            log_info("no added items detected from {} to {}".format(base_timestamp, test_timestamp))

deltaChecker = DeltaChecker()
deltaChecker.check_delta("20210502_16_58_20")